import Darwin
import Foundation
import os

/// A unix-domain persistent v1 session server.
public final class SocketServer: @unchecked Sendable {
    public struct Configuration: Sendable {
        public var maximumFrameBytes: Int
        public var maximumActiveRequests: Int
        public var maximumSessions: Int
        public var streamQueueDepth: Int
        public var maximumPendingWrites: Int
        public var handshakeTimeout: TimeInterval
        public var writeTimeout: TimeInterval

        public init(
            maximumFrameBytes: Int = daemonKitDefaultMaximumFrameBytes,
            maximumActiveRequests: Int = 64,
            maximumSessions: Int = 64,
            streamQueueDepth: Int = 16,
            maximumPendingWrites: Int = 64,
            handshakeTimeout: TimeInterval = 10,
            writeTimeout: TimeInterval = 10
        ) {
            self.maximumFrameBytes = maximumFrameBytes
            self.maximumActiveRequests = maximumActiveRequests
            self.maximumSessions = maximumSessions
            self.streamQueueDepth = streamQueueDepth
            self.maximumPendingWrites = maximumPendingWrites
            self.handshakeTimeout = handshakeTimeout
            self.writeTimeout = writeTimeout
        }
    }

    private enum State {
        case idle
        case starting
        case serving
        case stopping
        case stopped
    }

    private enum SocketPathProbe {
        case live
        case stale
        case uncertain
    }

    private struct ListenerResources: Sendable {
        let listener: Int32
        let shutdownRead: Int32
        let shutdownWrite: Int32
    }

    private final class ListenerResourcesOwner: @unchecked Sendable {
        private let path: String
        private let lock = NSLock()
        private var resources: ListenerResources?
        private var canceled = false

        init(path: String) {
            self.path = path
        }

        func install(_ resources: ListenerResources) throws {
            let accepted = lock.withLock {
                guard !canceled else { return false }
                self.resources = resources
                return true
            }
            guard accepted else {
                Self.close(resources, path: path)
                throw CancellationError()
            }
        }

        func commit(_ operation: (ListenerResources) -> Bool) throws {
            try lock.withLock {
                guard !canceled, let resources else { throw CancellationError() }
                guard operation(resources) else { throw CancellationError() }
                self.resources = nil
            }
        }

        func cancel() {
            let resources = lock.withLock {
                canceled = true
                let resources = self.resources
                self.resources = nil
                return resources
            }
            if let resources {
                Self.close(resources, path: path)
            }
        }

        func close() {
            let resources = lock.withLock {
                let resources = self.resources
                self.resources = nil
                return resources
            }
            if let resources {
                Self.close(resources, path: path)
            }
        }

        private static func close(_ resources: ListenerResources, path: String) {
            unlink(path)
            if resources.listener >= 0 {
                Darwin.close(resources.listener)
            }
            if resources.shutdownRead >= 0 {
                Darwin.close(resources.shutdownRead)
            }
            if resources.shutdownWrite >= 0 {
                Darwin.close(resources.shutdownWrite)
            }
        }
    }

    private let path: String
    private let wireBuild: String
    private let configuration: Configuration
    private let trust: PeerTrust
    private let handler: @Sendable (SocketRequest) async -> SocketResponse
    private let acceptQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.accept")
    private let controlQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.control")
    private let sessionGroup = DispatchGroup()
    private let lock = NSLock()
    private let startLatch = AsyncLatch()
    private let stopLatch = AsyncLatch()
    private var state = State.idle
    private var listenerDescriptor: Int32 = -1
    private var shutdownReadDescriptor: Int32 = -1
    private var shutdownWriteDescriptor: Int32 = -1
    private var sessions: [Int32: ServerSession] = [:]
    private var connections: Set<ServerConnection> = []
    var stopDrainHook: (@Sendable () async -> Void)?
    var startCommitHook: (@Sendable () async -> Void)?
    var stopFinishHook: (@Sendable () async -> Void)?
    var stopWaitHook: (@Sendable () async -> Void)?

    public init(
        path: String,
        wireBuild: String,
        configuration: Configuration = .init(),
        trust: PeerTrust,
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.path = path
        self.wireBuild = wireBuild
        self.configuration = configuration
        self.trust = trust
        self.handler = handler
    }
}

extension SocketServer {
    /// Reclaims a stale socket, binds with mode 0600, and starts accepting sessions.
    public func start() async throws {
        guard !wireBuild.isEmpty else { throw SocketServerError.emptyWireBuild }
        guard configuration.maximumFrameBytes > 0,
              configuration.maximumActiveRequests > 0,
              configuration.maximumSessions > 0,
              (1 ... Int(UInt32.max)).contains(configuration.streamQueueDepth)
        else {
            throw SessionTransportError.invalidFrame("stream queue exceeds protocol window")
        }
        guard configuration.maximumPendingWrites > 0 else {
            throw SessionTransportError.invalidFrame("write queue capacity")
        }
        guard configuration.handshakeTimeout.isFinite, configuration.handshakeTimeout > 0,
              configuration.writeTimeout.isFinite, configuration.writeTimeout > 0
        else { throw SessionTransportError.invalidFrame("timeout") }
        let mayStart = lock.withLock {
            guard case .idle = state else { return false }
            state = .starting
            return true
        }
        guard mayStart else { throw SocketServerError.alreadyRunning }
        let owner = ListenerResourcesOwner(path: path)
        do {
            try await withTaskCancellationHandler {
                try await controlQueue.performIO {
                    let resources = try self.makeListenerResources()
                    try owner.install(resources)
                }
                await startCommitHook?()
                try owner.commit { resources in
                    self.lock.withLock {
                        guard case .starting = self.state else { return false }
                        self.listenerDescriptor = resources.listener
                        self.shutdownReadDescriptor = resources.shutdownRead
                        self.shutdownWriteDescriptor = resources.shutdownWrite
                        self.acceptQueue.async { [weak self] in self?.acceptLoop() }
                        self.state = .serving
                        return true
                    }
                }
            } onCancel: {
                owner.cancel()
            }
            startLatch.finish()
        } catch {
            owner.close()
            lock.withLock { state = .stopped }
            startLatch.finish()
            stopLatch.finish()
            throw error
        }
    }

    /// Stops intake, disconnects sessions, joins their request tasks, and unlinks the path.
    public func stop() async {
        enum Action {
            case settle(Int32)
            case waitForStart
            case waitForStop
            case finishIdle
        }
        let action = lock.withLock { () -> Action in
            switch state {
            case .idle:
                state = .stopped
                return .finishIdle
            case .starting:
                state = .stopping
                return .waitForStart
            case .serving:
                state = .stopping
                return .settle(shutdownWriteDescriptor)
            case .stopping, .stopped:
                return .waitForStop
            }
        }
        switch action {
        case .finishIdle:
            startLatch.finish()
            stopLatch.finish()
            return
        case .waitForStart:
            await startLatch.wait()
            await stopLatch.wait()
            return
        case .waitForStop:
            await stopWaitHook?()
            await stopLatch.wait()
            return
        case .settle:
            break
        }
        guard case let .settle(writeDescriptor) = action else { return }
        var byte: UInt8 = 1
        while Darwin.write(writeDescriptor, &byte, 1) < 0 {
            if errno == EINTR {
                continue
            }
            // A full nonblocking pipe already has a pending shutdown signal.
            break
        }
        await withCheckedContinuation { continuation in
            acceptQueue.async {
                continuation.resume()
            }
        }
        await stopDrainHook?()
        let (active, connectionSnapshot): ([ServerSession], [ServerConnection]) = lock.withLock {
            (Array(sessions.values), Array(connections))
        }
        for session in active {
            session.close()
        }
        for connection in connectionSnapshot {
            connection.shutdown()
        }
        await withCheckedContinuation { continuation in
            DispatchQueue.global().async { [sessionGroup] in
                sessionGroup.wait()
                continuation.resume()
            }
        }
        let resources = lock.withLock {
            let resources = ListenerResources(
                listener: listenerDescriptor,
                shutdownRead: shutdownReadDescriptor,
                shutdownWrite: shutdownWriteDescriptor
            )
            listenerDescriptor = -1
            shutdownReadDescriptor = -1
            shutdownWriteDescriptor = -1
            state = .stopped
            return resources
        }
        await closeResources(resources)
        await stopFinishHook?()
        stopLatch.finish()
    }

    private func makeListenerResources() throws -> ListenerResources {
        let listener = try bind()
        var pipeDescriptors: [Int32] = [-1, -1]
        var complete = false
        defer {
            if !complete {
                unlink(path)
                Darwin.close(listener)
                if pipeDescriptors[0] >= 0 {
                    Darwin.close(pipeDescriptors[0])
                }
                if pipeDescriptors[1] >= 0 {
                    Darwin.close(pipeDescriptors[1])
                }
            }
        }
        guard pipe(&pipeDescriptors) == 0 else {
            let code = errno
            throw SocketServerError.socketFailed(errno: code)
        }
        guard fcntl(pipeDescriptors[1], F_SETNOSIGPIPE, 1) == 0 else {
            let code = errno
            throw SocketServerError.socketFailed(errno: code)
        }
        let pipeFlags = fcntl(pipeDescriptors[1], F_GETFL)
        guard pipeFlags >= 0,
              fcntl(pipeDescriptors[1], F_SETFL, pipeFlags | O_NONBLOCK) == 0
        else {
            let code = errno
            throw SocketServerError.socketFailed(errno: code)
        }
        complete = true
        return ListenerResources(
            listener: listener,
            shutdownRead: pipeDescriptors[0],
            shutdownWrite: pipeDescriptors[1]
        )
    }

    private func closeResources(_ resources: ListenerResources) async {
        await withCheckedContinuation { continuation in
            controlQueue.async { [path] in
                unlink(path)
                if resources.listener >= 0 {
                    Darwin.close(resources.listener)
                }
                if resources.shutdownRead >= 0 {
                    Darwin.close(resources.shutdownRead)
                }
                if resources.shutdownWrite >= 0 {
                    Darwin.close(resources.shutdownWrite)
                }
                continuation.resume()
            }
        }
    }

    private func bind() throws -> Int32 {
        if access(path, F_OK) == 0 {
            var status = stat()
            guard lstat(path, &status) == 0, status.st_mode & S_IFMT == S_IFSOCK else {
                throw SocketServerError.addressInUse(path: path)
            }
            switch probeSocket(at: path) {
            case .live, .uncertain:
                throw SocketServerError.addressInUse(path: path)
            case .stale:
                unlink(path)
            }
        }
        var address = try Self.makeAddress(path: path)
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw SocketServerError.socketFailed(errno: errno) }
        let bound = withAddress(&address) { Darwin.bind(descriptor, $0, $1) }
        guard bound == 0 else {
            let code = errno
            Darwin.close(descriptor)
            throw SocketServerError.bindFailed(path: path, errno: code)
        }
        guard chmod(path, 0o600) == 0 else {
            let code = errno
            unlink(path)
            Darwin.close(descriptor)
            throw SocketServerError.chmodFailed(path: path, errno: code)
        }
        guard listen(descriptor, 64) == 0 else {
            let code = errno
            unlink(path)
            Darwin.close(descriptor)
            throw SocketServerError.listenFailed(errno: code)
        }
        return descriptor
    }

    private func probeSocket(at path: String) -> SocketPathProbe {
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { return .uncertain }
        defer { Darwin.close(descriptor) }
        guard var address = try? Self.makeAddress(path: path) else { return .uncertain }
        let flags = fcntl(descriptor, F_GETFL)
        guard flags >= 0, fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0 else { return .uncertain }
        let connected = withAddress(&address) { connect(descriptor, $0, $1) }
        if connected == 0 {
            return .live
        }
        if errno == ECONNREFUSED || errno == ENOENT {
            return .stale
        }
        guard errno == EINPROGRESS else { return .uncertain }
        let deadline = DispatchTime.now().uptimeNanoseconds + 250_000_000
        while true {
            var writable = pollfd(fd: descriptor, events: Int16(POLLOUT), revents: 0)
            let timeout = SessionFrameCodec.pollTimeout(deadline: deadline, maximum: 250)
            let ready = poll(&writable, 1, timeout)
            if ready < 0, errno == EINTR {
                continue
            }
            guard ready > 0 else { return .uncertain }
            var socketError: Int32 = 0
            var length = socklen_t(MemoryLayout<Int32>.size)
            guard getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &socketError, &length) == 0 else {
                return .uncertain
            }
            if socketError == 0 {
                return .live
            }
            if socketError == ECONNREFUSED || socketError == ENOENT {
                return .stale
            }
            return .uncertain
        }
    }

    private func acceptLoop() {
        while true {
            var descriptors = [
                pollfd(fd: listenerDescriptor, events: Int16(POLLIN), revents: 0),
                pollfd(fd: shutdownReadDescriptor, events: Int16(POLLIN), revents: 0),
            ]
            let ready = poll(&descriptors, nfds_t(descriptors.count), -1)
            if ready < 0 {
                if errno == EINTR {
                    continue
                }
                break
            }
            if descriptors[1].revents != 0 {
                break
            }
            guard descriptors[0].revents & Int16(POLLIN) != 0 else { continue }
            let descriptor = accept(listenerDescriptor, nil, nil)
            if descriptor < 0 {
                if errno == EINTR || errno == ECONNABORTED {
                    continue
                }
                socketServerLog.error("accept failed: \(String(cString: strerror(errno)), privacy: .public)")
                continue
            }
            let connection = ServerConnection(descriptor: descriptor)
            lock.lock()
            if connections.count >= configuration.maximumSessions {
                lock.unlock()
                connection.close()
                continue
            }
            connections.insert(connection)
            lock.unlock()
            sessionGroup.enter()
            Task {
                await self.serve(connection)
                self.sessionGroup.leave()
            }
        }
    }

    private func serve(_ connection: ServerConnection) async {
        let descriptor = connection.descriptor
        defer {
            removeConnection(connection)
            connection.close()
        }
        do {
            let setupQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.setup.\(descriptor)")
            let peer = try await setupQueue.performIO { [trust] in
                try self.configure(descriptor)
                try trust.check(descriptor: descriptor)
                var user = uid_t()
                var group = gid_t()
                guard getpeereid(descriptor, &user, &group) == 0 else {
                    throw SessionTransportError.systemCall(operation: "getpeereid", errno: errno)
                }
                return SocketPeer(effectiveUserID: user, effectiveGroupID: group)
            }
            let session = ServerSession(
                descriptor: descriptor,
                shutdown: { connection.shutdown() },
                wireBuild: wireBuild,
                peer: peer,
                configuration: configuration,
                handler: handler
            )
            insert(session, descriptor: descriptor)
            defer {
                removeSession(descriptor)
            }
            do {
                try await session.run()
            } catch {
                socketServerLog.debug("session ended: \(String(describing: error), privacy: .public)")
            }
            await session.drainIO()
        } catch {
            socketServerLog.debug("session ended: \(String(describing: error), privacy: .public)")
        }
    }

    private func insert(_ session: ServerSession, descriptor: Int32) {
        lock.lock()
        sessions[descriptor] = session
        lock.unlock()
    }

    private func removeSession(_ descriptor: Int32) {
        lock.lock()
        sessions.removeValue(forKey: descriptor)
        lock.unlock()
    }

    private func removeConnection(_ connection: ServerConnection) {
        lock.lock()
        connections.remove(connection)
        lock.unlock()
    }

    private func configure(_ descriptor: Int32) throws {
        var enable: Int32 = 1
        setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enable, socklen_t(MemoryLayout<Int32>.size))
        let flags = fcntl(descriptor, F_GETFL)
        guard flags >= 0 else {
            throw SessionTransportError.systemCall(operation: "fcntl", errno: errno)
        }
        guard fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0 else {
            throw SessionTransportError.systemCall(operation: "fcntl", errno: errno)
        }
    }

    private static func makeAddress(path: String) throws -> sockaddr_un {
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let capacity = MemoryLayout.size(ofValue: address.sun_path)
        let bytes = Array(path.utf8)
        guard bytes.count < capacity else {
            throw SocketServerError.pathTooLong(path: path, limit: capacity - 1)
        }
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            bytes.withUnsafeBytes { destination.copyMemory(from: $0) }
        }
        return address
    }

    private func withAddress<Result>(
        _ address: inout sockaddr_un,
        _ body: (UnsafePointer<sockaddr>, socklen_t) -> Result
    ) -> Result {
        withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { rebound in
                body(rebound, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
    }
}

final class ServerSession: @unchecked Sendable {
    let descriptor: Int32
    private let serverWireBuild: String
    private let peer: SocketPeer
    private let configuration: SocketServer.Configuration
    private let handler: @Sendable (SocketRequest) async -> SocketResponse
    private let shutdownDescriptor: @Sendable () -> Void
    private let codec: SessionFrameCodec
    private let readQueue: DispatchQueue
    private let writer: SessionWriter
    private let eventWindow = SocketCreditWindow()
    private let lifecycle = SocketSessionLifecycle()
    private let generation: Data
    private let lock = NSLock()
    private var active: [UInt64: ServerRequestState] = [:]
    private var seen: Set<UInt64> = []
    private var watermark: UInt64 = 0
    private var closed = false

    init(
        descriptor: Int32,
        shutdown: @escaping @Sendable () -> Void,
        wireBuild: String,
        peer: SocketPeer,
        configuration: SocketServer.Configuration,
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.descriptor = descriptor
        shutdownDescriptor = shutdown
        serverWireBuild = wireBuild
        self.peer = peer
        self.configuration = configuration
        self.handler = handler
        var uuid = UUID().uuid
        generation = withUnsafeBytes(of: &uuid) { Data($0) }
        codec = SessionFrameCodec(
            descriptor: descriptor,
            maximumFrameBytes: configuration.maximumFrameBytes,
            writeTimeout: configuration.writeTimeout
        )
        writer = SessionWriter(
            codec: codec,
            maximumPendingWrites: configuration.maximumPendingWrites,
            label: "com.yasyf.daemonkit.SocketServer.write.\(descriptor)"
        )
        readQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.read.\(descriptor)")
    }

    deinit {
        lifecycle.close()
    }

    func run() async throws {
        do {
            let clientWireBuild = try await handshake()
            while true {
                let frame = try await read()
                switch frame.kind {
                case .request:
                    try await receiveRequest(frame, clientWireBuild: clientWireBuild)
                case .cancel:
                    try await receiveCancel(frame)
                case .stream:
                    try await receiveStream(frame)
                case .window:
                    try await receiveWindow(frame)
                case .acknowledgment:
                    try await receiveAcknowledgement(frame)
                case .goAway:
                    close()
                    await eventWindow.close()
                    await settleRequests()
                    return
                default:
                    throw SessionTransportError.invalidFrame("client frame kind \(frame.kind)")
                }
            }
        } catch {
            close()
            await eventWindow.close()
            await settleRequests()
            throw error
        }
    }

    func write(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock { closed }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try await writer.write(frame)
    }

    func writeSettlement(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock { closed }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try await writer.writeSettlement(frame)
    }

    func pushEvent(topic: String, payload: Data) async throws {
        guard await eventWindow.acquire() else { throw CancellationError() }
        try await write(SessionFrame(kind: .event, flags: .end, operation: topic, payload: payload))
    }

    func close() {
        lock.lock()
        guard !closed else {
            lock.unlock()
            return
        }
        closed = true
        lock.unlock()
        lifecycle.close()
        writer.abort()
        Task { await eventWindow.close() }
        shutdownDescriptor()
    }

    func drainIO() async {
        writer.abort()
        shutdownDescriptor()
        await withCheckedContinuation { continuation in
            writer.afterDrained { [readQueue] in
                readQueue.async {
                    continuation.resume()
                }
            }
        }
    }

    private func handshake() async throws -> String {
        let frame = try await read(timeout: configuration.handshakeTimeout)
        guard frame.kind == .hello, frame.flags == .end, frame.id == 0,
              frame.sequence == 0, frame.operation.isEmpty, frame.tenant.isEmpty
        else {
            throw SessionTransportError.handshake("invalid hello")
        }
        let identity = try JSONDecoder().decode(SessionWireIdentity.self, from: frame.payload)
        guard identity.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(identity.protocolVersion)
        }
        guard !identity.wireBuild.isEmpty else {
            throw SessionTransportError.handshake("empty wireBuild")
        }
        guard identity.session == nil else {
            throw SessionTransportError.handshake("client supplied a session generation")
        }
        let payload = try JSONEncoder().encode(SessionWireIdentity(
            protocolVersion: daemonKitSessionProtocolVersion,
            wireBuild: serverWireBuild,
            session: generation
        ))
        try await write(SessionFrame(kind: .helloAck, flags: .end, payload: payload))
        return identity.wireBuild
    }

    private func read(timeout: TimeInterval = 0) async throws -> SessionFrame {
        try await readQueue.performIO {
            try self.codec.read(timeout: timeout)
        }
    }

    private enum Admission {
        case accepted(ServerRequestState)
        case rejected(String)
    }

    private func receiveRequest(_ frame: SessionFrame, clientWireBuild: String) async throws {
        guard frame.id != 0, !frame.operation.isEmpty, frame.sequence == 0 else {
            throw SessionTransportError.invalidFrame("request")
        }
        let admission = try admit(frame, clientWireBuild: clientWireBuild)
        guard case let .accepted(state) = admission else {
            guard case let .rejected(reason) = admission else { return }
            try await sendRejected(id: frame.id, reason: reason)
            return
        }
        if frame.flags.contains(.end) {
            await state.finishInitialInput()
        }

        let chunks = SocketChunkStream(channel: state.channel) {
            Task { await state.cancel() }
        } consumptionOperation: { [weak self] _ in
            guard let self else { throw SessionTransportError.disconnected }
            try await writeSettlement(SessionFrame(kind: .window, id: frame.id, sequence: 1))
        }
        try await write(SessionFrame(
            kind: .window,
            id: frame.id,
            sequence: UInt32(configuration.streamQueueDepth)
        ))
        let publicSession = SocketSession(implementation: self, lifecycle: lifecycle)
        let request = SocketRequest(
            id: frame.id,
            operation: frame.operation,
            tenant: frame.tenant,
            payload: frame.payload,
            chunks: chunks,
            peer: peer,
            peerWireBuild: clientWireBuild,
            session: publicSession
        )
        let task = Task { [weak self] in
            guard let self else { return }
            var response = await handler(request)
            if let transportError = await state.error() {
                await cancelAndSettle(response)
                response = .terminal(SocketTerminal(error: String(describing: transportError)))
            } else if Task.isCancelled {
                await cancelAndSettle(response)
                response = .terminal(SocketTerminal(error: "wire: request canceled"))
            }
            do {
                try await send(response, id: frame.id, state: state)
            } catch {
                socketServerLog.debug("response failed: \(String(describing: error), privacy: .public)")
            }
            await state.finish()
            remove(frame.id)
        }
        await state.attach(task)
        if frame.deadlineUnixMilliseconds > 0 {
            let interval = SessionTime.remainingMilliseconds(until: frame.deadlineUnixMilliseconds)
            let timer = Task {
                try? await Task.sleep(for: .milliseconds(interval))
                if !Task.isCancelled {
                    task.cancel()
                }
            }
            await state.attachDeadline(timer)
        }
    }
}

private extension ServerSession {
    private func admit(_ frame: SessionFrame, clientWireBuild: String) throws -> Admission {
        lock.lock()
        defer { lock.unlock() }
        guard frame.id > watermark, !seen.contains(frame.id) else {
            throw SessionTransportError.duplicateRequestID(frame.id)
        }
        guard frame.id - watermark <= UInt64(configuration.maximumActiveRequests),
              active.count < configuration.maximumActiveRequests
        else {
            return .rejected("wire: queue at capacity")
        }
        seen.insert(frame.id)
        while seen.remove(watermark + 1) != nil {
            watermark += 1
        }
        if clientWireBuild != serverWireBuild {
            return .rejected("wire: client wireBuild does not match server wireBuild")
        }
        let state = ServerRequestState(capacity: configuration.streamQueueDepth)
        active[frame.id] = state
        return .accepted(state)
    }

    private func receiveCancel(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.flags == .end, frame.operation.isEmpty,
              frame.tenant.isEmpty, frame.payload.isEmpty
        else {
            throw SessionTransportError.invalidFrame("cancel")
        }
        await request(frame.id)?.cancel()
    }

    private func receiveStream(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("stream")
        }
        await request(frame.id)?.receive(frame)
    }

    private func settleRequests() async {
        let requests = takeRequests()
        for request in requests {
            await request.settle()
        }
    }

    private func takeRequests() -> [ServerRequestState] {
        lock.lock()
        let requests = Array(active.values)
        active.removeAll()
        lock.unlock()
        return requests
    }

    private func send(_ response: SocketResponse, id: UInt64, state: ServerRequestState) async throws {
        switch response {
        case let .terminal(terminal):
            try await sendTerminal(terminal, id: id, state: state)
        case let .stream(stream):
            try await send(stream, id: id, state: state)
        }
        try await state.waitForTerminalAcknowledgement(timeout: configuration.writeTimeout)
    }

    private func send(_ stream: SocketResponseStream, id: UInt64, state: ServerRequestState) async throws {
        let settlement = SocketResponseSettlement(stream: stream)
        var sequence = SessionSequence()
        try await withTaskCancellationHandler {
            do {
                while true {
                    try Task.checkCancellation()
                    guard await state.responseWindow.acquire() else { throw CancellationError() }
                    guard let payload = try await stream.nextChunk() else { break }
                    let current = try sequence.take()
                    try await write(SessionFrame(kind: .stream, id: id, sequence: current, payload: payload))
                }
                let terminal = try await settlement.value().get()
                try Task.checkCancellation()
                try await sendTerminal(terminal, id: id, state: state)
            } catch is CancellationError {
                stream.cancel()
                _ = await settlement.value()
                try await sendTerminal(SocketTerminal(error: "wire: request canceled"), id: id, state: state)
            } catch {
                stream.cancel()
                _ = await settlement.value()
                try await sendTerminal(SocketTerminal(error: String(describing: error)), id: id, state: state)
            }
        } onCancel: {
            stream.cancel()
        }
    }

    private func cancelAndSettle(_ response: SocketResponse) async {
        guard case let .stream(stream) = response else { return }
        stream.cancel()
        _ = await SocketResponseSettlement(stream: stream).value()
    }

    private func sendTerminal(_ terminal: SocketTerminal, id: UInt64, state: ServerRequestState) async throws {
        let envelope = try responseEnvelope(terminal, acknowledge: true)
        try await state.writeTerminal { [self] in
            try await writeSettlement(SessionFrame(kind: .response, flags: .end, id: id, payload: envelope))
        }
    }

    private func sendRejected(id: UInt64, reason: String) async throws {
        try await writeSettlement(SessionFrame(
            kind: .response,
            flags: .end,
            id: id,
            payload: responseEnvelope(SocketTerminal(rejected: true, reason: reason))
        ))
    }

    private func remove(_ id: UInt64) {
        lock.lock()
        active.removeValue(forKey: id)
        lock.unlock()
    }

    private func responseEnvelope(_ response: SocketTerminal, acknowledge: Bool = false) throws -> Data {
        var members: [String] = []
        if acknowledge {
            members.append("\"ack\":true")
        }
        if response.rejected {
            members.append("\"rejected\":true")
        }
        if let reason = response.reason {
            try members.append("\"reason\":\(jsonString(reason))")
        }
        if let error = response.error {
            try members.append("\"err\":\(jsonString(error))")
        }
        if let payload = response.payload {
            guard (try? JSONSerialization.jsonObject(with: payload, options: [.fragmentsAllowed])) != nil else {
                throw SessionTransportError.invalidFrame("response payload is not JSON")
            }
            guard let payloadString = String(data: payload, encoding: .utf8) else {
                throw SessionTransportError.invalidFrame("response payload is not UTF-8 JSON")
            }
            members.append("\"payload\":\(payloadString)")
        }
        return Data("{\(members.joined(separator: ","))}".utf8)
    }

    private func jsonString(_ value: String) throws -> String {
        let data = try JSONSerialization.data(withJSONObject: [value])
        guard let array = String(data: data, encoding: .utf8) else {
            throw SessionTransportError.invalidFrame("JSON string encoding is not UTF-8")
        }
        return String(array.dropFirst().dropLast())
    }
}

private extension ServerSession {
    func request(_ id: UInt64) -> ServerRequestState? {
        lock.lock()
        let state = active[id]
        lock.unlock()
        return state
    }

    func receiveWindow(_ frame: SessionFrame) async throws {
        guard frame.flags.isEmpty, frame.sequence > 0, frame.operation.isEmpty,
              frame.tenant.isEmpty, frame.payload.isEmpty
        else { throw SessionTransportError.invalidFrame("response or event window") }
        if frame.id == 0 {
            await eventWindow.grant(frame.sequence)
            return
        }
        await request(frame.id)?.grantResponseCredits(frame.sequence)
    }

    func receiveAcknowledgement(_ frame: SessionFrame) async throws {
        guard frame.flags == .end, frame.id != 0, frame.sequence == 0,
              frame.operation.isEmpty, frame.tenant.isEmpty, frame.payload == generation,
              let state = request(frame.id), await state.acknowledgeTerminal()
        else { throw SessionTransportError.invalidFrame("acknowledgement") }
    }
}
