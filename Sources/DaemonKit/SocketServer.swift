import Darwin
import Foundation
import os

private let socketServerLog = Logger(subsystem: DaemonKit.loggingSubsystem, category: "SocketServer")

/// Errors thrown while binding or running a ``SocketServer``.
public enum SocketServerError: Error, Sendable {
    case pathTooLong(path: String, limit: Int)
    case addressInUse(path: String)
    case socketFailed(errno: Int32)
    case bindFailed(path: String, errno: Int32)
    case listenFailed(errno: Int32)
    case alreadyRunning
    case emptyBuild
}

/// The immutable OS identity captured from an accepted socket.
public struct SocketPeer: Sendable {
    public let effectiveUserID: uid_t
    public let effectiveGroupID: gid_t
}

/// One ordered request-stream chunk.
public struct SocketRequestChunk: Sendable {
    public let sequence: UInt32
    public let payload: Data
    public let end: Bool
}

/// A request admitted on a persistent session.
public struct SocketRequest: Sendable {
    public let id: UInt64
    public let operation: String
    public let tenant: String
    public let payload: Data
    public let chunks: SocketChunkStream
    public let peer: SocketPeer
    public let peerBuild: String
    public let session: SocketSession
}

/// A trusted persistent server session exposed to request handlers.
public final class SocketSession: @unchecked Sendable {
    fileprivate weak var implementation: ServerSession?
    private let lifecycle: SocketSessionLifecycle

    fileprivate init(implementation: ServerSession, lifecycle: SocketSessionLifecycle) {
        self.implementation = implementation
        self.lifecycle = lifecycle
    }

    /// Whether the authenticated peer connection remains live.
    public var isConnected: Bool {
        lifecycle.isConnected
    }

    /// Suspends until the authenticated peer connection closes.
    public func waitUntilClosed() async {
        await lifecycle.waitUntilClosed()
    }

    /// Pushes one event to the peer on the session's serialized writer.
    public func pushEvent(topic: String, payload: Data = Data()) async throws {
        guard !topic.isEmpty else {
            throw SessionTransportError.invalidFrame("empty event topic")
        }
        guard let implementation else {
            throw SessionTransportError.disconnected
        }
        try await implementation.pushEvent(topic: topic, payload: payload)
    }
}

private final class SocketSessionLifecycle: @unchecked Sendable {
    private let lock = NSLock()
    private var connected = true
    private var waiters: [CheckedContinuation<Void, Never>] = []

    var isConnected: Bool {
        lock.lock()
        defer { lock.unlock() }
        return connected
    }

    func waitUntilClosed() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            guard connected else {
                lock.unlock()
                continuation.resume()
                return
            }
            waiters.append(continuation)
            lock.unlock()
        }
    }

    func close() {
        lock.lock()
        guard connected else {
            lock.unlock()
            return
        }
        connected = false
        let pending = waiters
        waiters.removeAll()
        lock.unlock()
        for waiter in pending {
            waiter.resume()
        }
    }
}

/// A unix-domain persistent v3 session server.
public final class SocketServer: @unchecked Sendable {
    public struct Configuration: Sendable {
        public var maximumFrameBytes: Int
        public var maximumActiveRequests: Int
        public var maximumSessions: Int
        public var streamQueueDepth: Int
        public var handshakeTimeout: TimeInterval
        public var writeTimeout: TimeInterval

        public init(
            maximumFrameBytes: Int = daemonKitDefaultMaximumFrameBytes,
            maximumActiveRequests: Int = 64,
            maximumSessions: Int = 64,
            streamQueueDepth: Int = 16,
            handshakeTimeout: TimeInterval = 10,
            writeTimeout: TimeInterval = 10
        ) {
            self.maximumFrameBytes = maximumFrameBytes
            self.maximumActiveRequests = maximumActiveRequests
            self.maximumSessions = maximumSessions
            self.streamQueueDepth = streamQueueDepth
            self.handshakeTimeout = handshakeTimeout
            self.writeTimeout = writeTimeout
        }
    }

    private enum State {
        case idle
        case serving
        case stopped
    }

    private let path: String
    private let build: String
    private let configuration: Configuration
    private let trust: PeerTrust
    private let handler: @Sendable (SocketRequest) async -> SocketResponse
    // Serial: stop() joins the single long-lived accept loop via acceptQueue.sync.
    private let acceptQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.accept")
    private let sessionGroup = DispatchGroup()
    private let lock = NSLock()
    private var state = State.idle
    private var listenerDescriptor: Int32 = -1
    private var shutdownReadDescriptor: Int32 = -1
    private var shutdownWriteDescriptor: Int32 = -1
    private var sessions: [Int32: ServerSession] = [:]
    private var connections: Set<Int32> = []

    public init(
        path: String,
        build: String,
        configuration: Configuration = .init(),
        trust: PeerTrust,
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.path = path
        self.build = build
        self.configuration = configuration
        self.trust = trust
        self.handler = handler
    }

    /// Reclaims a stale socket, binds with mode 0600, and starts accepting sessions.
    public func start() throws {
        guard !build.isEmpty else { throw SocketServerError.emptyBuild }
        guard (1 ... Int(UInt32.max)).contains(configuration.streamQueueDepth) else {
            throw SessionTransportError.invalidFrame("stream queue exceeds protocol window")
        }
        lock.lock()
        guard state == .idle else {
            lock.unlock()
            throw SocketServerError.alreadyRunning
        }
        state = .serving
        lock.unlock()
        do {
            let listener = try bind()
            var pipeDescriptors: [Int32] = [-1, -1]
            guard pipe(&pipeDescriptors) == 0 else {
                let code = errno
                close(listener)
                throw SocketServerError.socketFailed(errno: code)
            }
            lock.lock()
            listenerDescriptor = listener
            shutdownReadDescriptor = pipeDescriptors[0]
            shutdownWriteDescriptor = pipeDescriptors[1]
            lock.unlock()
        } catch {
            lock.lock()
            state = .idle
            lock.unlock()
            throw error
        }
        acceptQueue.async { [weak self] in self?.acceptLoop() }
    }

    /// Stops intake, disconnects sessions, joins their request tasks, and unlinks the path.
    public func stop() {
        lock.lock()
        guard state == .serving else {
            lock.unlock()
            return
        }
        state = .stopped
        let writeDescriptor = shutdownWriteDescriptor
        lock.unlock()
        var byte: UInt8 = 1
        while Darwin.write(writeDescriptor, &byte, 1) < 0, errno == EINTR {}
        acceptQueue.sync {}
        lock.lock()
        let active = Array(sessions.values)
        let descriptors = Array(connections)
        lock.unlock()
        for session in active {
            session.close()
        }
        for descriptor in descriptors {
            shutdown(descriptor, SHUT_RDWR)
        }
        sessionGroup.wait()
        lock.lock()
        close(shutdownReadDescriptor)
        close(shutdownWriteDescriptor)
        shutdownReadDescriptor = -1
        shutdownWriteDescriptor = -1
        lock.unlock()
        unlink(path)
    }

    private func bind() throws -> Int32 {
        if access(path, F_OK) == 0 {
            if isSocketLive(at: path) {
                throw SocketServerError.addressInUse(path: path)
            }
            unlink(path)
        }
        var address = try Self.makeAddress(path: path)
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw SocketServerError.socketFailed(errno: errno) }
        let bound = withAddress(&address) { Darwin.bind(descriptor, $0, $1) }
        guard bound == 0 else {
            let code = errno
            close(descriptor)
            throw SocketServerError.bindFailed(path: path, errno: code)
        }
        chmod(path, 0o600)
        guard listen(descriptor, 64) == 0 else {
            let code = errno
            close(descriptor)
            throw SocketServerError.listenFailed(errno: code)
        }
        return descriptor
    }

    private func isSocketLive(at path: String) -> Bool {
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { return false }
        defer { close(descriptor) }
        guard var address = try? Self.makeAddress(path: path) else { return false }
        return withAddress(&address) { connect(descriptor, $0, $1) } == 0
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
            lock.lock()
            if connections.count >= configuration.maximumSessions {
                lock.unlock()
                close(descriptor)
                continue
            }
            connections.insert(descriptor)
            lock.unlock()
            sessionGroup.enter()
            Task {
                await self.serve(descriptor)
                self.sessionGroup.leave()
            }
        }
        close(listenerDescriptor)
    }

    private func serve(_ descriptor: Int32) async {
        defer {
            removeConnection(descriptor)
            close(descriptor)
        }
        configure(descriptor)
        do {
            try trust.check(descriptor: descriptor)
            var user = uid_t()
            var group = gid_t()
            guard getpeereid(descriptor, &user, &group) == 0 else {
                throw SessionTransportError.systemCall(operation: "getpeereid", errno: errno)
            }
            let session = ServerSession(
                descriptor: descriptor,
                build: build,
                peer: SocketPeer(effectiveUserID: user, effectiveGroupID: group),
                configuration: configuration,
                handler: handler
            )
            insert(session, descriptor: descriptor)
            defer {
                removeSession(descriptor)
            }
            try await session.run()
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

    private func removeConnection(_ descriptor: Int32) {
        lock.lock()
        connections.remove(descriptor)
        lock.unlock()
    }

    private func configure(_ descriptor: Int32) {
        var enable: Int32 = 1
        setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enable, socklen_t(MemoryLayout<Int32>.size))
        setTimeout(descriptor, option: SO_RCVTIMEO, seconds: configuration.handshakeTimeout)
        setTimeout(descriptor, option: SO_SNDTIMEO, seconds: configuration.writeTimeout)
    }

    private func setTimeout(_ descriptor: Int32, option: Int32, seconds: TimeInterval) {
        var timeout = timeval(
            tv_sec: Int(seconds),
            tv_usec: Int32((seconds - Double(Int(seconds))) * 1_000_000)
        )
        setsockopt(descriptor, SOL_SOCKET, option, &timeout, socklen_t(MemoryLayout<timeval>.size))
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

private actor ServerRequestState {
    let channel: SocketBoundedChannel<SocketRequestChunk>
    let responseWindow = SocketCreditWindow()
    private var task: Task<Void, Never>?
    private var requestSequence = SessionSequence()
    private var ended = false
    private var canceled = false
    private var transportError: Error?

    init(capacity: Int) {
        channel = SocketBoundedChannel(capacity: capacity)
    }

    func attach(_ task: Task<Void, Never>) {
        self.task = task
        if canceled {
            task.cancel()
        }
    }

    func finishInitialInput() async {
        guard !ended else { return }
        ended = true
        await channel.finish()
    }

    func receive(_ frame: SessionFrame) async {
        guard !canceled else { return }
        guard !ended else {
            let error = SessionTransportError.invalidFrame("request stream already ended")
            transportError = error
            canceled = true
            task?.cancel()
            await channel.finish(throwing: error)
            await responseWindow.close()
            return
        }
        let expected: UInt32
        do {
            expected = try requestSequence.take()
        } catch {
            transportError = error
            canceled = true
            task?.cancel()
            await channel.finish(throwing: error)
            await responseWindow.close()
            return
        }
        guard frame.sequence == expected else {
            let error = SessionTransportError.streamSequence(
                id: frame.id,
                got: frame.sequence,
                want: expected
            )
            transportError = error
            canceled = true
            ended = true
            task?.cancel()
            await channel.finish(throwing: error)
            return
        }
        let end = frame.flags.contains(.end)
        if end {
            ended = true
        }
        let accepted = await channel.offer(SocketRequestChunk(
            sequence: frame.sequence,
            payload: frame.payload,
            end: end
        ))
        if !accepted {
            let error = SessionTransportError.invalidFrame("request stream exceeded granted window")
            transportError = error
            canceled = true
            task?.cancel()
            await channel.finish(throwing: error)
            await responseWindow.close()
            return
        }
        if end, accepted {
            await channel.finish()
        }
    }

    func cancel() async {
        guard !canceled else { return }
        canceled = true
        ended = true
        task?.cancel()
        await channel.discard()
        await responseWindow.close()
    }

    func error() -> Error? {
        transportError
    }

    func grantResponseCredits(_ count: UInt32) async {
        await responseWindow.grant(count)
    }

    func settle() async {
        await cancel()
        await task?.value
    }
}

private final class ServerSession: @unchecked Sendable {
    let descriptor: Int32
    private let serverBuild: String
    private let peer: SocketPeer
    private let configuration: SocketServer.Configuration
    private let handler: @Sendable (SocketRequest) async -> SocketResponse
    private let codec: SessionFrameCodec
    private let readQueue: DispatchQueue
    private let eventWindow = SocketCreditWindow()
    private let lifecycle = SocketSessionLifecycle()
    private let lock = NSLock()
    private var active: [UInt64: ServerRequestState] = [:]
    private var seen: Set<UInt64> = []
    private var watermark: UInt64 = 0
    private var closed = false

    init(
        descriptor: Int32,
        build: String,
        peer: SocketPeer,
        configuration: SocketServer.Configuration,
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.descriptor = descriptor
        serverBuild = build
        self.peer = peer
        self.configuration = configuration
        self.handler = handler
        codec = SessionFrameCodec(descriptor: descriptor, maximumFrameBytes: configuration.maximumFrameBytes)
        readQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.read.\(descriptor)")
    }

    deinit {
        lifecycle.close()
    }

    func run() async throws {
        do {
            let clientBuild = try await handshake()
            disableReadTimeout()
            while true {
                let frame = try await read()
                switch frame.kind {
                case .request:
                    try await receiveRequest(frame, clientBuild: clientBuild)
                case .cancel:
                    try await receiveCancel(frame)
                case .stream:
                    try await receiveStream(frame)
                case .window:
                    try await receiveWindow(frame)
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

    func write(_ frame: SessionFrame) throws {
        lock.lock()
        let isClosed = closed
        lock.unlock()
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try codec.write(frame)
    }

    func pushEvent(topic: String, payload: Data) async throws {
        guard await eventWindow.acquire() else { throw CancellationError() }
        try write(SessionFrame(kind: .event, flags: .end, operation: topic, payload: payload))
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
        Task { await eventWindow.close() }
        shutdown(descriptor, SHUT_RDWR)
    }

    private func handshake() async throws -> String {
        let frame = try await read()
        guard frame.kind == .hello, frame.flags == .end, frame.id == 0,
              frame.sequence == 0, frame.operation.isEmpty, frame.tenant.isEmpty
        else {
            throw SessionTransportError.handshake("invalid hello")
        }
        let identity = try JSONDecoder().decode(SessionBuildIdentity.self, from: frame.payload)
        guard identity.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(identity.protocolVersion)
        }
        guard !identity.build.isEmpty else {
            throw SessionTransportError.handshake("empty build")
        }
        let payload = try JSONEncoder().encode(SessionBuildIdentity(
            protocolVersion: daemonKitSessionProtocolVersion,
            build: serverBuild
        ))
        try codec.write(SessionFrame(kind: .helloAck, flags: .end, payload: payload))
        return identity.build
    }

    private func read() async throws -> SessionFrame {
        try await withCheckedThrowingContinuation { continuation in
            readQueue.async { [codec] in
                continuation.resume(with: Result { try codec.read() })
            }
        }
    }

    private func disableReadTimeout() {
        var noTimeout = timeval()
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &noTimeout, socklen_t(MemoryLayout<timeval>.size))
    }

    private enum Admission {
        case accepted(ServerRequestState)
        case rejected(String)
    }

    private func receiveRequest(_ frame: SessionFrame, clientBuild: String) async throws {
        guard frame.id != 0, !frame.operation.isEmpty, frame.sequence == 0 else {
            throw SessionTransportError.invalidFrame("request")
        }
        let admission = try admit(frame, clientBuild: clientBuild)
        guard case let .accepted(state) = admission else {
            guard case let .rejected(reason) = admission else { return }
            try sendRejected(id: frame.id, reason: reason)
            return
        }
        if frame.flags.contains(.end) {
            await state.finishInitialInput()
        }

        let chunks = SocketChunkStream(channel: state.channel) {
            Task { await state.cancel() }
        } consumptionOperation: { [weak self] _ in
            guard let self else { throw SessionTransportError.disconnected }
            try write(SessionFrame(kind: .window, id: frame.id, sequence: 1))
        }
        try write(SessionFrame(
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
            peerBuild: clientBuild,
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
            remove(frame.id)
        }
        await state.attach(task)
        if frame.deadlineUnixMilliseconds > 0 {
            let interval = max(0, frame.deadlineUnixMilliseconds - Int64(Date().timeIntervalSince1970 * 1000))
            Task {
                try? await Task.sleep(for: .milliseconds(interval))
                if !Task.isCancelled {
                    task.cancel()
                }
            }
        }
    }

    private func admit(_ frame: SessionFrame, clientBuild: String) throws -> Admission {
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
        if clientBuild != serverBuild, !["health", "shutdown", "handoff"].contains(frame.operation) {
            return .rejected("wire: client build does not match server build")
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

    private func receiveWindow(_ frame: SessionFrame) async throws {
        guard frame.flags.isEmpty, frame.sequence > 0, frame.operation.isEmpty,
              frame.tenant.isEmpty, frame.payload.isEmpty
        else { throw SessionTransportError.invalidFrame("response or event window") }
        if frame.id == 0 {
            await eventWindow.grant(frame.sequence)
            return
        }
        await request(frame.id)?.grantResponseCredits(frame.sequence)
    }

    private func request(_ id: UInt64) -> ServerRequestState? {
        lock.lock()
        let state = active[id]
        lock.unlock()
        return state
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
            try sendTerminal(terminal, id: id)
        case let .stream(stream):
            try await send(stream, id: id, state: state)
        }
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
                    try write(SessionFrame(kind: .stream, id: id, sequence: current, payload: payload))
                }
                let terminal = try await settlement.value().get()
                try Task.checkCancellation()
                try sendTerminal(terminal, id: id)
            } catch is CancellationError {
                stream.cancel()
                _ = await settlement.value()
                try sendTerminal(SocketTerminal(error: "wire: request canceled"), id: id)
            } catch {
                stream.cancel()
                _ = await settlement.value()
                try sendTerminal(SocketTerminal(error: String(describing: error)), id: id)
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

    private func sendTerminal(_ terminal: SocketTerminal, id: UInt64) throws {
        let envelope = try responseEnvelope(terminal)
        try write(SessionFrame(kind: .response, flags: .end, id: id, payload: envelope))
    }

    private func sendError(id: UInt64, _ error: Error) throws {
        try write(SessionFrame(
            kind: .response,
            flags: .end,
            id: id,
            payload: responseEnvelope(SocketTerminal(error: String(describing: error)))
        ))
    }

    private func sendRejected(id: UInt64, reason: String) throws {
        try write(SessionFrame(
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

    private func responseEnvelope(_ response: SocketTerminal) throws -> Data {
        var members: [String] = []
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
