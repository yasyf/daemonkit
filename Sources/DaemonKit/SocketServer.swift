import Darwin
import Foundation
import os

/// A unix-domain persistent v1 session server.
final class SocketServer: @unchecked Sendable {
    struct Configuration: Sendable {
        var maximumFrameBytes: Int
        var maximumActiveRequests: Int
        var maximumSessions: Int
        var streamQueueDepth: Int
        var maximumPendingWrites: Int
        var handshakeTimeout: TimeInterval
        var writeTimeout: TimeInterval

        init(
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

    private enum StopAction {
        case settle(Int32)
        case waitForStart
        case waitForStop
        case finishIdle
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
    private let runtimeLifecycle: RuntimeLifecycleController?
    private let controlOperations: Set<String>
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
    private var sessionReservations = 0
    var stopDrainHook: (@Sendable () async -> Void)?
    var startCommitHook: (@Sendable () async -> Void)?
    var stopFinishHook: (@Sendable () async -> Void)?
    var stopWaitHook: (@Sendable () async -> Void)?

    init(
        path: String,
        wireBuild: String,
        configuration: Configuration = .init(),
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.path = path
        self.wireBuild = wireBuild
        self.configuration = configuration
        runtimeLifecycle = nil
        controlOperations = []
        self.handler = handler
    }

    init(
        path: String,
        wireBuild: String,
        configuration: Configuration = .init(),
        runtimeLifecycle: RuntimeLifecycleController,
        controlOperations: Set<String> = [],
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.path = path
        self.wireBuild = wireBuild
        self.configuration = configuration
        self.runtimeLifecycle = runtimeLifecycle
        self.controlOperations = controlOperations
        self.handler = handler
    }
}

extension SocketServer {
    /// Reclaims a stale socket, binds with mode 0600, and starts accepting sessions.
    func start() async throws {
        guard !wireBuild.isEmpty else { throw SocketServerError.emptyWireBuild }
        guard runtimeLifecycle?.wireBuild == nil || runtimeLifecycle?.wireBuild == wireBuild else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime lifecycle wire build")
        }
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
                        self.runtimeLifecycle?.markServerLive()
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
    func stop() async {
        try? await stop(deadline: nil)
    }

    func stopRuntime(deadline: Date) async throws {
        guard deadline > Date() else { throw RuntimeShutdownError.deadlineExceeded }
        try await stop(deadline: deadline)
    }

    private func stop(deadline: Date?) async throws {
        switch stopAction() {
        case .finishIdle:
            startLatch.finish()
            stopLatch.finish()
        case .waitForStart:
            try await waitForStartAndStop(deadline: deadline)
        case .waitForStop:
            try await waitForStop(deadline: deadline)
        case let .settle(writeDescriptor):
            try await settleStop(writeDescriptor: writeDescriptor, deadline: deadline)
        }
    }

    private func stopAction() -> StopAction {
        lock.withLock {
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
    }

    private func waitForStartAndStop(deadline: Date?) async throws {
        if let deadline {
            try await startLatch.wait(deadline: deadline)
            try await stopLatch.wait(deadline: deadline)
        } else {
            await startLatch.wait()
            await stopLatch.wait()
        }
    }

    private func waitForStop(deadline: Date?) async throws {
        if deadline == nil {
            await stopWaitHook?()
        }
        if let deadline {
            try await stopLatch.wait(deadline: deadline)
        } else {
            await stopLatch.wait()
        }
    }

    private func settleStop(writeDescriptor: Int32, deadline: Date?) async throws {
        var byte: UInt8 = 1
        while Darwin.write(writeDescriptor, &byte, 1) < 0 {
            if errno == EINTR {
                continue
            }
            // A full nonblocking pipe already has a pending shutdown signal.
            break
        }
        let acceptLoopSettled = AsyncLatch()
        acceptQueue.async {
            acceptLoopSettled.finish()
        }
        if let deadline {
            try await acceptLoopSettled.wait(deadline: deadline)
        } else {
            await acceptLoopSettled.wait()
        }
        if deadline == nil {
            await stopDrainHook?()
        }
        let (active, connectionSnapshot): ([ServerSession], [ServerConnection]) = lock.withLock {
            (Array(sessions.values), Array(connections))
        }
        for session in active {
            session.close()
        }
        for connection in connectionSnapshot {
            connection.shutdown()
        }
        let settled = await withCheckedContinuation { continuation in
            DispatchQueue.global().async { [sessionGroup] in
                guard let deadline else {
                    sessionGroup.wait()
                    continuation.resume(returning: true)
                    return
                }
                let remaining = deadline.timeIntervalSinceNow
                guard remaining > 0 else {
                    continuation.resume(returning: false)
                    return
                }
                let result = sessionGroup.wait(timeout: .now() + remaining)
                continuation.resume(returning: result == .success)
            }
        }
        guard settled else { throw RuntimeShutdownError.deadlineExceeded }
        try releaseListenerResources(deadline: deadline)
        if deadline == nil {
            await stopFinishHook?()
        }
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

    private func releaseListenerResources(deadline: Date?) throws {
        // The listener path and descriptors are one synchronously released ownership unit.
        try requireShutdownBudget(deadline)
        if listenerDescriptor >= 0 {
            Darwin.close(listenerDescriptor)
            listenerDescriptor = -1
        }
        if shutdownReadDescriptor >= 0 {
            Darwin.close(shutdownReadDescriptor)
            shutdownReadDescriptor = -1
        }
        if shutdownWriteDescriptor >= 0 {
            Darwin.close(shutdownWriteDescriptor)
            shutdownWriteDescriptor = -1
        }
        unlink(path)
        lock.withLock { state = .stopped }
    }

    private func requireShutdownBudget(_ deadline: Date?) throws {
        if let deadline, deadline <= Date() {
            throw RuntimeShutdownError.deadlineExceeded
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
        var terminalError: (any Error)?
        defer { runtimeLifecycle?.markServerTerminal(terminalError) }
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
                terminalError = SessionTransportError.systemCall(operation: "poll", errno: errno)
                break
            }
            if descriptors[1].revents != 0 {
                break
            }
            guard descriptors[0].revents & Int16(POLLIN) != 0 else {
                terminalError = SessionTransportError.systemCall(operation: "poll", errno: EIO)
                break
            }
            let descriptor = accept(listenerDescriptor, nil, nil)
            if descriptor < 0 {
                if errno == EINTR || errno == ECONNABORTED {
                    continue
                }
                terminalError = SessionTransportError.systemCall(operation: "accept", errno: errno)
                socketServerLog.error("accept failed: \(String(cString: strerror(errno)), privacy: .public)")
                break
            }
            let connection = ServerConnection(descriptor: descriptor)
            lock.lock()
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
            try await setupQueue.performIO {
                try self.configure(descriptor)
            }
            let peer = try await setupQueue.performIO {
                var user = uid_t()
                var group = gid_t()
                guard getpeereid(descriptor, &user, &group) == 0 else {
                    throw SessionTransportError.systemCall(operation: "getpeereid", errno: errno)
                }
                return SocketPeer(effectiveUserID: user, effectiveGroupID: group)
            }
            guard reserveSession() else {
                try? await rejectHandshake(
                    descriptor: descriptor,
                    queue: setupQueue,
                    code: .sessionCapacity,
                    reason: "wire: session capacity exhausted"
                )
                return
            }
            defer { releaseSession() }
            let session = ServerSession(
                descriptor: descriptor,
                shutdown: { connection.shutdown() },
                wireBuild: wireBuild,
                peer: peer,
                configuration: configuration,
                runtimeLifecycle: runtimeLifecycle,
                controlOperations: controlOperations,
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

    private func reserveSession() -> Bool {
        lock.withLock {
            guard sessionReservations < configuration.maximumSessions else { return false }
            sessionReservations += 1
            return true
        }
    }

    private func releaseSession() {
        lock.withLock {
            precondition(sessionReservations > 0)
            sessionReservations -= 1
        }
    }

    private func rejectHandshake(
        descriptor: Int32,
        queue: DispatchQueue,
        code: SocketResponseCode,
        reason: String
    ) async throws {
        try await queue.performIO {
            let codec = SessionFrameCodec(
                descriptor: descriptor,
                maximumFrameBytes: self.configuration.maximumFrameBytes,
                writeTimeout: self.configuration.writeTimeout
            )
            let payload = try SessionHandshakeCodec.encodeRejection(
                wireBuild: self.wireBuild,
                code: code,
                reason: reason
            )
            try codec.write(SessionFrame(kind: .helloAck, flags: .end, payload: payload))
        }
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
