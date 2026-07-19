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
    public let chunks: AsyncStream<SocketRequestChunk>
    public let peer: SocketPeer
    public let peerBuild: String
    public let session: SocketSession
}

/// A terminal response with an optional ordered output stream.
public struct SocketResponse: Sendable {
    public let payload: Data?
    public let error: String?
    public let rejected: Bool
    public let reason: String?
    public let chunks: AsyncStream<Data>?

    public init(
        payload: Data? = nil,
        error: String? = nil,
        rejected: Bool = false,
        reason: String? = nil,
        chunks: AsyncStream<Data>? = nil
    ) {
        self.payload = payload
        self.error = error
        self.rejected = rejected
        self.reason = reason
        self.chunks = chunks
    }
}

/// A trusted persistent server session exposed to request handlers.
public final class SocketSession: @unchecked Sendable {
    fileprivate weak var implementation: ServerSession?

    fileprivate init(implementation: ServerSession) {
        self.implementation = implementation
    }

    /// Pushes one event to the peer on the session's serialized writer.
    public func pushEvent(topic: String, payload: Data = Data()) throws {
        guard !topic.isEmpty else {
            throw SessionTransportError.invalidFrame("empty event topic")
        }
        guard let implementation else {
            throw SessionTransportError.disconnected
        }
        try implementation.write(SessionFrame(kind: .event, flags: .end, operation: topic, payload: payload))
    }
}

/// A unix-domain persistent v2 session server.
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
    private let acceptQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.accept")
    private let sessionQueue = DispatchQueue(
        label: "com.yasyf.daemonkit.SocketServer.sessions",
        attributes: .concurrent
    )
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
            sessionQueue.async {
                self.serve(descriptor)
                self.sessionGroup.leave()
            }
        }
        close(listenerDescriptor)
    }

    private func serve(_ descriptor: Int32) {
        defer {
            lock.lock()
            connections.remove(descriptor)
            lock.unlock()
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
            lock.lock()
            sessions[descriptor] = session
            lock.unlock()
            defer {
                lock.lock()
                sessions.removeValue(forKey: descriptor)
                lock.unlock()
            }
            try session.run()
        } catch {
            socketServerLog.debug("session ended: \(String(describing: error), privacy: .public)")
        }
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

private final class ServerRequestState: @unchecked Sendable {
    let continuation: AsyncStream<SocketRequestChunk>.Continuation
    var task: Task<Void, Never>?
    var nextSequence: UInt32 = 0
    var ended = false
    var transportError: Error?

    init(continuation: AsyncStream<SocketRequestChunk>.Continuation) {
        self.continuation = continuation
    }
}

private final class ServerSession: @unchecked Sendable {
    let descriptor: Int32
    private let serverBuild: String
    private let peer: SocketPeer
    private let configuration: SocketServer.Configuration
    private let handler: @Sendable (SocketRequest) async -> SocketResponse
    private let codec: SessionFrameCodec
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
    }

    func run() throws {
        let clientBuild = try handshake()
        var noTimeout = timeval()
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &noTimeout, socklen_t(MemoryLayout<timeval>.size))
        while true {
            let frame = try codec.read()
            switch frame.kind {
            case .request:
                try receiveRequest(frame, clientBuild: clientBuild)
            case .cancel:
                try receiveCancel(frame)
            case .stream:
                try receiveStream(frame)
            case .goAway:
                close()
                return
            default:
                throw SessionTransportError.invalidFrame("client frame kind \(frame.kind)")
            }
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

    func close() {
        lock.lock()
        guard !closed else {
            lock.unlock()
            return
        }
        closed = true
        let requests = Array(active.values)
        active.removeAll()
        lock.unlock()
        for request in requests {
            request.task?.cancel()
            request.continuation.finish()
        }
        shutdown(descriptor, SHUT_RDWR)
    }

    private func handshake() throws -> String {
        let frame = try codec.read()
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

    private func receiveRequest(_ frame: SessionFrame, clientBuild: String) throws {
        guard frame.id != 0, !frame.operation.isEmpty, frame.sequence == 0 else {
            throw SessionTransportError.invalidFrame("request")
        }
        lock.lock()
        guard frame.id > watermark, !seen.contains(frame.id) else {
            lock.unlock()
            throw SessionTransportError.duplicateRequestID(frame.id)
        }
        guard frame.id - watermark <= UInt64(configuration.maximumActiveRequests),
              active.count < configuration.maximumActiveRequests
        else {
            lock.unlock()
            try sendRejected(id: frame.id, reason: "wire: queue at capacity")
            return
        }
        seen.insert(frame.id)
        while seen.remove(watermark + 1) != nil {
            watermark += 1
        }
        if clientBuild != serverBuild, !["health", "shutdown", "handoff"].contains(frame.operation) {
            lock.unlock()
            try sendRejected(id: frame.id, reason: "wire: client build does not match server build")
            return
        }
        var continuation: AsyncStream<SocketRequestChunk>.Continuation!
        let policy = AsyncStream<SocketRequestChunk>.Continuation.BufferingPolicy.bufferingOldest(
            configuration.streamQueueDepth
        )
        let chunks = AsyncStream<SocketRequestChunk>(bufferingPolicy: policy) {
            continuation = $0
        }
        let state = ServerRequestState(continuation: continuation)
        if frame.flags.contains(.end) {
            state.ended = true
            continuation.finish()
        }
        active[frame.id] = state
        lock.unlock()

        let publicSession = SocketSession(implementation: self)
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
            let transportError = transportError(for: state)
            if let transportError {
                response = SocketResponse(error: String(describing: transportError))
            }
            do {
                try await send(response, id: frame.id)
            } catch {
                socketServerLog.debug("response failed: \(String(describing: error), privacy: .public)")
            }
            remove(frame.id)
        }
        state.task = task
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

    private func receiveCancel(_ frame: SessionFrame) throws {
        guard frame.id != 0, frame.flags == .end, frame.operation.isEmpty,
              frame.tenant.isEmpty, frame.payload.isEmpty
        else {
            throw SessionTransportError.invalidFrame("cancel")
        }
        lock.lock()
        let state = active[frame.id]
        if let state, !state.ended {
            state.ended = true
            state.continuation.finish()
        }
        lock.unlock()
        state?.task?.cancel()
    }

    private func receiveStream(_ frame: SessionFrame) throws {
        guard frame.id != 0, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("stream")
        }
        lock.lock()
        guard let state = active[frame.id] else {
            lock.unlock()
            return
        }
        guard !state.ended, frame.sequence == state.nextSequence else {
            let expected = state.nextSequence
            state.transportError = SessionTransportError.streamSequence(
                id: frame.id,
                got: frame.sequence,
                want: expected
            )
            lock.unlock()
            state.task?.cancel()
            return
        }
        state.nextSequence += 1
        let result = state.continuation.yield(SocketRequestChunk(
            sequence: frame.sequence,
            payload: frame.payload,
            end: frame.flags.contains(.end)
        ))
        if case .dropped = result {
            state.transportError = SessionTransportError.queueFull
            lock.unlock()
            state.task?.cancel()
            return
        }
        if frame.flags.contains(.end) {
            state.ended = true
            state.continuation.finish()
        }
        lock.unlock()
    }

    private func send(_ response: SocketResponse, id: UInt64) async throws {
        if let chunks = response.chunks {
            var sequence: UInt32 = 0
            for await payload in chunks {
                try write(SessionFrame(kind: .stream, id: id, sequence: sequence, payload: payload))
                sequence += 1
            }
            try write(SessionFrame(kind: .stream, flags: .end, id: id, sequence: sequence))
        }
        let envelope = try responseEnvelope(response)
        try write(SessionFrame(kind: .response, flags: .end, id: id, payload: envelope))
    }

    private func sendError(id: UInt64, _ error: Error) throws {
        try write(SessionFrame(
            kind: .response,
            flags: .end,
            id: id,
            payload: responseEnvelope(SocketResponse(error: String(describing: error)))
        ))
    }

    private func sendRejected(id: UInt64, reason: String) throws {
        try write(SessionFrame(
            kind: .response,
            flags: .end,
            id: id,
            payload: responseEnvelope(SocketResponse(rejected: true, reason: reason))
        ))
    }

    private func remove(_ id: UInt64) {
        lock.lock()
        active.removeValue(forKey: id)
        lock.unlock()
    }

    private func transportError(for state: ServerRequestState) -> Error? {
        lock.lock()
        defer { lock.unlock() }
        return state.transportError
    }

    private func responseEnvelope(_ response: SocketResponse) throws -> Data {
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
