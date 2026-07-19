import Darwin
import Foundation

/// One terminal result from a persistent ``SocketClient`` call.
public struct SocketCallResult: Sendable {
    public let payload: Data?
    public let error: String?
    public let rejected: Bool
    public let reason: String?
}

/// One server-pushed event.
public struct SocketEvent: Sendable {
    public let topic: String
    public let payload: Data
}

/// One request multiplexed over a persistent ``SocketClient``.
public final class SocketCall: @unchecked Sendable {
    public let id: UInt64
    public let chunks: AsyncThrowingStream<SocketRequestChunk, Error>

    private let client: SocketClient
    private let state: ClientRequestState

    fileprivate init(client: SocketClient, id: UInt64, state: ClientRequestState) {
        self.client = client
        self.id = id
        self.state = state
        chunks = state.chunks
    }

    /// Appends an ordered request-stream chunk.
    public func sendChunk(_ payload: Data) throws {
        try state.sendLock.withLock {
            guard !state.sendEnded else {
                throw SessionTransportError.invalidFrame("request stream already ended")
            }
            try client.write(SessionFrame(kind: .stream, id: id, sequence: state.nextSend, payload: payload))
            state.nextSend += 1
        }
    }

    /// Sends the request-stream terminal marker exactly once.
    public func closeSend() throws {
        try state.sendLock.withLock {
            guard !state.sendEnded else {
                throw SessionTransportError.invalidFrame("request stream already ended")
            }
            try client.write(SessionFrame(kind: .stream, flags: .end, id: id, sequence: state.nextSend))
            state.nextSend += 1
            state.sendEnded = true
        }
    }

    /// Waits for the terminal response; task cancellation sends one cancel frame.
    public func response() async throws -> SocketCallResult {
        if let cached = try state.cachedResult() {
            return cached
        }
        return try await withTaskCancellationHandler {
            for try await result in state.results {
                return result
            }
            if let cached = try state.cachedResult() {
                return cached
            }
            throw SessionTransportError.disconnected
        } onCancel: {
            self.cancel()
        }
    }

    /// Requests cancellation without disconnecting the session.
    public func cancel() {
        state.cancelLock.lock()
        guard !state.cancelSent, !state.isTerminal() else {
            state.cancelLock.unlock()
            return
        }
        state.cancelSent = true
        state.cancelLock.unlock()
        state.endSending()
        do {
            try client.write(SessionFrame(kind: .cancel, flags: .end, id: id))
        } catch {
            client.fail(error)
            return
        }
        Task { [client, state] in
            let nanoseconds = UInt64(max(0, client.configuration.cancellationSettlementTimeout) * 1_000_000_000)
            try? await Task.sleep(nanoseconds: nanoseconds)
            if !state.isTerminal() {
                client.fail(SessionTransportError.cancellationDidNotSettle)
            }
        }
    }
}

/// A persistent, multiplexed exact-v2 unix-socket client.
public final class SocketClient: @unchecked Sendable {
    public struct Configuration: Sendable {
        public var maximumFrameBytes: Int
        public var streamQueueDepth: Int
        public var eventQueueDepth: Int
        public var handshakeTimeout: TimeInterval
        public var writeTimeout: TimeInterval
        public var cancellationSettlementTimeout: TimeInterval

        public init(
            maximumFrameBytes: Int = daemonKitDefaultMaximumFrameBytes,
            streamQueueDepth: Int = 16,
            eventQueueDepth: Int = 16,
            handshakeTimeout: TimeInterval = 10,
            writeTimeout: TimeInterval = 10,
            cancellationSettlementTimeout: TimeInterval = 5
        ) {
            self.maximumFrameBytes = maximumFrameBytes
            self.streamQueueDepth = streamQueueDepth
            self.eventQueueDepth = eventQueueDepth
            self.handshakeTimeout = handshakeTimeout
            self.writeTimeout = writeTimeout
            self.cancellationSettlementTimeout = cancellationSettlementTimeout
        }
    }

    /// Events pushed by the server, bounded by ``Configuration/eventQueueDepth``.
    public let events: AsyncThrowingStream<SocketEvent, Error>
    /// Server build identity established by the mandatory handshake.
    public let peerBuild: String

    private let descriptor: Int32
    private let codec: SessionFrameCodec
    fileprivate let configuration: Configuration
    private let readQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketClient.read")
    private let lock = NSLock()
    private var pending: [UInt64: ClientRequestState] = [:]
    private var nextID: UInt64 = 1
    private var closed = false
    private let eventContinuation: AsyncThrowingStream<SocketEvent, Error>.Continuation

    public init(path: String, build: String, configuration: Configuration = .init()) throws {
        guard !build.isEmpty else { throw SessionTransportError.handshake("empty build") }
        self.configuration = configuration
        descriptor = try Self.connect(path: path)
        codec = SessionFrameCodec(descriptor: descriptor, maximumFrameBytes: configuration.maximumFrameBytes)
        var continuation: AsyncThrowingStream<SocketEvent, Error>.Continuation!
        events = AsyncThrowingStream(bufferingPolicy: .bufferingOldest(configuration.eventQueueDepth)) {
            continuation = $0
        }
        eventContinuation = continuation
        Self.configure(descriptor, receive: configuration.handshakeTimeout, send: configuration.writeTimeout)
        do {
            peerBuild = try Self.handshake(codec: codec, build: build)
        } catch {
            Darwin.close(descriptor)
            throw error
        }
        Self.configure(descriptor, receive: 0, send: configuration.writeTimeout)
        readQueue.async { [weak self] in self?.readLoop() }
    }

    deinit {
        close()
    }

    /// Opens a request. Set endInput false when request chunks will follow.
    public func open(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        endInput: Bool = true,
        deadline: Date? = nil
    ) throws -> SocketCall {
        guard !operation.isEmpty else { throw SessionTransportError.invalidFrame("empty operation") }
        lock.lock()
        guard !closed else {
            lock.unlock()
            throw SessionTransportError.disconnected
        }
        let id = nextID
        nextID += 1
        let state = ClientRequestState(streamQueueDepth: configuration.streamQueueDepth, sendEnded: endInput)
        pending[id] = state
        lock.unlock()
        let milliseconds = deadline.map { Int64($0.timeIntervalSince1970 * 1000) } ?? 0
        do {
            try write(SessionFrame(
                kind: .request,
                flags: endInput ? .end : [],
                id: id,
                deadlineUnixMilliseconds: milliseconds,
                operation: operation,
                tenant: tenant,
                payload: payload
            ))
        } catch {
            _ = remove(id)
            throw error
        }
        return SocketCall(client: self, id: id, state: state)
    }

    /// Sends a unary request and waits for its terminal response.
    public func call(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        deadline: Date? = nil
    ) async throws -> SocketCallResult {
        try await open(
            operation: operation,
            tenant: tenant,
            payload: payload,
            deadline: deadline
        ).response()
    }

    /// Closes the session and fails every pending call.
    public func close() {
        lock.lock()
        guard !closed else {
            lock.unlock()
            return
        }
        closed = true
        let requests = Array(pending.values)
        pending.removeAll()
        lock.unlock()
        try? codec.write(SessionFrame(kind: .goAway, flags: .end))
        shutdown(descriptor, SHUT_RDWR)
        Darwin.close(descriptor)
        let error = SessionTransportError.disconnected
        for request in requests {
            request.finish(throwing: error)
        }
        eventContinuation.finish(throwing: error)
    }

    fileprivate func write(_ frame: SessionFrame) throws {
        lock.lock()
        let isClosed = closed
        lock.unlock()
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try codec.write(frame)
    }

    private func readLoop() {
        do {
            while true {
                let frame = try codec.read()
                switch frame.kind {
                case .response:
                    try receiveResponse(frame)
                case .stream:
                    try receiveStream(frame)
                case .event:
                    try receiveEvent(frame)
                case .goAway:
                    close()
                    return
                default:
                    throw SessionTransportError.invalidFrame("server frame kind \(frame.kind)")
                }
            }
        } catch {
            fail(error)
        }
    }

    private func receiveResponse(_ frame: SessionFrame) throws {
        guard frame.id != 0, frame.flags == .end, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("response")
        }
        guard let state = remove(frame.id) else { return }
        let result = try Self.decodeResponse(frame.payload)
        state.finish(returning: result)
    }

    private func receiveStream(_ frame: SessionFrame) throws {
        guard frame.id != 0, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("response stream")
        }
        lock.lock()
        let state = pending[frame.id]
        lock.unlock()
        guard let state else { return }
        state.receiveLock.lock()
        guard !state.receiveEnded, frame.sequence == state.nextReceive else {
            state.receiveLock.unlock()
            throw SessionTransportError.streamSequence(id: frame.id, got: frame.sequence, want: state.nextReceive)
        }
        state.nextReceive += 1
        let result = state.chunkContinuation.yield(SocketRequestChunk(
            sequence: frame.sequence,
            payload: frame.payload,
            end: frame.flags.contains(.end)
        ))
        if case .dropped = result {
            state.receiveLock.unlock()
            _ = remove(frame.id)
            state.finish(throwing: SessionTransportError.queueFull)
            try? write(SessionFrame(kind: .cancel, flags: .end, id: frame.id))
            return
        }
        if frame.flags.contains(.end) {
            state.receiveEnded = true
            state.chunkContinuation.finish()
        }
        state.receiveLock.unlock()
    }

    private func receiveEvent(_ frame: SessionFrame) throws {
        guard frame.id == 0, frame.flags == .end, !frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("event")
        }
        let result = eventContinuation.yield(SocketEvent(topic: frame.operation, payload: frame.payload))
        if case .dropped = result {
            throw SessionTransportError.queueFull
        }
    }

    private func remove(_ id: UInt64) -> ClientRequestState? {
        lock.lock()
        defer { lock.unlock() }
        return pending.removeValue(forKey: id)
    }

    fileprivate func fail(_ error: Error) {
        lock.lock()
        guard !closed else {
            lock.unlock()
            return
        }
        closed = true
        let requests = Array(pending.values)
        pending.removeAll()
        lock.unlock()
        shutdown(descriptor, SHUT_RDWR)
        Darwin.close(descriptor)
        for request in requests {
            request.finish(throwing: error)
        }
        eventContinuation.finish(throwing: error)
    }

    private static func handshake(codec: SessionFrameCodec, build: String) throws -> String {
        let payload = try JSONEncoder().encode(SessionBuildIdentity(
            protocolVersion: daemonKitSessionProtocolVersion,
            build: build
        ))
        try codec.write(SessionFrame(kind: .hello, flags: .end, payload: payload))
        let response = try codec.read()
        guard response.kind == .helloAck, response.flags == .end, response.id == 0,
              response.sequence == 0, response.operation.isEmpty, response.tenant.isEmpty
        else {
            throw SessionTransportError.handshake("invalid acknowledgment")
        }
        let identity = try JSONDecoder().decode(SessionBuildIdentity.self, from: response.payload)
        guard identity.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(identity.protocolVersion)
        }
        guard !identity.build.isEmpty else { throw SessionTransportError.handshake("empty server build") }
        return identity.build
    }

    private static func decodeResponse(_ data: Data) throws -> SocketCallResult {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw SessionTransportError.invalidFrame("response JSON")
        }
        let payload: Data? = if let value = object["payload"] {
            try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
        } else {
            nil
        }
        return SocketCallResult(
            payload: payload,
            error: object["err"] as? String,
            rejected: object["rejected"] as? Bool ?? false,
            reason: object["reason"] as? String
        )
    }

    private static func connect(path: String) throws -> Int32 {
        var address = try makeAddress(path: path)
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else {
            throw SessionTransportError.systemCall(operation: "socket", errno: errno)
        }
        let connected = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard connected == 0 else {
            let code = errno
            Darwin.close(descriptor)
            throw SessionTransportError.systemCall(operation: "connect", errno: code)
        }
        var enable: Int32 = 1
        setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enable, socklen_t(MemoryLayout<Int32>.size))
        return descriptor
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

    private static func configure(_ descriptor: Int32, receive: TimeInterval, send: TimeInterval) {
        var receiveTimeout = timeval(
            tv_sec: Int(receive),
            tv_usec: Int32((receive - Double(Int(receive))) * 1_000_000)
        )
        var sendTimeout = timeval(
            tv_sec: Int(send),
            tv_usec: Int32((send - Double(Int(send))) * 1_000_000)
        )
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &receiveTimeout, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(descriptor, SOL_SOCKET, SO_SNDTIMEO, &sendTimeout, socklen_t(MemoryLayout<timeval>.size))
    }
}

private final class ClientRequestState: @unchecked Sendable {
    let chunks: AsyncThrowingStream<SocketRequestChunk, Error>
    let results: AsyncThrowingStream<SocketCallResult, Error>
    let chunkContinuation: AsyncThrowingStream<SocketRequestChunk, Error>.Continuation
    let resultContinuation: AsyncThrowingStream<SocketCallResult, Error>.Continuation
    let sendLock = NSLock()
    let receiveLock = NSLock()
    let cancelLock = NSLock()
    let terminalLock = NSLock()
    var nextSend: UInt32 = 0
    var sendEnded: Bool
    var nextReceive: UInt32 = 0
    var receiveEnded = false
    var cancelSent = false
    var terminalResult: SocketCallResult?
    var terminalError: Error?
    var settlementStarted = false
    var terminalReady = false

    init(streamQueueDepth: Int, sendEnded: Bool) {
        var chunkContinuation: AsyncThrowingStream<SocketRequestChunk, Error>.Continuation!
        chunks = AsyncThrowingStream(bufferingPolicy: .bufferingOldest(streamQueueDepth)) {
            chunkContinuation = $0
        }
        self.chunkContinuation = chunkContinuation
        var resultContinuation: AsyncThrowingStream<SocketCallResult, Error>.Continuation!
        results = AsyncThrowingStream(bufferingPolicy: .bufferingOldest(1)) {
            resultContinuation = $0
        }
        self.resultContinuation = resultContinuation
        self.sendEnded = sendEnded
    }

    func finish(returning result: SocketCallResult) {
        terminalLock.lock()
        settlementStarted = true
        terminalLock.unlock()
        endSending()
        receiveLock.lock()
        if !receiveEnded {
            receiveEnded = true
            chunkContinuation.finish()
        }
        receiveLock.unlock()
        terminalLock.lock()
        terminalResult = result
        terminalReady = true
        terminalLock.unlock()
        resultContinuation.yield(result)
        resultContinuation.finish()
    }

    func finish(throwing error: Error) {
        terminalLock.lock()
        settlementStarted = true
        terminalLock.unlock()
        endSending()
        receiveLock.lock()
        if !receiveEnded {
            receiveEnded = true
            chunkContinuation.finish(throwing: error)
        }
        receiveLock.unlock()
        terminalLock.lock()
        terminalError = error
        terminalReady = true
        terminalLock.unlock()
        resultContinuation.finish(throwing: error)
    }

    func cachedResult() throws -> SocketCallResult? {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        guard terminalReady else { return nil }
        if let terminalError {
            throw terminalError
        }
        return terminalResult
    }

    func endSending() {
        sendLock.lock()
        sendEnded = true
        sendLock.unlock()
    }

    func isTerminal() -> Bool {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        return settlementStarted
    }
}

private extension NSLock {
    func withLock<Result>(_ body: () throws -> Result) rethrows -> Result {
        lock()
        defer { unlock() }
        return try body()
    }
}
