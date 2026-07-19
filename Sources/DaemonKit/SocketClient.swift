import Darwin
import Foundation

/// One server-pushed event.
public struct SocketEvent: Sendable {
    public let topic: String
    public let payload: Data
}

/// One request multiplexed over a persistent ``SocketClient``.
public final class SocketCall: @unchecked Sendable {
    public let id: UInt64
    public let chunks: SocketChunkStream

    private let client: SocketClient
    private let state: ClientRequestState

    fileprivate init(client: SocketClient, id: UInt64, state: ClientRequestState) {
        self.client = client
        self.id = id
        self.state = state
        chunks = SocketChunkStream(channel: state.chunkChannel) { [client, state] in
            Self.cancel(client: client, id: id, state: state)
        } consumptionOperation: { [client] _ in
            try client.write(SessionFrame(kind: .window, id: id, sequence: 1))
        }
    }

    /// Appends an ordered request-stream chunk.
    public func sendChunk(_ payload: Data) async throws {
        try await state.sender.send(client: client, id: id, payload: payload, end: false)
    }

    /// Sends the request-stream terminal marker exactly once.
    public func closeSend() async throws {
        try await state.sender.send(client: client, id: id, payload: Data(), end: true)
    }

    /// Waits for the terminal response; task cancellation sends one cancel frame.
    public func response() async throws -> SocketTerminal {
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
        Self.cancel(client: client, id: id, state: state)
    }

    private static func cancel(client: SocketClient, id: UInt64, state: ClientRequestState) {
        state.cancelLock.lock()
        guard !state.cancelSent, !state.isTerminal() else {
            state.cancelLock.unlock()
            return
        }
        state.cancelSent = true
        state.cancelLock.unlock()
        state.endSending()
        state.discardOutput()
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

/// A persistent, multiplexed exact-v3 unix-socket client.
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
    public var events: SocketEventStream {
        SocketEventStream(channel: eventChannel) { [weak self] in
            self?.close()
        } consumptionOperation: { [weak self] _ in
            guard let self else { throw SessionTransportError.disconnected }
            try write(SessionFrame(kind: .window, sequence: 1))
        }
    }

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
    private let eventChannel: SocketBoundedChannel<SocketEvent>

    public init(path: String, build: String, configuration: Configuration = .init()) throws {
        guard !build.isEmpty else { throw SessionTransportError.handshake("empty build") }
        guard (1 ... Int(UInt32.max)).contains(configuration.streamQueueDepth),
              (1 ... Int(UInt32.max)).contains(configuration.eventQueueDepth)
        else { throw SessionTransportError.invalidFrame("stream queue exceeds protocol window") }
        self.configuration = configuration
        descriptor = try Self.connect(path: path)
        codec = SessionFrameCodec(descriptor: descriptor, maximumFrameBytes: configuration.maximumFrameBytes)
        eventChannel = SocketBoundedChannel(capacity: configuration.eventQueueDepth)
        Self.configure(descriptor, receive: configuration.handshakeTimeout, send: configuration.writeTimeout)
        do {
            peerBuild = try Self.handshake(codec: codec, build: build)
        } catch {
            Darwin.close(descriptor)
            throw error
        }
        Self.configure(descriptor, receive: 0, send: configuration.writeTimeout)
        try codec.write(SessionFrame(
            kind: .window,
            sequence: UInt32(configuration.eventQueueDepth)
        ))
        Task { [weak self] in
            await self?.readLoop()
        }
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
            try write(SessionFrame(
                kind: .window,
                id: id,
                sequence: UInt32(configuration.streamQueueDepth)
            ))
        } catch {
            fail(error)
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
    ) async throws -> SocketTerminal {
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
        Task { await eventChannel.finish(throwing: error) }
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

    private func readLoop() async {
        do {
            while true {
                let frame = try await readFrame()
                switch frame.kind {
                case .response:
                    try receiveResponse(frame)
                case .stream:
                    try await receiveStream(frame)
                case .event:
                    try await receiveEvent(frame)
                case .window:
                    try await receiveWindow(frame)
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

    private func readFrame() async throws -> SessionFrame {
        try await withCheckedThrowingContinuation { continuation in
            readQueue.async { [codec] in
                do {
                    try continuation.resume(returning: codec.read())
                } catch {
                    continuation.resume(throwing: error)
                }
            }
        }
    }

    private func receiveResponse(_ frame: SessionFrame) throws {
        guard frame.id != 0, frame.flags == .end, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("response")
        }
        let result = try Self.decodeResponse(frame.payload)
        guard let state = remove(frame.id) else { return }
        state.finish(returning: result)
    }

    private func receiveStream(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("response stream")
        }
        guard let state = pendingState(frame.id) else { return }
        guard let chunk = try state.receive(frame) else { return }
        let accepted = await state.chunkChannel.offer(chunk)
        guard accepted else {
            throw SessionTransportError.invalidFrame("response stream exceeded granted window")
        }
        if chunk.end, accepted {
            await state.chunkChannel.finish()
        }
    }

    private func receiveEvent(_ frame: SessionFrame) async throws {
        guard frame.id == 0, frame.flags == .end, !frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("event")
        }
        let accepted = await eventChannel.offer(SocketEvent(topic: frame.operation, payload: frame.payload))
        guard accepted else {
            throw SessionTransportError.invalidFrame("event stream exceeded granted window")
        }
    }

    private func receiveWindow(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.flags.isEmpty, frame.sequence > 0,
              frame.operation.isEmpty, frame.tenant.isEmpty, frame.payload.isEmpty
        else { throw SessionTransportError.invalidFrame("request stream window") }
        guard let state = pendingState(frame.id) else { return }
        await state.sender.grant(frame.sequence)
    }

    private func remove(_ id: UInt64) -> ClientRequestState? {
        lock.lock()
        defer { lock.unlock() }
        return pending.removeValue(forKey: id)
    }

    private func pendingState(_ id: UInt64) -> ClientRequestState? {
        lock.withLock { pending[id] }
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
        Task { await eventChannel.finish(throwing: error) }
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

    private static func decodeResponse(_ data: Data) throws -> SocketTerminal {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw SessionTransportError.invalidFrame("response JSON")
        }
        let payload: Data? = if let value = object["payload"] {
            try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
        } else {
            nil
        }
        return SocketTerminal(
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
    let results: AsyncThrowingStream<SocketTerminal, Error>
    let chunkChannel: SocketBoundedChannel<SocketRequestChunk>
    let resultContinuation: AsyncThrowingStream<SocketTerminal, Error>.Continuation
    let sender: ClientRequestSender
    let receiveLock = NSLock()
    let cancelLock = NSLock()
    let terminalLock = NSLock()
    var receiveSequence = SessionSequence()
    var receiveEnded = false
    var discardingOutput = false
    var cancelSent = false
    var terminalResult: SocketTerminal?
    var terminalError: Error?
    var settlementStarted = false
    var terminalReady = false

    init(streamQueueDepth: Int, sendEnded: Bool) {
        sender = ClientRequestSender(ended: sendEnded)
        chunkChannel = SocketBoundedChannel(capacity: streamQueueDepth)
        var resultContinuation: AsyncThrowingStream<SocketTerminal, Error>.Continuation!
        results = AsyncThrowingStream(bufferingPolicy: .bufferingOldest(1)) {
            resultContinuation = $0
        }
        self.resultContinuation = resultContinuation
    }

    func finish(returning result: SocketTerminal) {
        terminalLock.lock()
        settlementStarted = true
        terminalLock.unlock()
        endSending()
        receiveLock.lock()
        if !receiveEnded {
            receiveEnded = true
            Task { await chunkChannel.finish() }
        }
        receiveLock.unlock()
        terminalLock.lock()
        terminalResult = result
        terminalReady = true
        terminalLock.unlock()
        resultContinuation.yield(result)
        resultContinuation.finish()
    }

    func receive(_ frame: SessionFrame) throws -> SocketRequestChunk? {
        receiveLock.lock()
        defer { receiveLock.unlock() }
        if discardingOutput {
            return nil
        }
        guard !receiveEnded else {
            throw SessionTransportError.invalidFrame("response stream already ended")
        }
        let expected = try receiveSequence.take()
        guard frame.sequence == expected else {
            throw SessionTransportError.streamSequence(id: frame.id, got: frame.sequence, want: expected)
        }
        let ended = frame.flags.contains(.end)
        if ended {
            receiveEnded = true
        }
        return SocketRequestChunk(sequence: frame.sequence, payload: frame.payload, end: ended)
    }

    func finish(throwing error: Error) {
        terminalLock.lock()
        settlementStarted = true
        terminalLock.unlock()
        endSending()
        receiveLock.lock()
        if !receiveEnded {
            receiveEnded = true
            Task { await chunkChannel.finish(throwing: error) }
        }
        receiveLock.unlock()
        terminalLock.lock()
        terminalError = error
        terminalReady = true
        terminalLock.unlock()
        resultContinuation.finish(throwing: error)
    }

    func cachedResult() throws -> SocketTerminal? {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        guard terminalReady else { return nil }
        if let terminalError {
            throw terminalError
        }
        return terminalResult
    }

    func endSending() {
        Task { await sender.close() }
    }

    func discardOutput() {
        receiveLock.lock()
        receiveEnded = true
        discardingOutput = true
        Task { await chunkChannel.discard() }
        receiveLock.unlock()
    }

    func isTerminal() -> Bool {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        return settlementStarted
    }
}

private actor ClientRequestSender {
    private let window = SocketCreditWindow()
    private var sequence = SessionSequence()
    private var ended: Bool

    init(ended: Bool) {
        self.ended = ended
    }

    func send(client: SocketClient, id: UInt64, payload: Data, end: Bool) async throws {
        guard !ended else {
            throw SessionTransportError.invalidFrame("request stream already ended")
        }
        guard await window.acquire() else { throw CancellationError() }
        guard !ended else { throw CancellationError() }
        let current = try sequence.take()
        try client.write(SessionFrame(
            kind: .stream,
            flags: end ? .end : [],
            id: id,
            sequence: current,
            payload: payload
        ))
        ended = end
    }

    func grant(_ count: UInt32) async {
        await window.grant(count)
    }

    func close() async {
        ended = true
        await window.close()
    }
}

private extension NSLock {
    func withLock<Result>(_ body: () throws -> Result) rethrows -> Result {
        lock()
        defer { unlock() }
        return try body()
    }
}
