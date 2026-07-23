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

    private let client: SocketClientCore
    private let owner: SocketClient
    private let state: ClientRequestState

    fileprivate init(owner: SocketClient, client: SocketClientCore, id: UInt64, state: ClientRequestState) {
        self.owner = owner
        self.client = client
        self.id = id
        self.state = state
        chunks = SocketChunkStream(channel: state.chunkChannel) { [client, state] in
            Task { await Self.cancel(client: client, id: id, state: state) }
        } consumptionOperation: { [client] _ in
            try await client.writeSettlement(SessionFrame(kind: .window, id: id, sequence: 1))
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
            try Task.checkCancellation()
            throw SessionTransportError.disconnected
        } onCancel: {
            Task { await self.cancel() }
        }
    }

    /// Requests cancellation without disconnecting the session.
    public func cancel() async {
        await Self.cancel(client: client, id: id, state: state)
    }

    fileprivate static func cancel(client: SocketClientCore, id: UInt64, state: ClientRequestState) async {
        let shouldCancel = state.cancelLock.withLock {
            guard !state.cancelSent, !state.isTerminal() else { return false }
            state.cancelSent = true
            return true
        }
        guard shouldCancel else { return }
        await state.cancelIO()
        guard !state.isTerminal() else { return }
        do {
            try await client.writeSettlement(SessionFrame(kind: .cancel, flags: .end, id: id))
        } catch {
            client.fail(error)
            return
        }
        let nanoseconds = SessionFrameCodec.durationNanoseconds(
            client.configuration.cancellationSettlementTimeout
        )
        let timer = Task { [weak client, weak state] in
            try? await Task.sleep(nanoseconds: nanoseconds)
            guard let client, let state else { return }
            await client.cancellationTimeoutHook?()
            let error = SessionTransportError.cancellationDidNotSettle
            let won = await state.finish(throwing: error)
            await client.cancellationTimeoutResultHook?(won)
            if won {
                client.fail(error)
            }
        }
        state.attachCancellationTimer(timer)
    }
}

/// A persistent, multiplexed exact-v1 unix-socket client.
public final class SocketClient: @unchecked Sendable {
    let core: SocketClientCore

    public init(
        path: String,
        build: String,
        configuration: Configuration = .init(),
        trust: PeerTrust
    ) async throws {
        core = try await SocketClientCore(
            path: path,
            build: build,
            configuration: configuration,
            trust: trust
        )
    }

    deinit {
        core.abort()
    }
}

final class SocketClientCore: @unchecked Sendable {
    private enum CloseState {
        case open
        case closing([ClientRequestState])
        case closed
    }

    private struct Bootstrap {
        let descriptor: Int32
        let codec: SessionFrameCodec
        let writer: SessionWriter
        let readQueue: DispatchQueue
        let peerBuild: String
        let sessionGeneration: Data
    }

    /// Events pushed by the server, bounded by ``Configuration/eventQueueDepth``.
    var events: SocketEventStream {
        SocketEventStream(channel: eventChannel) { [weak self] in
            Task { await self?.close() }
        } consumptionOperation: { [weak self] _ in
            guard let self else { throw SessionTransportError.disconnected }
            try await writeSettlement(SessionFrame(kind: .window, sequence: 1))
        }
    }

    /// Server build identity established by the mandatory handshake.
    let peerBuild: String
    private let sessionGeneration: Data

    private let descriptor: Int32
    private let codec: SessionFrameCodec
    private let writer: SessionWriter
    fileprivate let configuration: SocketClient.Configuration
    private let readQueue: DispatchQueue
    private let lock = NSLock()
    private let closeLatch = AsyncLatch()
    private var pending: [UInt64: ClientRequestState] = [:]
    private var nextID: UInt64 = 1
    private var closeState = CloseState.open
    private let eventChannel: SocketBoundedChannel<SocketEvent>
    var openCommitHook: (@Sendable () async -> Void)?
    var requestSettlementHook: (@Sendable () async -> Void)?
    var requestSettlementWaitHook: (@Sendable () async -> Void)?
    var requestSendAdmissionHook: (@Sendable () async -> Void)?
    var requestSendHook: (@Sendable () async -> Void)?
    var requestSendDrainWaitHook: (@Sendable () -> Void)?
    var receiveStreamOfferHook: (@Sendable () async -> Void)?
    var cancellationTimeoutHook: (@Sendable () async -> Void)?
    var cancellationTimeoutResultHook: (@Sendable (Bool) async -> Void)?

    init(
        path: String,
        build: String,
        configuration: SocketClient.Configuration,
        trust: PeerTrust
    ) async throws {
        guard !build.isEmpty else { throw SessionTransportError.handshake("empty build") }
        guard configuration.maximumFrameBytes > 0,
              (1 ... Int(UInt32.max)).contains(configuration.streamQueueDepth),
              (1 ... Int(UInt32.max)).contains(configuration.eventQueueDepth),
              configuration.maximumPendingWrites > 0
        else { throw SessionTransportError.invalidFrame("stream queue exceeds protocol window") }
        guard configuration.handshakeTimeout.isFinite, configuration.handshakeTimeout > 0,
              configuration.writeTimeout.isFinite, configuration.writeTimeout > 0,
              configuration.cancellationSettlementTimeout.isFinite,
              configuration.cancellationSettlementTimeout >= 0
        else { throw SessionTransportError.invalidFrame("timeout") }
        let bootstrap = try await Self.bootstrap(
            path: path,
            build: build,
            configuration: configuration,
            trust: trust
        )
        self.configuration = configuration
        readQueue = bootstrap.readQueue
        descriptor = bootstrap.descriptor
        codec = bootstrap.codec
        writer = bootstrap.writer
        eventChannel = SocketBoundedChannel(capacity: configuration.eventQueueDepth)
        peerBuild = bootstrap.peerBuild
        sessionGeneration = bootstrap.sessionGeneration
        let codec = bootstrap.codec
        let readQueue = bootstrap.readQueue
        Task { [weak self, codec, readQueue] in
            do {
                while true {
                    let frame = try await readQueue.performIO { try codec.read() }
                    guard let client = self else { return }
                    guard try await client.receive(frame) else { return }
                }
            } catch {
                self?.fail(error)
            }
        }
    }

    deinit {
        beginAbort(error: SessionTransportError.disconnected)
    }

    /// Opens a request. Set endInput false when request chunks will follow.
    func open(
        owner: SocketClient,
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        endInput: Bool = true,
        deadline: Date? = nil
    ) async throws -> SocketCall {
        guard !operation.isEmpty else { throw SessionTransportError.invalidFrame("empty operation") }
        let (id, state) = try lock.withLock { () throws -> (UInt64, ClientRequestState) in
            guard case .open = closeState else { throw SessionTransportError.disconnected }
            let id = nextID
            nextID += 1
            let state = ClientRequestState(
                streamQueueDepth: configuration.streamQueueDepth,
                sendEnded: endInput,
                settlementHook: requestSettlementHook,
                settlementWaitHook: requestSettlementWaitHook,
                sendAdmissionHook: requestSendAdmissionHook,
                sendHook: requestSendHook,
                sendDrainWaitHook: requestSendDrainWaitHook
            )
            pending[id] = state
            return (id, state)
        }
        let milliseconds = deadline.map(SessionTime.unixMilliseconds) ?? 0
        var requestCommitted = false
        do {
            try await write(SessionFrame(
                kind: .request,
                flags: endInput ? .end : [],
                id: id,
                deadlineUnixMilliseconds: milliseconds,
                operation: operation,
                tenant: tenant,
                payload: payload
            ))
            requestCommitted = true
            await openCommitHook?()
            try Task.checkCancellation()
            try await write(SessionFrame(
                kind: .window,
                id: id,
                sequence: UInt32(configuration.streamQueueDepth)
            ))
            try Task.checkCancellation()
        } catch is CancellationError {
            if requestCommitted {
                await SocketCall.cancel(client: self, id: id, state: state)
            } else {
                _ = await state.finish(throwing: CancellationError())
                _ = remove(id)
            }
            throw CancellationError()
        } catch {
            fail(error)
            throw error
        }
        return SocketCall(owner: owner, client: self, id: id, state: state)
    }

    /// Sends go-away, then closes the session and fails every pending call.
    func close() async {
        await withTaskCancellationHandler {
            if let requests = beginGracefulClose() {
                await Self.settle(requests, throwing: SessionTransportError.disconnected)
                try? await writer.close(with: SessionFrame(kind: .goAway, flags: .end))
                if completeGracefulClose() != nil {
                    finishClose(requests: [], error: SessionTransportError.disconnected)
                }
            }
            await closeLatch.wait()
        } onCancel: {
            self.beginAbort(error: CancellationError())
        }
    }

    func abortAndWait() async {
        beginAbort(error: SessionTransportError.disconnected)
        await closeLatch.wait()
    }

    func abort() {
        beginAbort(error: SessionTransportError.disconnected)
    }

    private func beginAbort(error: Error) {
        let requests = lock.withLock { () -> [ClientRequestState]? in
            switch closeState {
            case .open:
                let requests = Array(pending.values)
                pending.removeAll()
                closeState = .closed
                return requests
            case let .closing(requests):
                closeState = .closed
                return requests
            case .closed:
                return nil
            }
        }
        guard let requests else { return }
        finishClose(requests: requests, error: error)
    }

    fileprivate func write(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock {
            if case .closed = closeState {
                return true
            }
            return false
        }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try await writer.write(frame)
    }

    fileprivate func writeSettlement(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock {
            if case .closed = closeState {
                return true
            }
            return false
        }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        do {
            try await writer.writeSettlement(frame)
        } catch {
            fail(error)
            throw error
        }
    }

    fileprivate func writeCommitted(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock {
            if case .closed = closeState {
                return true
            }
            return false
        }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        do {
            try await writer.writeCommitted(frame)
        } catch {
            fail(error)
            throw error
        }
    }

    private func beginGracefulClose() -> [ClientRequestState]? {
        lock.withLock {
            guard case .open = closeState else { return nil }
            let requests = Array(pending.values)
            pending.removeAll()
            closeState = .closing(requests)
            return requests
        }
    }

    private func completeGracefulClose() -> [ClientRequestState]? {
        lock.withLock {
            guard case let .closing(requests) = closeState else { return nil }
            closeState = .closed
            return requests
        }
    }

    private func finishClose(requests: [ClientRequestState], error: Error) {
        shutdown(descriptor, SHUT_RDWR)
        writer.abort()
        writer.afterDrained { [closeLatch, descriptor, eventChannel, readQueue] in
            Task {
                await withCheckedContinuation { continuation in
                    readQueue.async {
                        Darwin.close(descriptor)
                        continuation.resume()
                    }
                }
                await Self.settle(requests, throwing: error)
                await eventChannel.finish(throwing: error)
                closeLatch.finish()
            }
        }
    }

    private static func settle(_ requests: [ClientRequestState], throwing error: Error) async {
        await withTaskGroup(of: Void.self) { group in
            for request in requests {
                group.addTask { _ = await request.finish(throwing: error) }
            }
        }
    }
}

private extension SocketClientCore {
    func receive(_ frame: SessionFrame) async throws -> Bool {
        switch frame.kind {
        case .response:
            try await receiveResponse(frame)
        case .stream:
            try await receiveStream(frame)
        case .event:
            try await receiveEvent(frame)
        case .window:
            try await receiveWindow(frame)
        case .goAway:
            beginAbort(error: SessionTransportError.disconnected)
            return false
        default:
            throw SessionTransportError.invalidFrame("server frame kind \(frame.kind)")
        }
        return true
    }

    private func receiveResponse(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.flags == .end, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("response")
        }
        let response = try Self.decodeResponse(frame.payload)
        guard let state = pendingState(frame.id) else { return }
        try await state.finish(returning: response.terminal) {
            if response.acknowledge {
                try await self.writeSettlement(SessionFrame(
                    kind: .acknowledgment,
                    flags: .end,
                    id: frame.id,
                    payload: self.sessionGeneration
                ))
            }
        }
        _ = remove(frame.id)
    }

    private func receiveStream(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("response stream")
        }
        guard let state = pendingState(frame.id) else { return }
        guard let chunk = try state.receive(frame) else { return }
        await receiveStreamOfferHook?()
        let accepted = await state.chunkChannel.offer(chunk)
        if !accepted {
            if state.isDiscardingOutput() {
                return
            }
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

    func fail(_ error: Error) {
        beginAbort(error: error)
    }

    private static func handshake(
        codec: SessionFrameCodec,
        build: String,
        timeout: TimeInterval
    ) throws -> SessionBuildIdentity {
        let payload = try JSONEncoder().encode(SessionBuildIdentity(
            protocolVersion: daemonKitSessionProtocolVersion,
            build: build
        ))
        try codec.write(SessionFrame(kind: .hello, flags: .end, payload: payload))
        let response = try codec.read(timeout: timeout)
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
        return identity
    }

    private struct DecodedResponse {
        let terminal: SocketTerminal
        let acknowledge: Bool
    }

    private static func decodeResponse(_ data: Data) throws -> DecodedResponse {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw SessionTransportError.invalidFrame("response JSON")
        }
        let payload: Data? = if let value = object["payload"] {
            try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
        } else {
            nil
        }
        let terminal = SocketTerminal(
            payload: payload,
            error: object["err"] as? String,
            rejected: object["rejected"] as? Bool ?? false,
            reason: object["reason"] as? String
        )
        return DecodedResponse(terminal: terminal, acknowledge: object["ack"] as? Bool ?? false)
    }
}

private extension SocketClientCore {
    private static func bootstrap(
        path: String,
        build: String,
        configuration: SocketClient.Configuration,
        trust: PeerTrust
    ) async throws -> Bootstrap {
        let owner = OwnedDescriptor()
        let readQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketClient.read")
        return try await withTaskCancellationHandler {
            var writer: SessionWriter?
            do {
                let descriptor = try await readQueue.performIO {
                    try connect(path: path, timeout: configuration.handshakeTimeout, owner: owner)
                }
                try await readQueue.performIO {
                    try trust.check(descriptor: descriptor)
                    try configureNonblocking(descriptor)
                }
                let codec = SessionFrameCodec(
                    descriptor: descriptor,
                    maximumFrameBytes: configuration.maximumFrameBytes,
                    writeTimeout: configuration.writeTimeout
                )
                let sessionWriter = SessionWriter(
                    codec: codec,
                    maximumPendingWrites: configuration.maximumPendingWrites,
                    label: "com.yasyf.daemonkit.SocketClient.write"
                )
                writer = sessionWriter
                let identity = try await readQueue.performIO {
                    try handshake(codec: codec, build: build, timeout: configuration.handshakeTimeout)
                }
                guard let session = identity.session, session.count == 16 else {
                    throw SessionTransportError.handshake("invalid session generation")
                }
                try await sessionWriter.write(SessionFrame(
                    kind: .window,
                    sequence: UInt32(configuration.eventQueueDepth)
                ))
                try Task.checkCancellation()
                try owner.releaseIfNotCanceled()
                return Bootstrap(
                    descriptor: descriptor,
                    codec: codec,
                    writer: sessionWriter,
                    readQueue: readQueue,
                    peerBuild: identity.build,
                    sessionGeneration: session
                )
            } catch {
                let canceled = Task.isCancelled || owner.isCanceled
                owner.cancel()
                writer?.abort()
                await writer?.drain()
                _ = try? await readQueue.performIO {}
                owner.close()
                if canceled || Task.isCancelled {
                    throw CancellationError()
                }
                throw error
            }
        } onCancel: {
            owner.cancel()
        }
    }

    private static func connect(path: String, timeout: TimeInterval, owner: OwnedDescriptor) throws -> Int32 {
        var address = try makeAddress(path: path)
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else {
            throw SessionTransportError.systemCall(operation: "socket", errno: errno)
        }
        try owner.install(descriptor)
        var enable: Int32 = 1
        setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enable, socklen_t(MemoryLayout<Int32>.size))
        try configureNonblocking(descriptor)
        let connected = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        if connected == 0 {
            return descriptor
        }
        guard errno == EINPROGRESS else {
            let code = errno
            owner.close()
            throw SessionTransportError.systemCall(operation: "connect", errno: code)
        }
        let deadline = SessionFrameCodec.deadline(after: timeout)
        while true {
            if owner.isCanceled {
                throw CancellationError()
            }
            var writable = pollfd(fd: descriptor, events: Int16(POLLOUT), revents: 0)
            let ready = poll(&writable, 1, SessionFrameCodec.pollTimeout(deadline: deadline, maximum: 100))
            if ready < 0, errno == EINTR {
                continue
            }
            if ready > 0 {
                var socketError: Int32 = 0
                var length = socklen_t(MemoryLayout<Int32>.size)
                guard getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &socketError, &length) == 0 else {
                    throw SessionTransportError.systemCall(operation: "getsockopt", errno: errno)
                }
                guard socketError == 0 else {
                    throw SessionTransportError.systemCall(operation: "connect", errno: socketError)
                }
                return descriptor
            }
            if let deadline, DispatchTime.now().uptimeNanoseconds >= deadline {
                throw SessionTransportError.systemCall(operation: "connect", errno: ETIMEDOUT)
            }
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

    private static func configureNonblocking(_ descriptor: Int32) throws {
        let flags = fcntl(descriptor, F_GETFL)
        guard flags >= 0 else {
            throw SessionTransportError.systemCall(operation: "fcntl", errno: errno)
        }
        guard fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0 else {
            throw SessionTransportError.systemCall(operation: "fcntl", errno: errno)
        }
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
    let settlementLatch = AsyncLatch()
    let settlementHook: (@Sendable () async -> Void)?
    let settlementWaitHook: (@Sendable () async -> Void)?
    var receiveSequence = SessionSequence()
    var receiveEnded = false
    var discardingOutput = false
    var cancelSent = false
    var terminalResult: SocketTerminal?
    var terminalError: Error?
    var settlementStarted = false
    var terminalReady = false
    var cancellationTimer: Task<Void, Never>?

    init(
        streamQueueDepth: Int,
        sendEnded: Bool,
        settlementHook: (@Sendable () async -> Void)?,
        settlementWaitHook: (@Sendable () async -> Void)?,
        sendAdmissionHook: (@Sendable () async -> Void)?,
        sendHook: (@Sendable () async -> Void)?,
        sendDrainWaitHook: (@Sendable () -> Void)?
    ) {
        sender = ClientRequestSender(
            ended: sendEnded,
            admissionHook: sendAdmissionHook,
            sendHook: sendHook,
            drainWaitHook: sendDrainWaitHook
        )
        chunkChannel = SocketBoundedChannel(capacity: streamQueueDepth)
        self.settlementHook = settlementHook
        self.settlementWaitHook = settlementWaitHook
        var resultContinuation: AsyncThrowingStream<SocketTerminal, Error>.Continuation!
        results = AsyncThrowingStream(bufferingPolicy: .bufferingOldest(1)) {
            resultContinuation = $0
        }
        self.resultContinuation = resultContinuation
    }

    func finish(
        returning result: SocketTerminal,
        beforePublishing: @Sendable () async throws -> Void
    ) async throws {
        let claimed = terminalLock.withLock {
            guard !settlementStarted else { return false }
            settlementStarted = true
            return true
        }
        guard claimed else {
            await settlementWaitHook?()
            await settlementLatch.wait()
            _ = try cachedResult()
            return
        }
        cancelCancellationTimer()
        receiveLock.withLock {
            receiveEnded = true
        }
        await chunkChannel.finish()
        await settlementHook?()
        await sender.close()
        do {
            try await beforePublishing()
        } catch {
            terminalLock.withLock {
                terminalError = error
                terminalReady = true
            }
            resultContinuation.finish(throwing: error)
            settlementLatch.finish()
            throw error
        }
        terminalLock.withLock {
            terminalResult = result
            terminalReady = true
        }
        resultContinuation.yield(result)
        resultContinuation.finish()
        settlementLatch.finish()
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

    @discardableResult
    func finish(throwing error: Error) async -> Bool {
        let claimed = terminalLock.withLock {
            guard !settlementStarted else { return false }
            settlementStarted = true
            return true
        }
        guard claimed else {
            await settlementWaitHook?()
            await settlementLatch.wait()
            return false
        }
        cancelCancellationTimer()
        receiveLock.withLock {
            discardingOutput = true
            receiveEnded = true
        }
        await chunkChannel.finish(throwing: error)
        await settlementHook?()
        await sender.close()
        terminalLock.withLock {
            terminalError = error
            terminalReady = true
        }
        resultContinuation.finish(throwing: error)
        settlementLatch.finish()
        return true
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

    func attachCancellationTimer(_ timer: Task<Void, Never>) {
        let cancel = terminalLock.withLock {
            guard !settlementStarted else { return true }
            cancellationTimer = timer
            return false
        }
        if cancel {
            timer.cancel()
        }
    }

    private func cancelCancellationTimer() {
        let timer = terminalLock.withLock {
            let timer = cancellationTimer
            cancellationTimer = nil
            return timer
        }
        timer?.cancel()
    }

    func cancelIO() async {
        receiveLock.withLock {
            receiveEnded = true
            discardingOutput = true
        }
        await chunkChannel.discard()
        await sender.close()
    }

    func isDiscardingOutput() -> Bool {
        receiveLock.withLock { discardingOutput }
    }

    func isTerminal() -> Bool {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        return settlementStarted
    }
}

private actor ClientRequestSender {
    private let window = SocketCreditWindow()
    private let admissionHook: (@Sendable () async -> Void)?
    private let sendHook: (@Sendable () async -> Void)?
    private let drainWaitHook: (@Sendable () -> Void)?
    private var sequence = SessionSequence()
    private var ended: Bool
    private var inFlight = 0
    private var drainWaiters: [CheckedContinuation<Void, Never>] = []
    private var turnActive = false
    private var turnWaiters: [CheckedContinuation<Bool, Never>] = []

    init(
        ended: Bool,
        admissionHook: (@Sendable () async -> Void)?,
        sendHook: (@Sendable () async -> Void)?,
        drainWaitHook: (@Sendable () -> Void)?
    ) {
        self.ended = ended
        self.admissionHook = admissionHook
        self.sendHook = sendHook
        self.drainWaitHook = drainWaitHook
    }

    func send(client: SocketClientCore, id: UInt64, payload: Data, end: Bool) async throws {
        guard !ended else {
            throw SessionTransportError.invalidFrame("request stream already ended")
        }
        inFlight += 1
        defer { finishSend() }
        await admissionHook?()
        guard await acquireTurn() else { throw CancellationError() }
        defer { releaseTurn() }
        guard await window.acquire() else { throw CancellationError() }
        guard !ended else { throw CancellationError() }
        let current = try sequence.take()
        await sendHook?()
        guard !ended else { throw CancellationError() }
        try await client.writeCommitted(SessionFrame(
            kind: .stream,
            flags: end ? .end : [],
            id: id,
            sequence: current,
            payload: payload
        ))
        if end {
            ended = true
        }
    }

    func grant(_ count: UInt32) async {
        await window.grant(count)
    }

    func close() async {
        ended = true
        await window.close()
        guard inFlight > 0 else { return }
        await withCheckedContinuation { continuation in
            drainWaiters.append(continuation)
            drainWaitHook?()
        }
    }

    private func finishSend() {
        inFlight -= 1
        guard inFlight == 0 else { return }
        let waiters = drainWaiters
        drainWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func acquireTurn() async -> Bool {
        if !turnActive {
            turnActive = true
            return true
        }
        return await withCheckedContinuation { turnWaiters.append($0) }
    }

    private func releaseTurn() {
        if ended {
            turnActive = false
            let waiters = turnWaiters
            turnWaiters.removeAll()
            for waiter in waiters {
                waiter.resume(returning: false)
            }
            return
        }
        if turnWaiters.isEmpty {
            turnActive = false
            return
        }
        turnWaiters.removeFirst().resume(returning: true)
    }
}
