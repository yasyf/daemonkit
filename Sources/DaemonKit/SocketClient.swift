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

    init(owner: SocketClient, client: SocketClientCore, id: UInt64, state: ClientRequestState) {
        self.owner = owner
        self.client = client
        self.id = id
        self.state = state
        chunks = SocketChunkStream(channel: state.chunkChannel) { [client, state] in
            Task { await Self.cancel(client: client, id: id, state: state) }
        } consumptionOperation: { _ in }
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

    static func cancel(client: SocketClientCore, id: UInt64, state: ClientRequestState) async {
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
        schema: String,
        lane: SessionLane,
        configuration: Configuration = .init(),
        onPhase: (@Sendable (PhaseSnapshot) -> Void)? = nil
    ) async throws {
        core = try await SocketClientCore(
            path: path,
            schema: schema,
            lane: lane,
            configuration: configuration,
            onPhase: onPhase
        )
    }

    /// Blocks until the server's phase stream reports ``SessionPhase/ready``.
    /// Throws ``RuntimeFailedError`` on failed and ``SessionDrainingError`` on
    /// draining, mirroring internal/wire.Client.WaitReady bounded by deadline.
    public func waitReady(deadline: Date) async throws {
        try await core.waitReady(deadline: deadline)
    }

    /// The most recent lifecycle snapshot the server published.
    public var phase: PhaseSnapshot {
        core.currentPhase
    }

    deinit {
        core.abort()
    }
}

/// One atomic capture of a ``PhaseGate``: snapshot, its revision, and closure.
struct PhaseGateState {
    let snapshot: PhaseSnapshot
    let revision: UInt64
    let closed: Bool
    let error: Error?
}

/// A race-free phase gate mirroring Go's phaseMu/phaseChanged: the snapshot and
/// its revision live under one lock, so a reader captures both atomically.
final class PhaseGate: @unchecked Sendable {
    private let lock = NSLock()
    private var snapshot: PhaseSnapshot
    private var revision: UInt64 = 0
    private var closed = false
    private var closeError: Error?
    private var waiters: [UUID: CheckedContinuation<Void, Never>] = [:]

    init(_ initial: PhaseSnapshot) {
        snapshot = initial
    }

    var current: PhaseSnapshot {
        lock.withLock { snapshot }
    }

    func read() -> PhaseGateState {
        lock.withLock {
            PhaseGateState(snapshot: snapshot, revision: revision, closed: closed, error: closeError)
        }
    }

    func update(_ next: PhaseSnapshot) {
        let woken = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            snapshot = next
            revision &+= 1
            let woken = Array(waiters.values)
            waiters.removeAll()
            return woken
        }
        for continuation in woken {
            continuation.resume()
        }
    }

    func close(_ error: Error) {
        let woken = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
            guard !closed else { return [] }
            closed = true
            closeError = error
            revision &+= 1
            let woken = Array(waiters.values)
            waiters.removeAll()
            return woken
        }
        for continuation in woken {
            continuation.resume()
        }
    }

    func waitChange(after expected: UInt64) async {
        let id = UUID()
        await withTaskCancellationHandler {
            await withCheckedContinuation { continuation in
                let resume = lock.withLock {
                    guard revision == expected, !closed else { return true }
                    waiters[id] = continuation
                    return false
                }
                if resume {
                    continuation.resume()
                }
            }
        } onCancel: {
            let waiter = lock.withLock { waiters.removeValue(forKey: id) }
            waiter?.resume()
        }
    }
}

final class SocketClientCore: @unchecked Sendable {
    enum CloseState {
        case open
        case closing([ClientRequestState])
        case closed
    }

    private struct Bootstrap {
        let descriptor: Int32
        let codec: SessionFrameCodec
        let writer: SessionWriter
        let readQueue: DispatchQueue
        let peerSchema: String
        let sessionGeneration: Data
        let phase: SessionPhase
    }

    /// Events pushed by the server, bounded by ``Configuration/eventQueueDepth``.
    var events: SocketEventStream {
        SocketEventStream(channel: eventChannel) { [weak self] in
            Task { await self?.close() }
        } consumptionOperation: { _ in }
    }

    /// Server schema identity established by the mandatory handshake.
    let peerSchema: String
    private let sessionGeneration: Data

    private let descriptor: Int32
    private let codec: SessionFrameCodec
    let writer: SessionWriter
    let configuration: SocketClient.Configuration
    private let readQueue: DispatchQueue
    let lock = NSLock()
    private let closeLatch = AsyncLatch()
    private let goAwayLatch = AsyncLatch()
    var pending: [UInt64: ClientRequestState] = [:]
    var nextID: UInt64 = 1
    var closeState = CloseState.open
    private let eventChannel: SocketBoundedChannel<SocketEvent>
    private let phaseGate: PhaseGate
    private let onPhase: (@Sendable (PhaseSnapshot) -> Void)?
    var openCommitHook: (@Sendable () async -> Void)?
    var requestWriteStartHook: (@Sendable () -> Void)?
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
        schema: String,
        lane: SessionLane,
        configuration: SocketClient.Configuration,
        onPhase: (@Sendable (PhaseSnapshot) -> Void)?
    ) async throws {
        switch lane {
        case .business:
            guard !schema.isEmpty else { throw SessionTransportError.handshake("empty schema") }
        case .control:
            guard schema.isEmpty else { throw SessionTransportError.handshake("control lane carries no schema") }
        }
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
            schema: schema,
            lane: lane,
            configuration: configuration
        )
        self.configuration = configuration
        self.onPhase = onPhase
        readQueue = bootstrap.readQueue
        descriptor = bootstrap.descriptor
        codec = bootstrap.codec
        writer = bootstrap.writer
        eventChannel = SocketBoundedChannel(capacity: configuration.eventQueueDepth)
        phaseGate = PhaseGate(PhaseSnapshot(sequence: 0, phase: bootstrap.phase))
        peerSchema = bootstrap.peerSchema
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

    /// Sends go-away, then closes the session and fails every pending call.
    func close() async {
        await withTaskCancellationHandler {
            if let requests = beginGracefulClose() {
                await Self.settle(requests, throwing: SessionTransportError.disconnected)
                do {
                    try await writer.close(with: SessionFrame(kind: .goAway, flags: .end))
                    let deadline = Date().addingTimeInterval(configuration.writeTimeout)
                    while !goAwayLatch.isFinished, deadline > Date() {
                        if Task.isCancelled {
                            break
                        }
                        try? await Task.sleep(for: .milliseconds(5))
                    }
                } catch {
                    beginAbort(error: error)
                }
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

    func waitUntilClosed() async {
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

    func write(_ frame: SessionFrame) async throws {
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

    func writeCommitted(_ frame: SessionFrame) async throws {
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
        phaseGate.close(error)
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

extension SocketClientCore {
    func receive(_ frame: SessionFrame) async throws -> Bool {
        switch frame.kind {
        case .response:
            try await receiveResponse(frame)
        case .stream:
            try await receiveStream(frame)
        case .event:
            try await receiveEvent(frame)
        case .lifecycle:
            try await receiveLifecycle(frame)
        case .window:
            try await receiveWindow(frame)
        case .goAway:
            let closing = lock.withLock {
                if case .closing = closeState {
                    return true
                }
                return false
            }
            guard closing else {
                beginAbort(error: SessionTransportError.disconnected)
                return false
            }
            goAwayLatch.finish()
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

    private func receiveLifecycle(_ frame: SessionFrame) async throws {
        let snapshot = try SessionHandshakeCodec.decodeLifecycle(frame.payload)
        phaseGate.update(snapshot)
        onPhase?(snapshot)
    }

    var currentPhase: PhaseSnapshot {
        phaseGate.current
    }

    func waitReady(deadline: Date) async throws {
        while true {
            let state = phaseGate.read()
            switch state.snapshot.phase {
            case .ready:
                return
            case .failed:
                throw RuntimeFailedError(snapshot: state.snapshot)
            case .draining:
                throw SessionDrainingError()
            case .starting:
                break
            }
            if state.closed {
                throw state.error ?? SessionTransportError.disconnected
            }
            try Task.checkCancellation()
            guard deadline > Date() else { throw SocketCallDeadlineExceededError() }
            let nanoseconds = deadlineSleepNanoseconds(until: deadline)
            try await withThrowingTaskGroup(of: Void.self) { group in
                group.addTask { await self.phaseGate.waitChange(after: state.revision) }
                group.addTask {
                    if nanoseconds > 0 {
                        try await Task.sleep(nanoseconds: nanoseconds)
                    }
                }
                _ = try await group.next()
                group.cancelAll()
            }
        }
    }

    private func receiveWindow(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.flags.isEmpty, frame.sequence > 0,
              frame.operation.isEmpty, frame.tenant.isEmpty, frame.payload.isEmpty
        else { throw SessionTransportError.invalidFrame("request stream window") }
        guard let state = pendingState(frame.id) else { return }
        await state.sender.grant(frame.sequence)
    }

    func remove(_ id: UInt64) -> ClientRequestState? {
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

    static func handshake(
        codec: SessionFrameCodec,
        schema: String,
        lane: SessionLane,
        timeout: TimeInterval
    ) throws -> SessionWireIdentity {
        let payload = try JSONEncoder().encode(SessionHelloIdentity(
            protocolVersion: daemonKitSessionProtocolVersion,
            lane: lane,
            schema: schema.isEmpty ? nil : schema
        ))
        let writeFailure: Error?
        do {
            try codec.write(SessionFrame(kind: .hello, flags: .end, payload: payload))
            writeFailure = nil
        } catch let error as SessionTransportError where error.isPeerEndWriteFailure {
            writeFailure = error
        }
        let drain: Bool
        do {
            drain = try codec.peekPreamble(timeout: timeout)
        } catch {
            throw writeFailure ?? error
        }
        if drain {
            throw SessionDrainingError()
        }
        let response: SessionFrame
        do {
            response = try codec.read(timeout: timeout)
        } catch {
            throw writeFailure ?? error
        }
        guard response.kind == .helloAck, response.flags == .end, response.id == 0,
              response.sequence == 0, response.operation.isEmpty, response.tenant.isEmpty
        else {
            throw writeFailure ?? SessionTransportError.handshake("invalid acknowledgment")
        }
        do {
            return try SessionHandshakeCodec.decodeAck(response.payload)
        } catch let rejection as SocketHandshakeRejectionError {
            throw rejection
        } catch {
            throw writeFailure ?? error
        }
    }

    private struct DecodedResponse {
        let terminal: SocketTerminal
        let acknowledge: Bool
    }

    private static func decodeResponse(_ data: Data) throws -> DecodedResponse {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any] else {
            throw SessionTransportError.invalidFrame("response JSON")
        }
        let allowed = Set(["payload", "err", "rejected", "code", "reason", "ack"])
        guard Set(object.keys).isSubset(of: allowed) else {
            throw SessionTransportError.invalidFrame("response fields")
        }
        let error = try optionalString("err", in: object)
        let rejected = try optionalBool("rejected", in: object) ?? false
        let code = try optionalString("code", in: object).map(SocketResponseCode.init(rawValue:))
        let reason = try optionalString("reason", in: object)
        let acknowledge = try optionalBool("ack", in: object) ?? false
        let payload: Data? = if let value = object["payload"] {
            try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
        } else {
            nil
        }
        let terminal = SocketTerminal(
            payload: payload,
            error: error,
            rejected: rejected,
            code: code,
            reason: reason
        )
        return DecodedResponse(terminal: terminal, acknowledge: acknowledge)
    }

    private static func optionalString(_ key: String, in object: [String: Any]) throws -> String? {
        guard let value = object[key] else { return nil }
        guard let value = value as? String else {
            throw SessionTransportError.invalidFrame("response \(key)")
        }
        return value
    }

    private static func optionalBool(_ key: String, in object: [String: Any]) throws -> Bool? {
        guard let value = object[key] else { return nil }
        guard CFGetTypeID(value as CFTypeRef) == CFBooleanGetTypeID(), let value = value as? Bool else {
            throw SessionTransportError.invalidFrame("response \(key)")
        }
        return value
    }
}

private extension SocketClientCore {
    private static func bootstrap(
        path: String,
        schema: String,
        lane: SessionLane,
        configuration: SocketClient.Configuration
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
                    try handshake(
                        codec: codec,
                        schema: schema,
                        lane: lane,
                        timeout: configuration.handshakeTimeout
                    )
                }
                try Task.checkCancellation()
                try owner.releaseIfNotCanceled()
                return Bootstrap(
                    descriptor: descriptor,
                    codec: codec,
                    writer: sessionWriter,
                    readQueue: readQueue,
                    peerSchema: identity.schema,
                    sessionGeneration: identity.session,
                    phase: identity.phase
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
