import Foundation

/// One deadline-bounded logical unary request against any ready session.
public struct ServiceSocketCall: Sendable {
    public let operation: String
    public let payload: Data
    public let replay: ServiceSocketReplayPolicy
    public let deadline: Date

    public init(
        operation: String,
        payload: Data = Data(),
        replay: ServiceSocketReplayPolicy = .provenNonDispatch,
        deadline: Date
    ) {
        self.operation = operation
        self.payload = payload
        self.replay = replay
        self.deadline = deadline
    }
}

/// The one terminal state of a logical service lifetime.
public enum ServiceSocketTermination: @unchecked Sendable {
    case closed
    case failed(any Error)
}

/// A replayable one-shot signal for logical service termination.
public final class ServiceSocketTerminationSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var result: ServiceSocketTermination?
    private var waiters: [CheckedContinuation<ServiceSocketTermination, Never>] = []

    /// Waits for explicit close or a retained terminal service failure.
    public func wait() async -> ServiceSocketTermination {
        await withCheckedContinuation { continuation in
            let immediate = lock.withLock { () -> ServiceSocketTermination? in
                if let result {
                    return result
                }
                waiters.append(continuation)
                return nil
            }
            if let immediate {
                continuation.resume(returning: immediate)
            }
        }
    }

    func finish(_ result: ServiceSocketTermination) {
        let waiters = lock.withLock { () -> [CheckedContinuation<ServiceSocketTermination, Never>] in
            guard self.result == nil else { return [] }
            self.result = result
            let waiters = self.waiters
            self.waiters.removeAll()
            return waiters
        }
        for waiter in waiters {
            waiter.resume(returning: result)
        }
    }
}

enum ServiceSocketCloseStep: Equatable, Sendable {
    case socketSettled
}

final class ServiceStateSignal: @unchecked Sendable {
    private let lock = NSLock()
    private var revision: UInt64 = 0
    private var waiters: [UUID: CheckedContinuation<Void, Error>] = [:]
    private var canceled: Set<UUID> = []

    var currentRevision: UInt64 {
        lock.withLock { revision }
    }

    func signal() {
        let waiters = lock.withLock { () -> [CheckedContinuation<Void, Error>] in
            revision &+= 1
            let current = Array(self.waiters.values)
            self.waiters.removeAll()
            return current
        }
        for waiter in waiters {
            waiter.resume()
        }
    }

    func wait(after expected: UInt64) async throws {
        let id = UUID()
        defer { lock.withLock { _ = canceled.remove(id) } }
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            try await withCheckedThrowingContinuation { continuation in
                let immediate = lock.withLock { () -> Result<Void, Error>? in
                    if canceled.remove(id) != nil {
                        return .failure(CancellationError())
                    }
                    guard revision == expected else { return .success(()) }
                    waiters[id] = continuation
                    return nil
                }
                if let immediate {
                    continuation.resume(with: immediate)
                }
            }
        } onCancel: {
            let waiter = self.lock.withLock { () -> CheckedContinuation<Void, Error>? in
                if let waiter = self.waiters.removeValue(forKey: id) {
                    return waiter
                }
                self.canceled.insert(id)
                return nil
            }
            waiter?.resume(throwing: CancellationError())
        }
    }
}

/// A machine-readable rejected service response.
public struct ServiceSocketRejectionError: Error, Sendable {
    public let code: SocketResponseCode
    public let reason: String
}

/// A persistent unary client that crosses expected service startup and takeover
/// by targeting any ready session on the schema- and trust-gated socket.
public actor ServiceSocketClient {
    struct Generation: Sendable {
        let id: UInt64
        let client: Task<SocketClient, Error>
    }

    enum Transition: Error {
        case reconnect
    }

    private let path: String
    private let schema: String
    private let lane: SessionLane
    private let configuration: SocketClient.Configuration
    private let progressHandler: (@Sendable (PhaseSnapshot) -> Void)?
    private var generation: Generation?
    private var nextGeneration: UInt64 = 1
    private var closed = false
    private var terminal: (any Error)?
    private var retrySleepHook: (@Sendable () -> Void)?
    private var closeStepHook: (@Sendable (ServiceSocketCloseStep) -> Void)?
    public nonisolated let termination = ServiceSocketTerminationSignal()

    var startedGenerations: UInt64 {
        nextGeneration - 1
    }

    func setRetrySleepHook(_ hook: @escaping @Sendable () -> Void) {
        retrySleepHook = hook
    }

    func setCloseStepHook(_ hook: @escaping @Sendable (ServiceSocketCloseStep) -> Void) {
        closeStepHook = hook
    }

    /// Creates a lazy schema-pinned service client on the given lane.
    public init(
        path: String,
        schema: String,
        lane: SessionLane = .business,
        configuration: SocketClient.Configuration = .init(),
        onProgress: (@Sendable (PhaseSnapshot) -> Void)? = nil
    ) throws {
        switch lane {
        case .business:
            guard !schema.isEmpty else { throw SessionTransportError.handshake("empty schema") }
        case .control:
            guard schema.isEmpty else { throw SessionTransportError.handshake("control lane carries no schema") }
        }
        self.path = path
        self.schema = schema
        self.lane = lane
        self.configuration = configuration
        progressHandler = onProgress
    }

    /// Executes one logical call, waiting for a ready session across reconnects.
    public func call(_ request: ServiceSocketCall) async throws -> SocketTerminal {
        try checkClientState()
        guard request.deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        guard !request.operation.isEmpty else {
            throw SessionTransportError.invalidFrame("empty operation")
        }
        while true {
            try checkBound(request.deadline)
            let current: (Generation, SocketClient)
            do {
                current = try await readySession(deadline: request.deadline)
            } catch Transition.reconnect {
                continue
            } catch {
                if Self.provesTransientConnect(error) {
                    try await waitForRetry(deadline: request.deadline)
                    continue
                }
                if Self.isDeadlineExpiry(error) {
                    try checkBound(request.deadline)
                    if let generation {
                        await retire(generation)
                    }
                    continue
                }
                await retainIfTerminal(error)
                throw error
            }

            let attempt = await current.1.attempt(
                operation: request.operation,
                payload: request.payload,
                deadline: request.deadline
            )
            if let terminal = try await handle(attempt, request: request, generation: current.0) {
                return terminal
            }
        }
    }

    /// Waits until any ready session is established on the socket, reconnecting
    /// across transient connect failures and draining successors until deadline.
    public func waitReady(deadline: Date) async throws {
        try checkClientState()
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        while true {
            try checkBound(deadline)
            do {
                _ = try await readySession(deadline: deadline)
                return
            } catch Transition.reconnect {
                continue
            } catch {
                if Self.provesTransientConnect(error) {
                    try await waitForRetry(deadline: deadline)
                    continue
                }
                if Self.isDeadlineExpiry(error) {
                    try checkBound(deadline)
                    if let generation {
                        await retire(generation)
                    }
                    continue
                }
                await retainIfTerminal(error)
                throw error
            }
        }
    }

    /// Closes the service lifetime and its current session generation.
    public func close() async {
        guard !closed, terminal == nil else { return }
        closed = true
        termination.finish(.closed)
        if let current = generation {
            current.client.cancel()
            if let client = try? await current.client.value {
                await client.close()
                closeStepHook?(.socketSettled)
            }
            if generation?.id == current.id {
                generation = nil
            }
        }
    }
}

private extension ServiceSocketClient {
    func readySession(deadline: Date) async throws -> (Generation, SocketClient) {
        let current = try await session(deadline: deadline)
        do {
            try await current.1.waitReady(deadline: deadline)
        } catch let error as RuntimeFailedError {
            throw error
        } catch {
            if error is SessionDrainingError || Self.provesSessionTransition(error) {
                await retire(current.0)
                throw Transition.reconnect
            }
            throw error
        }
        guard generation?.id == current.0.id else { throw Transition.reconnect }
        return current
    }

    func session(deadline: Date) async throws -> (Generation, SocketClient) {
        try checkClientState()
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        let current: Generation
        if let generation {
            current = generation
        } else {
            let remaining = deadline.timeIntervalSinceNow
            guard remaining > 0 else { throw ServiceSocketClientError.deadlineExceeded }
            var attemptConfiguration = configuration
            attemptConfiguration.handshakeTimeout = min(attemptConfiguration.handshakeTimeout, remaining)
            let id = nextGeneration
            nextGeneration += 1
            let path = path
            let schema = schema
            let lane = lane
            let onPhase = progressHandler
            let task = Task {
                try await SocketClient(
                    path: path,
                    schema: schema,
                    lane: lane,
                    configuration: attemptConfiguration,
                    onPhase: onPhase
                )
            }
            current = Generation(id: id, client: task)
            generation = current
        }

        let client: SocketClient
        do {
            client = try await current.client.value
        } catch {
            if generation?.id == current.id {
                generation = nil
            }
            throw error
        }
        guard !closed else {
            await client.close()
            throw ServiceSocketClientError.closed
        }
        if let terminal {
            await client.close()
            throw terminal
        }
        return (current, client)
    }

    func handle(
        _ attempt: SocketCallAttempt,
        request: ServiceSocketCall,
        generation: Generation
    ) async throws -> SocketTerminal? {
        switch attempt.outcome {
        case .delivered:
            guard let terminal = attempt.terminal else { throw ServiceSocketClientError.malformedAttempt }
            return terminal
        case .rejected:
            guard let terminal = attempt.terminal else { throw ServiceSocketClientError.malformedAttempt }
            switch terminal.code {
            case .runtimeStarting, .runtimeDraining:
                await retire(generation)
                return nil
            case .buildMismatch:
                let error = ServiceSocketRejectionError(
                    code: .buildMismatch,
                    reason: terminal.reason ?? "wire: build mismatch"
                )
                await fail(error)
                throw error
            case .peerUntrusted:
                let error = ServiceSocketRejectionError(
                    code: .peerUntrusted,
                    reason: terminal.reason ?? "wire: untrusted peer"
                )
                await fail(error)
                throw error
            default:
                return terminal
            }
        case .preSendFailure:
            let error = try attemptError(attempt)
            return try await handleCallFailure(error, request: request, generation: generation, sent: false)
        case .postSendFailure, .deliveryUnknown:
            let error = try attemptError(attempt)
            return try await handleCallFailure(error, request: request, generation: generation, sent: true)
        }
    }

    func handleCallFailure(
        _ error: any Error,
        request: ServiceSocketCall,
        generation: Generation,
        sent: Bool
    ) async throws -> SocketTerminal? {
        if Self.isDeadlineExpiry(error) {
            guard request.deadline > Date() else {
                throw ServiceSocketClientError.deadlineExceeded
            }
            await retire(generation)
            guard !sent || request.replay == .idempotent else { throw error }
            return nil
        }
        if error is CancellationError || (!sent && Self.isLocalCallFailure(error)) {
            throw error
        }
        guard Self.provesSessionTransition(error) else {
            await fail(error)
            throw error
        }
        await retire(generation)
        guard !sent || request.replay == .idempotent else { throw error }
        return nil
    }

    func attemptError(_ attempt: SocketCallAttempt) throws -> any Error {
        guard let error = attempt.error else { throw ServiceSocketClientError.malformedAttempt }
        return error
    }

    func retire(_ current: Generation) async {
        guard generation?.id == current.id else { return }
        generation = nil
        current.client.cancel()
        if let client = try? await current.client.value {
            client.abort()
        }
    }

    func fail(_ error: any Error) async {
        guard terminal == nil, !closed else { return }
        terminal = error
        termination.finish(.failed(error))
        guard let current = generation else { return }
        generation = nil
        current.client.cancel()
        if let client = try? await current.client.value {
            client.abort()
        }
    }

    func retainIfTerminal(_ error: any Error) async {
        guard Self.isLifetimeTerminal(error) else { return }
        await fail(error)
    }

    func waitForRetry(deadline: Date) async throws {
        try checkBound(deadline)
        retrySleepHook?()
        let remaining = deadline.timeIntervalSinceNow
        guard remaining > 0 else {
            throw ServiceSocketClientError.deadlineExceeded
        }
        let nanoseconds = min(UInt64(25_000_000), SessionFrameCodec.durationNanoseconds(remaining))
        try await Task.sleep(nanoseconds: nanoseconds)
        try checkBound(deadline)
    }

    func checkBound(_ deadline: Date) throws {
        try checkClientState()
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
    }

    func checkClientState() throws {
        try Task.checkCancellation()
        if let terminal {
            throw terminal
        }
        if closed {
            throw ServiceSocketClientError.closed
        }
    }
}

extension ServiceSocketClient {
    static func isLifetimeTerminal(_ error: any Error) -> Bool {
        if error is CancellationError {
            return false
        }
        if isDeadlineExpiry(error) {
            return false
        }
        if error is RuntimeFailedError {
            return true
        }
        if error is SessionDrainingError {
            return false
        }
        if let client = error as? ServiceSocketClientError {
            return client == .malformedAttempt
        }
        if let rejection = error as? ServiceSocketRejectionError {
            return rejection.code == .buildMismatch || rejection.code == .peerUntrusted
        }
        if let rejection = error as? SocketHandshakeRejectionError {
            return rejection.code != .sessionCapacity
        }
        return !provesTransientConnect(error)
    }

    static func provesNoListener(_ error: any Error) -> Bool {
        guard case let SessionTransportError.systemCall(operation, code) = error else { return false }
        return operation == "connect" && (code == ENOENT || code == ECONNREFUSED)
    }

    static func provesTransientConnect(_ error: any Error) -> Bool {
        if provesNoListener(error) {
            return true
        }
        if error is SessionDrainingError {
            return true
        }
        guard let rejection = error as? SocketHandshakeRejectionError else { return false }
        return rejection.code == .sessionCapacity
    }

    static func provesSessionTransition(_ error: any Error) -> Bool {
        guard let transport = error as? SessionTransportError else { return false }
        switch transport {
        case let .systemCall(operation, code):
            let peerIO = operation == "read" || operation == "send" || operation == "poll"
            let peerEnd = code == ECONNRESET || code == ECONNABORTED || code == EPIPE
                || code == ENOTCONN
            return peerIO && peerEnd
        case .cancellationDidNotSettle, .disconnected:
            return true
        default:
            return false
        }
    }

    static func isDeadlineExpiry(_ error: any Error) -> Bool {
        if error is SocketCallDeadlineExceededError {
            return true
        }
        guard case let SessionTransportError.systemCall(_, code) = error else { return false }
        return code == ETIMEDOUT
    }

    static func isLocalCallFailure(_ error: any Error) -> Bool {
        guard let transport = error as? SessionTransportError else { return false }
        switch transport {
        case .invalidFrame, .frameTooLarge:
            return true
        default:
            return false
        }
    }
}
