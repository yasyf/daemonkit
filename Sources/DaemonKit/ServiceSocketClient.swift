import Foundation

/// One deadline-bounded logical unary request.
public struct ServiceSocketCall: Sendable {
    public let operation: String
    public let tenant: String
    public let payload: Data
    public let replay: ServiceSocketReplayPolicy
    public let runtimeTarget: ServiceRuntimeTarget
    public let deadline: Date

    public init(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        replay: ServiceSocketReplayPolicy = .provenNonDispatch,
        runtimeTarget: ServiceRuntimeTarget,
        deadline: Date
    ) {
        self.operation = operation
        self.tenant = tenant
        self.payload = payload
        self.replay = replay
        self.runtimeTarget = runtimeTarget
        self.deadline = deadline
    }

    var expectedIdentity: RuntimeIdentity? {
        guard case let .exact(identity) = runtimeTarget else { return nil }
        return identity
    }

    var allowsSuccessor: Bool {
        runtimeTarget == .anyAuthenticatedSuccessor
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

/// A persistent unary client that crosses expected service startup and takeover.
public actor ServiceSocketClient {
    struct Generation: Sendable {
        let id: UInt64
        let client: Task<SocketClient, Error>
    }

    enum Transition: Error {
        case reconnect
    }

    struct GenerationObservation: Sendable {
        let id: UInt64
        let event: RuntimeReadinessEvent
    }

    private let path: String
    private let wireBuild: String
    private let role: String
    private let readinessOperation: String
    private let configuration: SocketClient.Configuration
    private let noProgressTimeout: TimeInterval
    private let progressHandler: (@Sendable (ReadinessProgress) -> Void)?
    private let stateSignal = ServiceStateSignal()
    private var generation: Generation?
    private var generationObservation: GenerationObservation?
    private var generationReceipt: (id: UInt64, receipt: RuntimeProcessReceipt)?
    private var readinessDriverGeneration: UInt64?
    private var subscribedGeneration: UInt64?
    private var lifecycleObserverGeneration: UInt64?
    private var lifecycleObserver: Task<Void, Never>?
    private var lifecycleFailure: (generation: UInt64, error: any Error)?
    private var nextGeneration: UInt64 = 1
    private var closed = false
    private var terminal: (any Error)?
    private var retrySleepHook: (@Sendable () -> Void)?
    public nonisolated let termination = ServiceSocketTerminationSignal()

    var startedGenerations: UInt64 {
        nextGeneration - 1
    }

    func setRetrySleepHook(_ hook: @escaping @Sendable () -> Void) {
        retrySleepHook = hook
    }

    /// Creates a lazy exact-build service client.
    public init(
        path: String,
        wireBuild: String,
        role: String,
        noProgressTimeout: TimeInterval,
        configuration: SocketClient.Configuration = .init(),
        onProgress: (@Sendable (ReadinessProgress) -> Void)? = nil
    ) throws {
        try self.init(
            path: path,
            wireBuild: wireBuild,
            role: role,
            readinessOperation: runtimeReadinessSubscribeOperation,
            noProgressTimeout: noProgressTimeout,
            configuration: configuration,
            onProgress: onProgress
        )
    }

    init(
        path: String,
        wireBuild: String,
        role: String,
        readinessOperation: String,
        noProgressTimeout: TimeInterval,
        configuration: SocketClient.Configuration = .init(),
        onProgress: (@Sendable (ReadinessProgress) -> Void)? = nil
    ) throws {
        guard !wireBuild.isEmpty else { throw SessionTransportError.handshake("empty wireBuild") }
        guard !role.isEmpty else { throw SessionTransportError.handshake("empty role") }
        guard !readinessOperation.isEmpty else { throw SessionTransportError.invalidFrame("empty readiness operation") }
        guard noProgressTimeout.isFinite, noProgressTimeout > 0 else {
            throw RuntimeReadinessValidationError.invalidResponse("positive no-progress timeout is required")
        }
        self.path = path
        self.wireBuild = wireBuild
        self.role = role
        self.readinessOperation = readinessOperation
        self.noProgressTimeout = noProgressTimeout
        self.configuration = configuration
        progressHandler = onProgress
    }

    /// Executes one logical call through typed lifecycle transitions.
    public func call(_ request: ServiceSocketCall) async throws -> SocketTerminal {
        try checkClientState()
        guard request.deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        guard !request.operation.isEmpty else {
            throw SessionTransportError.invalidFrame("empty operation")
        }
        if let expected = request.expectedIdentity,
           expected.runtimeBuild.isEmpty
        {
            throw RuntimeReadinessValidationError.invalidResponse("exact runtime identity is required")
        }
        let progress = RuntimeProgressTracker(
            wireBuild: wireBuild,
            expected: request.expectedIdentity,
            noProgressTimeout: noProgressTimeout
        )
        while true {
            try checkBound(request.deadline, progress: progress)
            let current: (Generation, SocketClient)
            do {
                current = try await readySession(request: request, progress: progress)
            } catch Transition.reconnect {
                continue
            } catch {
                if Self.provesTransientConnect(error) {
                    try await waitForRetry(deadline: request.deadline, progress: progress)
                    continue
                }
                if let validation = error as? RuntimeReadinessValidationError,
                   case .draining = validation, !request.allowsSuccessor
                {
                    await fail(error)
                } else {
                    await retainIfTerminal(error)
                }
                throw error
            }

            let attempt = await current.1.attempt(
                operation: request.operation,
                tenant: request.tenant,
                payload: request.payload,
                deadline: request.deadline
            )
            if let terminal = try await handle(
                attempt,
                request: request,
                generation: current.0
            ) {
                return terminal
            }
        }
    }

    func acquireReadyRuntime(
        expectedRuntimeBuild: String,
        deadline: Date
    ) async throws -> RuntimeProcessReceipt {
        try checkClientState()
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        guard !expectedRuntimeBuild.isEmpty else {
            throw RuntimeReadinessValidationError.invalidResponse("expected runtime build is required")
        }
        let progress = RuntimeProgressTracker(
            wireBuild: wireBuild,
            expected: nil,
            noProgressTimeout: noProgressTimeout
        )
        while true {
            try checkBound(deadline, progress: progress)
            do {
                let current = try await session(deadline: effectiveDeadline(deadline, progress: progress))
                let receipt: RuntimeProcessReceipt
                if let cached = generationReceipt, cached.id == current.0.id {
                    guard cached.receipt.runtimeIdentity.runtimeBuild == expectedRuntimeBuild else {
                        throw RuntimeReadinessValidationError.invalidResponse(
                            "runtime receipt build mismatch"
                        )
                    }
                    receipt = cached.receipt
                } else {
                    receipt = try await current.1.acquireRuntimeReceipt(
                        expectedRuntimeBuild: expectedRuntimeBuild,
                        deadline: effectiveDeadline(deadline, progress: progress)
                    )
                    guard generation?.id == current.0.id else { throw Transition.reconnect }
                    generationReceipt = (current.0.id, receipt)
                }
                progress.pin(receipt.runtimeIdentity)
                let request = ServiceSocketCall(
                    operation: readinessOperation,
                    runtimeTarget: .exact(receipt.runtimeIdentity),
                    deadline: deadline
                )
                _ = try await readySession(request: request, progress: progress)
                return receipt
            } catch Transition.reconnect {
                continue
            } catch {
                if Self.provesTransientConnect(error) {
                    try await waitForRetry(deadline: deadline, progress: progress)
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
        stateSignal.signal()
        termination.finish(.closed)
        lifecycleObserver?.cancel()
        lifecycleObserver = nil
        guard let current = generation else { return }
        generation = nil
        current.client.cancel()
        if let client = try? await current.client.value {
            await client.close()
        }
    }
}

/// Acquires and verifies one exact runtime, then closes its private control session.
private extension ServiceSocketClient {
    func readySession(
        request: ServiceSocketCall,
        progress: RuntimeProgressTracker
    ) async throws -> (Generation, SocketClient) {
        let current = try await session(deadline: effectiveDeadline(request.deadline, progress: progress))
        if let expected = request.expectedIdentity {
            let receipt: RuntimeProcessReceipt
            if let cached = generationReceipt, cached.id == current.0.id {
                receipt = cached.receipt
            } else {
                receipt = try await current.1.acquireRuntimeReceipt(
                    expectedRuntimeBuild: expected.runtimeBuild,
                    deadline: effectiveDeadline(request.deadline, progress: progress)
                )
                guard generation?.id == current.0.id else { throw Transition.reconnect }
                generationReceipt = (current.0.id, receipt)
            }
            guard receipt.runtimeIdentity == expected else {
                throw RuntimeReadinessValidationError.runtimeIdentity(
                    got: receipt.runtimeIdentity,
                    want: expected
                )
            }
        }
        startLifecycleObserver(current.0, client: current.1)
        while true {
            let revision = stateSignal.currentRevision
            try checkBound(request.deadline, progress: progress)
            guard generation?.id == current.0.id else { throw Transition.reconnect }

            if let observation = generationObservation, observation.id == current.0.id {
                do {
                    if try progress.adopt(
                        observation.event.snapshot,
                        allowSuccessor: request.allowsSuccessor
                    ) {
                        return current
                    }
                } catch let error as RuntimeReadinessValidationError {
                    if case .draining = error {
                        await retire(current.0)
                        guard request.allowsSuccessor else {
                            throw error
                        }
                        throw Transition.reconnect
                    }
                    throw error
                } catch let error as RuntimeFailedError {
                    await retire(current.0)
                    throw error
                }
            }

            if let failure = lifecycleFailure, failure.generation == current.0.id {
                try await handleReadinessFailure(
                    failure.error,
                    generation: current.0,
                    deadline: request.deadline,
                    progress: progress
                )
                throw Transition.reconnect
            }

            if subscribedGeneration == current.0.id {
                try await waitForStateChange(
                    after: revision,
                    deadline: request.deadline,
                    progress: progress
                )
                continue
            }

            if readinessDriverGeneration == current.0.id {
                try await waitForStateChange(
                    after: revision,
                    deadline: request.deadline,
                    progress: progress
                )
                continue
            }

            readinessDriverGeneration = current.0.id
            do {
                try await driveReadiness(current, deadline: request.deadline, progress: progress)
                if readinessDriverGeneration == current.0.id {
                    readinessDriverGeneration = nil
                    stateSignal.signal()
                }
            } catch {
                if readinessDriverGeneration == current.0.id {
                    readinessDriverGeneration = nil
                    stateSignal.signal()
                }
                throw error
            }
        }
    }

    func driveReadiness(
        _ current: (Generation, SocketClient),
        deadline: Date,
        progress: RuntimeProgressTracker
    ) async throws {
        let payload = try RuntimeReadinessCodec.encodeSubscribe()
        let attempt = try await current.1.attempt(
            operation: readinessOperation,
            payload: payload,
            deadline: effectiveDeadline(deadline, progress: progress)
        )
        switch attempt.outcome {
        case .delivered:
            guard let terminal = attempt.terminal,
                  terminal.error == nil,
                  let payload = terminal.payload
            else {
                throw ServiceSocketClientError.malformedAttempt
            }
            try RuntimeReadinessCodec.decodeSubscribeAck(payload)
            guard generation?.id == current.0.id else { throw Transition.reconnect }
            subscribedGeneration = current.0.id
            stateSignal.signal()
        case .rejected:
            guard let terminal = attempt.terminal else { throw ServiceSocketClientError.malformedAttempt }
            if terminal.code == .readinessSubscriptionExists {
                await retire(current.0)
                throw Transition.reconnect
            }
            throw ServiceSocketRejectionError(
                code: terminal.code ?? SocketResponseCode(rawValue: "untyped"),
                reason: terminal.reason ?? "wire: readiness rejected"
            )
        case .preSendFailure, .postSendFailure, .deliveryUnknown:
            try await handleReadinessFailure(
                attempt.error,
                generation: current.0,
                deadline: deadline,
                progress: progress
            )
            throw Transition.reconnect
        }
    }

    func publish(_ event: RuntimeReadinessEvent, generation current: Generation) throws {
        guard event.wireBuild == wireBuild else {
            throw SocketWireBuildMismatchError(server: event.wireBuild, client: wireBuild)
        }
        let next = event.snapshot
        if let observation = generationObservation, observation.id == current.id {
            guard next.identity == observation.event.runtimeIdentity else {
                throw RuntimeReadinessValidationError.invalidResponse(
                    "runtime identity changed on one authenticated session"
                )
            }
            switch next.progress.sequence {
            case ..<observation.event.progress.sequence:
                throw RuntimeReadinessValidationError.sequenceRegression(
                    got: next.progress.sequence,
                    previous: observation.event.progress.sequence
                )
            case observation.event.progress.sequence:
                guard next.progress == observation.event.progress else {
                    throw RuntimeReadinessValidationError.sequenceMutation(next.progress.sequence)
                }
                return
            default:
                try validateReadinessTransition(
                    from: observation.event.progress,
                    to: next.progress
                )
            }
        }
        generationObservation = GenerationObservation(
            id: current.id,
            event: event
        )
        stateSignal.signal()
        progressHandler?(next.progress)
    }

    func startLifecycleObserver(_ current: Generation, client: SocketClient) {
        guard lifecycleObserverGeneration != current.id else { return }
        lifecycleObserver?.cancel()
        lifecycleObserverGeneration = current.id
        lifecycleFailure = nil
        lifecycleObserver = Task { [weak self, weak client] in
            guard let client else { return }
            do {
                while let payload = try await client.nextLifecycleSnapshot() {
                    let event = try RuntimeReadinessCodec.decodeEvent(payload)
                    await self?.receiveLifecycle(event, generation: current)
                }
                await self?.receiveLifecycleFailure(
                    SessionTransportError.disconnected,
                    generation: current
                )
            } catch {
                await self?.receiveLifecycleFailure(error, generation: current)
            }
        }
    }

    func receiveLifecycle(_ event: RuntimeReadinessEvent, generation current: Generation) async {
        guard generation?.id == current.id, lifecycleObserverGeneration == current.id else { return }
        do {
            try publish(event, generation: current)
        } catch {
            await fail(error)
        }
    }

    func receiveLifecycleFailure(_ error: any Error, generation current: Generation) async {
        guard generation?.id == current.id, lifecycleObserverGeneration == current.id else { return }
        lifecycleFailure = (current.id, error)
        stateSignal.signal()
    }

    func session(deadline: Date) async throws -> (Generation, SocketClient) {
        try checkClientState()
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        let current: Generation
        if let generation {
            current = generation
        } else {
            generationObservation = nil
            generationReceipt = nil
            readinessDriverGeneration = nil
            subscribedGeneration = nil
            lifecycleObserverGeneration = nil
            lifecycleFailure = nil
            var attemptConfiguration = configuration
            let remaining = deadline.timeIntervalSinceNow
            guard remaining > 0 else { throw ServiceSocketClientError.deadlineExceeded }
            attemptConfiguration.handshakeTimeout = min(attemptConfiguration.handshakeTimeout, remaining)
            let id = nextGeneration
            nextGeneration += 1
            let path = path
            let wireBuild = wireBuild
            let role = role
            let task = Task {
                try await SocketClient(
                    path: path,
                    wireBuild: wireBuild,
                    role: role,
                    configuration: attemptConfiguration
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
                generationObservation = nil
                readinessDriverGeneration = nil
                subscribedGeneration = nil
                lifecycleObserverGeneration = nil
                lifecycleFailure = nil
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
            case .runtimeStarting:
                await retire(generation)
                return nil
            case .runtimeDraining:
                guard request.allowsSuccessor else {
                    return terminal
                }
                await retire(generation)
                return nil
            case .buildMismatch:
                let error = ServiceSocketRejectionError(
                    code: .buildMismatch,
                    reason: terminal.reason ?? "wire: build mismatch"
                )
                await fail(error)
                throw error
            default:
                return terminal
            }
        case .preSendFailure:
            let error = try attemptError(attempt)
            if error is SocketCallDeadlineExceededError {
                throw ServiceSocketClientError.deadlineExceeded
            }
            if error is CancellationError || Self.isLocalCallFailure(error) {
                throw error
            }
            guard Self.provesSessionTransition(error) else {
                await fail(error)
                throw error
            }
            await retire(generation)
            return nil
        case .postSendFailure, .deliveryUnknown:
            let error = try attemptError(attempt)
            if error is SocketCallDeadlineExceededError {
                throw ServiceSocketClientError.deadlineExceeded
            }
            if error is CancellationError {
                throw error
            }
            guard Self.provesSessionTransition(error) else {
                await fail(error)
                throw error
            }
            await retire(generation)
            guard request.replay == .idempotent else { throw error }
            return nil
        }
    }

    func handleReadinessFailure(
        _ failure: (any Error)?,
        generation: Generation,
        deadline: Date,
        progress: RuntimeProgressTracker
    ) async throws {
        let error = failure ?? ServiceSocketClientError.malformedAttempt
        if error is CancellationError {
            throw error
        }
        if error is SocketCallDeadlineExceededError {
            try checkBound(deadline, progress: progress)
            throw ServiceSocketClientError.deadlineExceeded
        }
        guard Self.provesSessionTransition(error) else {
            throw error
        }
        await retire(generation)
    }

    func attemptError(_ attempt: SocketCallAttempt) throws -> any Error {
        guard let error = attempt.error else { throw ServiceSocketClientError.malformedAttempt }
        return error
    }

    func retire(_ current: Generation) async {
        guard generation?.id == current.id else { return }
        generation = nil
        stateSignal.signal()
        if generationObservation?.id == current.id {
            generationObservation = nil
        }
        if generationReceipt?.id == current.id {
            generationReceipt = nil
        }
        if readinessDriverGeneration == current.id {
            readinessDriverGeneration = nil
        }
        if subscribedGeneration == current.id {
            subscribedGeneration = nil
        }
        if lifecycleObserverGeneration == current.id {
            lifecycleObserver?.cancel()
            lifecycleObserver = nil
            lifecycleObserverGeneration = nil
            lifecycleFailure = nil
        }
        current.client.cancel()
        if let client = try? await current.client.value {
            client.abort()
        }
    }

    func fail(_ error: any Error) async {
        guard terminal == nil, !closed else { return }
        terminal = error
        stateSignal.signal()
        termination.finish(.failed(error))
        guard let current = generation else { return }
        generation = nil
        generationObservation = nil
        generationReceipt = nil
        readinessDriverGeneration = nil
        subscribedGeneration = nil
        lifecycleObserver?.cancel()
        lifecycleObserver = nil
        lifecycleObserverGeneration = nil
        lifecycleFailure = nil
        current.client.cancel()
        if let client = try? await current.client.value {
            client.abort()
        }
    }

    func retainIfTerminal(_ error: any Error) async {
        guard Self.isLifetimeTerminal(error) else { return }
        await fail(error)
    }

    func waitForStateChange(
        after revision: UInt64,
        deadline: Date,
        progress: RuntimeProgressTracker
    ) async throws {
        try checkBound(deadline, progress: progress)
        let remaining = try min(deadline.timeIntervalSinceNow, progress.remainingTimeInterval())
        guard remaining > 0 else {
            try checkBound(deadline, progress: progress)
            throw ServiceSocketClientError.deadlineExceeded
        }
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await self.stateSignal.wait(after: revision) }
            group.addTask { try await Task.sleep(for: .seconds(remaining)) }
            _ = try await group.next()
            group.cancelAll()
        }
        try checkBound(deadline, progress: progress)
    }

    func waitForRetry(deadline: Date, progress: RuntimeProgressTracker) async throws {
        try checkBound(deadline, progress: progress)
        retrySleepHook?()
        let remaining = try effectiveDeadline(deadline, progress: progress).timeIntervalSinceNow
        guard remaining > 0 else {
            try checkBound(deadline, progress: progress)
            throw ServiceSocketClientError.deadlineExceeded
        }
        let nanoseconds = min(UInt64(25_000_000), UInt64(remaining * 1_000_000_000))
        try await Task.sleep(nanoseconds: nanoseconds)
        try checkBound(deadline, progress: progress)
    }

    func checkBound(_ deadline: Date, progress: RuntimeProgressTracker) throws {
        try checkClientState()
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        try progress.checkDeadline()
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

    func effectiveDeadline(_ deadline: Date, progress: RuntimeProgressTracker) throws -> Date {
        try min(deadline, Date().addingTimeInterval(progress.remainingTimeInterval()))
    }

    static func isLifetimeTerminal(_ error: any Error) -> Bool {
        if error is CancellationError || error is ReadinessNoProgressError {
            return false
        }
        if let client = error as? ServiceSocketClientError {
            return client == .malformedAttempt
        }
        if let validation = error as? RuntimeReadinessValidationError {
            switch validation {
            case .draining:
                return false
            case .runtimeIdentity:
                return true
            case .invalidResponse, .sequenceRegression, .sequenceMutation:
                return true
            }
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
        guard let rejection = error as? SocketHandshakeRejectionError else { return false }
        return rejection.code == .sessionCapacity
    }

    static func provesSessionTransition(_ error: any Error) -> Bool {
        guard let transport = error as? SessionTransportError else { return false }
        switch transport {
        case let .systemCall(operation, code):
            let peerIO = operation == "read" || operation == "send" || operation == "poll"
            let peerEnd = code == ECONNRESET || code == ECONNABORTED || code == EPIPE
                || code == ENOTCONN || code == ETIMEDOUT || code == EAGAIN
            return peerIO && peerEnd
        case .cancellationDidNotSettle, .disconnected:
            return true
        default:
            return false
        }
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
