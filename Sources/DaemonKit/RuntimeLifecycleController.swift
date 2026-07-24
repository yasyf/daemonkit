import Darwin
import Foundation

/// Errors reported by generation-fenced runtime activation and publication operations.
enum RuntimeActivationError: Error, Equatable, Sendable {
    case activationAlreadyIssued
    case staleActivation
    case runtimeNotStarting
    case invalidPublication
    case publicationAlreadyCommitted
    case publicationStale
    case runtimeNotReady
}

/// RuntimeLifecycleSequenceExhaustedError seals a generation before sequence wrap.
struct RuntimeLifecycleSequenceExhaustedError: Error, Equatable, Sendable {}

/// RuntimeServerTerminatedError reports listener loss before lifecycle shutdown.
struct RuntimeServerTerminatedError: Error, Equatable, Sendable {}

/// RuntimeActivationContext is the preparation lifetime of one runtime generation.
final class RuntimeActivationContext: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    /// Whether preparation has been cancelled by failure or intake closure.
    var isCancelled: Bool {
        lock.withLock { cancelled }
    }

    /// Suspends until preparation is cancelled.
    func waitUntilCancelled() async {
        await withCheckedContinuation { continuation in
            let resume = lock.withLock {
                guard !cancelled else { return true }
                waiters.append(continuation)
                return false
            }
            if resume {
                continuation.resume()
            }
        }
    }

    func cancel() {
        let pending = lock.withLock {
            guard !cancelled else { return [CheckedContinuation<Void, Never>]() }
            cancelled = true
            let pending = waiters
            waiters.removeAll()
            return pending
        }
        for waiter in pending {
            waiter.resume()
        }
    }
}

/// RuntimeActivation is the sole generation-fenced preparation authority.
final class RuntimeActivation: @unchecked Sendable {
    fileprivate let id: UUID
    fileprivate let controller: RuntimeLifecycleController
    let context: RuntimeActivationContext

    fileprivate init(
        id: UUID,
        controller: RuntimeLifecycleController,
        context: RuntimeActivationContext
    ) {
        self.id = id
        self.controller = controller
        self.context = context
    }

    /// Updates opaque preparation progress without changing lifecycle on identical bytes.
    func updateProgress(_ detail: Data) throws {
        try controller.updateProgress(activation: self, detail: detail)
    }

    /// Atomically publishes one staged value, opens business admission, and announces Ready.
    func commitReady(_ publication: RuntimePublication<some Sendable>) throws {
        try controller.commitReady(activation: self, publication: publication.token)
    }

    /// Terminates preparation. The cause is retained for runtime waiters, not sent on the wire.
    func fail(_ cause: any Error) async throws {
        try await controller.fail(activation: self, cause: cause)
    }

    /// Returns the generation-fenced product health and activity reporter.
    func statusReporter() -> StatusReporter {
        StatusReporter(controller: controller, activationID: id)
    }
}

final class RuntimePublicationValue: @unchecked Sendable {
    let value: Any

    init(_ value: some Sendable) {
        self.value = value
    }
}

struct RuntimePublicationToken: Sendable {
    let id: UUID
    let slotID: UUID
    let activationID: UUID
    let controllerID: ObjectIdentifier
}

/// RuntimePublication is an opaque staged value that can be committed exactly once.
struct RuntimePublication<Value: Sendable>: Sendable {
    fileprivate let token: RuntimePublicationToken
}

/// RuntimePublicationSlot owns one typed runtime publication.
final class RuntimePublicationSlot<Value: Sendable>: @unchecked Sendable {
    private let controller: RuntimeLifecycleController
    private let id: UUID

    init(controller: RuntimeLifecycleController) {
        self.controller = controller
        id = controller.registerSlot()
    }

    /// Stages a generation-bound value without making it visible.
    func stage(_ activation: RuntimeActivation, value: Value) throws -> RuntimePublication<Value> {
        try RuntimePublication(token: controller.stage(
            slotID: id,
            activation: activation,
            value: RuntimePublicationValue(value)
        ))
    }

    /// Loads the committed value only while this runtime is Ready.
    func load() -> Value? {
        controller.load(slotID: id)?.value as? Value
    }

    /// Loads the value pinned when this request was admitted, including during Draining.
    func loadPinned(from request: SocketRequest) -> Value? {
        request.runtimeAdmission?.value(for: id)?.value as? Value
    }
}

final class RuntimeAdmissionPin: @unchecked Sendable {
    private let lock = NSLock()
    private var alive = true
    private let values: [UUID: RuntimePublicationValue]
    private let revocation: (@Sendable () -> Void)?

    init(
        values: [UUID: RuntimePublicationValue],
        revocation: (@Sendable () -> Void)? = nil
    ) {
        self.values = values
        self.revocation = revocation
    }

    func value(for slotID: UUID) -> RuntimePublicationValue? {
        lock.withLock {
            guard alive else { return nil }
            return values[slotID]
        }
    }

    func revoke() {
        let revoked = lock.withLock {
            guard alive else { return false }
            alive = false
            return true
        }
        if revoked {
            revocation?()
        }
    }
}

enum RuntimeBusinessAdmission: Sendable {
    case admitted(RuntimeAdmissionPin)
    case rejected(code: SocketResponseCode, reason: String)
}

/// RuntimeWaitResult is the retained terminal result of one runtime generation.
enum RuntimeWaitResult: @unchecked Sendable {
    case failed(any Error)
    case draining
}

/// RuntimeLifecycleController owns lifecycle, publication, and business admission atomically.
final class RuntimeLifecycleController: @unchecked Sendable {
    private final class Subscriber: @unchecked Sendable {
        weak var session: ServerSession?
        var active = false
        var pendingTerminal: LifecycleWriteReceipt?

        init(session: ServerSession) {
            self.session = session
        }
    }

    private struct StagedPublication: Sendable {
        let slotID: UUID
        let activationID: UUID
        let value: RuntimePublicationValue
    }

    let wireBuild: String
    let runtimeIdentity: RuntimeIdentity
    private let lock = NSLock()
    private var current: RuntimeReadinessEvent
    private var subscribers: [ObjectIdentifier: Subscriber] = [:]
    private var terminalReceipts: [LifecycleWriteReceipt] = []
    private var activation: (id: UUID, context: RuntimeActivationContext)?
    private var slots: Set<UUID> = []
    private var staged: [UUID: StagedPublication] = [:]
    private var published: [UUID: RuntimePublicationValue] = [:]
    private var committedPublication: UUID?
    private var waitResult: RuntimeWaitResult?
    private var waiters: [CheckedContinuation<RuntimeWaitResult, Never>] = []
    private var poisoned = false
    private var serverLive = false
    private var health: HealthStatus?
    private var admissions = 0
    private var workers = 0
    private var activities: Set<UUID> = []
    var registrationHook: (@Sendable () -> Void)?
    var activationHook: (@Sendable () -> Void)?
    var admissionRevocationHook: (@Sendable () -> Void)?
    var subscriberCount: Int {
        lock.withLock { subscribers.count }
    }

    init(wireBuild: String, runtimeIdentity: RuntimeIdentity) throws {
        guard !wireBuild.isEmpty,
              !runtimeIdentity.runtimeBuild.isEmpty
        else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime lifecycle initial values")
        }
        self.wireBuild = wireBuild
        self.runtimeIdentity = runtimeIdentity
        current = RuntimeReadinessEvent(
            protocolVersion: daemonKitSessionProtocolVersion,
            wireBuild: wireBuild,
            runtimeIdentity: runtimeIdentity,
            progress: ReadinessProgress(sequence: 1, state: .starting, detail: Data())
        )
    }

    func beginActivation() throws -> RuntimeActivation {
        let created = try lock.withLock { () throws -> (UUID, RuntimeActivationContext) in
            guard !poisoned, serverLive, current.progress.state == .starting else {
                throw RuntimeActivationError.runtimeNotStarting
            }
            guard activation == nil else { throw RuntimeActivationError.activationAlreadyIssued }
            let created = (UUID(), RuntimeActivationContext())
            activation = created
            return created
        }
        return RuntimeActivation(id: created.0, controller: self, context: created.1)
    }

    func registerSlot() -> UUID {
        lock.withLock {
            let id = UUID()
            slots.insert(id)
            return id
        }
    }

    func markServerLive() {
        lock.withLock {
            guard !poisoned, current.progress.state == .starting else { return }
            serverLive = true
        }
    }

    @discardableResult
    func markServerTerminal(_ serverError: (any Error)? = nil) -> RuntimeWaitResult? {
        var terminalization: Terminalization?
        let result = lock.withLock { () -> RuntimeWaitResult? in
            guard serverLive else { return waitResult }
            serverLive = false
            guard !poisoned else { return waitResult }
            switch current.progress.state {
            case .failed:
                return waitResult
            case .draining:
                guard let serverError, !Self.isExpectedServerClosure(serverError) else {
                    return waitResult ?? .draining
                }
                let result = RuntimeWaitResult.failed(serverError)
                waitResult = result
                return result
            case .starting, .ready:
                break
            }
            let error = serverError ?? RuntimeServerTerminatedError()
            if current.progress.sequence == .max {
                terminalization = try? exhaustLocked(RuntimeLifecycleSequenceExhaustedError())
                return terminalization?.result ?? waitResult
            }
            let context = activation?.context
            activation = nil
            staged.removeAll()
            activities.removeAll()
            waitResult = .failed(error)
            guard let prepared = try? prepareLocked(state: .failed, detail: current.progress.detail) else {
                terminalization = try? exhaustLocked(RuntimeLifecycleSequenceExhaustedError())
                return terminalization?.result ?? waitResult
            }
            terminalization = Terminalization(
                transition: applyLocked(prepared),
                context: context,
                result: .failed(error)
            )
            return .failed(error)
        }
        finishTerminalization(terminalization)
        return result
    }

    private static func isExpectedServerClosure(_ error: any Error) -> Bool {
        if error is CancellationError {
            return true
        }
        guard let transport = error as? SessionTransportError else { return false }
        switch transport {
        case .disconnected:
            return true
        case let .systemCall(_, code):
            return code == ECANCELED || code == EBADF || code == ENOTCONN
        default:
            return false
        }
    }

    func stage(
        slotID: UUID,
        activation candidate: RuntimeActivation,
        value: RuntimePublicationValue
    ) throws -> RuntimePublicationToken {
        var terminalization: Terminalization?
        do {
            return try lock.withLock {
                try validate(candidate)
                if current.progress.sequence == .max {
                    let error = RuntimeLifecycleSequenceExhaustedError()
                    terminalization = try exhaustLocked(error)
                    throw error
                }
                guard slots.contains(slotID) else { throw RuntimeActivationError.invalidPublication }
                let id = UUID()
                staged[id] = StagedPublication(slotID: slotID, activationID: candidate.id, value: value)
                return RuntimePublicationToken(
                    id: id,
                    slotID: slotID,
                    activationID: candidate.id,
                    controllerID: ObjectIdentifier(self)
                )
            }
        } catch {
            finishTerminalization(terminalization)
            throw error
        }
    }

    func load(slotID: UUID) -> RuntimePublicationValue? {
        lock.withLock {
            guard !poisoned, current.progress.state == .ready else { return nil }
            return published[slotID]
        }
    }

    func admitBusiness() -> RuntimeBusinessAdmission {
        lock.withLock {
            guard !poisoned else {
                return .rejected(code: .runtimeDraining, reason: "wire: runtime is draining")
            }
            switch current.progress.state {
            case .ready:
                admissions += 1
                let hook = admissionRevocationHook
                return .admitted(RuntimeAdmissionPin(
                    values: published,
                    revocation: { [weak self] in
                        self?.finishAdmission()
                        hook?()
                    }
                ))
            case .starting:
                return .rejected(code: .runtimeStarting, reason: "wire: runtime is starting")
            case .draining, .failed:
                return .rejected(code: .runtimeDraining, reason: "wire: runtime is draining")
            }
        }
    }

    func updateHealth(activationID: UUID, status: HealthStatus) throws {
        guard status.detail.count <= daemonKitMaxReadinessDetailBytes else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime health detail exceeds 4096 bytes")
        }
        let copied = HealthStatus(state: status.state, detail: status.detail)
        try lock.withLock {
            try validateReporter(activationID)
            guard health != copied else { return }
            health = copied
        }
    }

    func beginActivity(activationID: UUID) throws -> ActivityLease {
        let id = try lock.withLock { () throws -> UUID in
            try validateReporter(activationID)
            guard current.progress.state == .ready else { throw RuntimeActivationError.runtimeNotReady }
            let id = UUID()
            activities.insert(id)
            return id
        }
        return ActivityLease(id: id, controller: self)
    }

    func releaseActivity(id: UUID) {
        lock.withLock { _ = activities.remove(id) }
    }

    func statusSnapshot() -> RuntimeStatusSnapshot {
        lock.withLock {
            RuntimeStatusSnapshot(
                health: health.map { HealthStatus(state: $0.state, detail: $0.detail) },
                busy: admissions > 0 || workers > 0 || !activities.isEmpty,
                admissions: admissions,
                workers: workers,
                activities: activities.count
            )
        }
    }

    func admitControl() -> RuntimeBusinessAdmission {
        lock.withLock {
            guard !poisoned else {
                return .rejected(code: .runtimeDraining, reason: "wire: runtime is draining")
            }
            switch current.progress.state {
            case .starting, .ready:
                return .admitted(RuntimeAdmissionPin(values: [:]))
            case .draining, .failed:
                return .rejected(code: .runtimeDraining, reason: "wire: runtime is draining")
            }
        }
    }
}

extension RuntimeLifecycleController {
    func updateProgress(activation candidate: RuntimeActivation, detail: Data) throws {
        guard detail.count <= daemonKitMaxReadinessDetailBytes else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime lifecycle progress values")
        }
        let copied = Data(detail)
        var terminalization: Terminalization?
        do {
            let sessionsToClose = try lock.withLock { () throws -> [ServerSession] in
                try validate(candidate)
                guard copied != current.progress.detail else { return [] }
                do {
                    return try applyLocked(prepareLocked(state: .starting, detail: copied)).sessionsToClose
                } catch let error as RuntimeLifecycleSequenceExhaustedError {
                    terminalization = try exhaustLocked(error)
                    throw error
                }
            }
            sessionsToClose.forEach { $0.close() }
        } catch {
            finishTerminalization(terminalization)
            throw error
        }
    }

    func commitReady(activation candidate: RuntimeActivation, publication token: RuntimePublicationToken) throws {
        var terminalization: Terminalization?
        do {
            let sessionsToClose = try lock.withLock { () throws -> [ServerSession] in
                try validate(candidate)
                guard committedPublication == nil else { throw RuntimeActivationError.publicationAlreadyCommitted }
                guard token.controllerID == ObjectIdentifier(self),
                      token.activationID == candidate.id,
                      let publication = staged[token.id],
                      publication.slotID == token.slotID,
                      publication.activationID == candidate.id
                else { throw RuntimeActivationError.invalidPublication }
                let transition: PreparedTransition
                do {
                    transition = try prepareLocked(state: .ready, detail: current.progress.detail)
                } catch let error as RuntimeLifecycleSequenceExhaustedError {
                    terminalization = try exhaustLocked(error)
                    throw error
                }
                published[publication.slotID] = publication.value
                committedPublication = token.id
                staged.removeAll()
                return applyLocked(transition).sessionsToClose
            }
            sessionsToClose.forEach { $0.close() }
        } catch {
            finishTerminalization(terminalization)
            throw error
        }
    }

    func fail(activation candidate: RuntimeActivation, cause: any Error) async throws {
        var terminalization: Terminalization?
        let transition: TransitionResult
        do {
            transition = try lock.withLock { () throws -> TransitionResult in
                try validate(candidate)
                let prepared: PreparedTransition
                do {
                    prepared = try prepareLocked(state: .failed, detail: current.progress.detail)
                } catch let error as RuntimeLifecycleSequenceExhaustedError {
                    terminalization = try exhaustLocked(error)
                    throw error
                }
                staged.removeAll()
                activation = nil
                activities.removeAll()
                waitResult = .failed(cause)
                return applyLocked(prepared)
            }
        } catch {
            try await finishTerminalization(terminalization, awaitingReceipts: true)
            throw error
        }
        candidate.context.cancel()
        transition.sessionsToClose.forEach { $0.close() }
        finishWaiters(.failed(cause))
        try await Self.settle(transition.receipts)
    }

    func closeIntake(deadline: Date? = nil) async throws {
        var terminalization: Terminalization?
        let transition: (TransitionResult, RuntimeActivationContext?, RuntimeWaitResult?)
        do {
            transition = try lock.withLock {
                if poisoned {
                    return (TransitionResult(), nil, nil)
                }
                switch current.progress.state {
                case .failed, .draining:
                    return (TransitionResult(receipts: terminalReceipts), nil, nil)
                case .starting, .ready:
                    break
                }
                let prepared: PreparedTransition
                do {
                    prepared = try prepareLocked(state: .draining, detail: current.progress.detail)
                } catch let error as RuntimeLifecycleSequenceExhaustedError {
                    terminalization = try exhaustLocked(error)
                    throw error
                }
                let context = activation?.context
                activation = nil
                staged.removeAll()
                activities.removeAll()
                waitResult = .draining
                return (applyLocked(prepared), context, .draining)
            }
        } catch {
            try await finishTerminalization(terminalization, awaitingReceipts: true)
            throw error
        }
        transition.1?.cancel()
        transition.0.sessionsToClose.forEach { $0.close() }
        if let result = transition.2 {
            finishWaiters(result)
        }
        try await Self.settle(transition.0.receipts, deadline: deadline)
    }

    func wait() async -> RuntimeWaitResult {
        await withCheckedContinuation { continuation in
            let immediate = lock.withLock { () -> RuntimeWaitResult? in
                if let waitResult {
                    return waitResult
                }
                waiters.append(continuation)
                return nil
            }
            if let immediate {
                continuation.resume(returning: immediate)
            }
        }
    }

    func finishShutdown() {
        lock.withLock {
            staged.removeAll()
            published.removeAll()
            committedPublication = nil
            health = nil
            admissions = 0
            workers = 0
            activities.removeAll()
        }
    }

    func currentEvent() -> RuntimeReadinessEvent {
        lock.withLock { current }
    }

    func setStartingSequenceForTesting(_ sequence: UInt64) {
        lock.withLock {
            precondition(current.progress.state == .starting && !poisoned)
            current = RuntimeReadinessEvent(
                protocolVersion: daemonKitSessionProtocolVersion,
                wireBuild: wireBuild,
                runtimeIdentity: runtimeIdentity,
                progress: ReadinessProgress(
                    sequence: sequence,
                    state: .starting,
                    detail: current.progress.detail
                )
            )
        }
    }

    func register(_ session: ServerSession) {
        lock.withLock { subscribers[ObjectIdentifier(session)] = Subscriber(session: session) }
        registrationHook?()
    }

    func activate(_ session: ServerSession) {
        activationHook?()
        var shouldClose = false
        var relay: (LifecycleWriteReceipt, LifecycleWriteReceipt)?
        lock.withLock {
            let id = ObjectIdentifier(session)
            guard let subscriber = subscribers[id], subscriber.session != nil else { return }
            subscriber.active = true
            do {
                let payload = try JSONEncoder().encode(current)
                if let receipt = try session.enqueueLifecycle(payload), current.progress.isTerminal {
                    if let pending = subscriber.pendingTerminal {
                        subscriber.pendingTerminal = nil
                        relay = (receipt, pending)
                    } else {
                        terminalReceipts.append(receipt)
                    }
                }
            } catch {
                shouldClose = true
                subscriber.pendingTerminal?.finish(.failure(error))
                subscriber.pendingTerminal = nil
            }
        }
        if let (receipt, pending) = relay {
            Task {
                do { try await receipt.wait(); pending.finish(.success(())) }
                catch { pending.finish(.failure(error)) }
            }
        }
        if shouldClose {
            session.close()
        }
    }

    func unregister(_ session: ServerSession) {
        let pending = lock.withLock {
            subscribers.removeValue(forKey: ObjectIdentifier(session))
        }?.pendingTerminal
        pending?.finish(.failure(SessionTransportError.disconnected))
    }
}

private extension RuntimeLifecycleController {
    private struct TransitionResult {
        var receipts: [LifecycleWriteReceipt] = []
        var sessionsToClose: [ServerSession] = []
    }

    private struct Terminalization {
        let transition: TransitionResult
        let context: RuntimeActivationContext?
        let result: RuntimeWaitResult
    }

    private func validate(_ candidate: RuntimeActivation) throws {
        guard !poisoned, serverLive,
              candidate.controller === self,
              let activation,
              activation.id == candidate.id,
              current.progress.state == .starting
        else { throw RuntimeActivationError.staleActivation }
    }

    private func validateReporter(_ activationID: UUID) throws {
        guard !poisoned, let activation, activation.id == activationID,
              current.progress.state == .starting || current.progress.state == .ready
        else { throw RuntimeActivationError.publicationStale }
    }

    private func finishAdmission() {
        lock.withLock {
            precondition(admissions > 0)
            admissions -= 1
        }
    }

    private struct PreparedTransition {
        let event: RuntimeReadinessEvent
        let payload: Data
    }

    private func prepareLocked(
        state: RuntimeReadinessState,
        detail: Data
    ) throws -> PreparedTransition {
        guard current.progress.sequence < .max else {
            throw RuntimeLifecycleSequenceExhaustedError()
        }
        if current.progress.sequence == .max - 1, state != .failed, state != .draining {
            throw RuntimeLifecycleSequenceExhaustedError()
        }
        let sequence = current.progress.sequence + 1
        let progress = ReadinessProgress(
            sequence: sequence,
            state: state,
            detail: detail
        )
        try validateReadinessTransition(from: current.progress, to: progress)
        let next = RuntimeReadinessEvent(
            protocolVersion: daemonKitSessionProtocolVersion,
            wireBuild: wireBuild,
            runtimeIdentity: runtimeIdentity,
            progress: progress
        )
        let payload = try JSONEncoder().encode(next)
        return PreparedTransition(event: next, payload: payload)
    }

    private func exhaustLocked(_ error: RuntimeLifecycleSequenceExhaustedError) throws -> Terminalization {
        let context = activation?.context
        poisoned = true
        activation = nil
        staged.removeAll()
        activities.removeAll()
        waitResult = .failed(error)
        let transition: TransitionResult
        if current.progress.sequence < .max {
            let progress = ReadinessProgress(
                sequence: .max,
                state: .failed,
                detail: current.progress.detail
            )
            let event = RuntimeReadinessEvent(
                protocolVersion: daemonKitSessionProtocolVersion,
                wireBuild: wireBuild,
                runtimeIdentity: runtimeIdentity,
                progress: progress
            )
            transition = try applyLocked(PreparedTransition(
                event: event,
                payload: JSONEncoder().encode(event)
            ))
        } else {
            current = RuntimeReadinessEvent(
                protocolVersion: daemonKitSessionProtocolVersion,
                wireBuild: wireBuild,
                runtimeIdentity: runtimeIdentity,
                progress: ReadinessProgress(
                    sequence: .max,
                    state: .failed,
                    detail: current.progress.detail
                )
            )
            var forced = TransitionResult()
            forced.sessionsToClose = subscribers.values.compactMap(\.session)
            subscribers.removeAll()
            transition = forced
        }
        return Terminalization(transition: transition, context: context, result: .failed(error))
    }

    private func finishTerminalization(_ terminalization: Terminalization?) {
        guard let terminalization else { return }
        terminalization.context?.cancel()
        terminalization.transition.sessionsToClose.forEach { $0.close() }
        finishWaiters(terminalization.result)
        Task { try? await Self.settle(terminalization.transition.receipts) }
    }

    private func finishTerminalization(
        _ terminalization: Terminalization?,
        awaitingReceipts: Bool
    ) async throws {
        guard let terminalization else { return }
        terminalization.context?.cancel()
        terminalization.transition.sessionsToClose.forEach { $0.close() }
        finishWaiters(terminalization.result)
        if awaitingReceipts {
            try await Self.settle(terminalization.transition.receipts)
        }
    }

    private func applyLocked(_ prepared: PreparedTransition) -> TransitionResult {
        current = prepared.event
        let progress = prepared.event.progress
        var result = TransitionResult()
        var stale: [ObjectIdentifier] = []
        for (id, subscriber) in subscribers {
            guard let session = subscriber.session else { stale.append(id); continue }
            guard subscriber.active else {
                if progress.isTerminal {
                    let pending = LifecycleWriteReceipt()
                    subscriber.pendingTerminal = pending
                    result.receipts.append(pending)
                }
                continue
            }
            do {
                if let receipt = try session.enqueueLifecycle(prepared.payload) {
                    result.receipts.append(receipt)
                }
            } catch {
                result.sessionsToClose.append(session)
            }
        }
        for id in stale {
            subscribers.removeValue(forKey: id)
        }
        if progress.isTerminal {
            terminalReceipts = result.receipts
        }
        return result
    }

    private func finishWaiters(_ result: RuntimeWaitResult) {
        let pending = lock.withLock {
            let pending = waiters
            waiters.removeAll()
            return pending
        }
        for waiter in pending {
            waiter.resume(returning: result)
        }
    }

    private static func settle(
        _ receipts: [LifecycleWriteReceipt],
        deadline: Date? = nil
    ) async throws {
        var firstError: Error?
        for receipt in receipts {
            do { try await receipt.wait(deadline: deadline) }
            catch {
                if firstError == nil {
                    firstError = error
                }
            }
        }
        if let firstError {
            throw firstError
        }
    }
}

private extension ReadinessProgress {
    var isTerminal: Bool {
        state == .failed || state == .draining
    }
}
