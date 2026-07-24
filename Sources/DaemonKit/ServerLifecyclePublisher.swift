import Foundation

final class LifecycleWriteReceipt: @unchecked Sendable {
    private struct Waiter {
        let continuation: CheckedContinuation<Void, Error>
        let timeout: Task<Void, Never>?
    }

    private let lock = NSLock()
    private var result: Result<Void, Error>?
    private var waiters: [UUID: Waiter] = [:]

    func wait(deadline: Date? = nil) async throws {
        if let deadline, deadline <= Date() {
            throw RuntimeShutdownError.deadlineExceeded
        }
        try await withCheckedThrowingContinuation { continuation in
            let id = UUID()
            let settled = lock.withLock { () -> Result<Void, Error>? in
                if let currentResult = result {
                    return currentResult
                }
                let timeout = deadline.map { deadline in
                    Task { [weak self] in
                        let nanoseconds = deadlineSleepNanoseconds(until: deadline)
                        if nanoseconds > 0 {
                            try? await Task.sleep(nanoseconds: nanoseconds)
                        }
                        guard !Task.isCancelled else { return }
                        self?.timeout(id)
                    }
                }
                waiters[id] = Waiter(continuation: continuation, timeout: timeout)
                return nil
            }
            if let settled {
                continuation.resume(with: settled)
            }
        }
    }

    func finish(_ result: Result<Void, Error>) {
        let waiters = lock.withLock { () -> [Waiter] in
            guard self.result == nil else { return [] }
            self.result = result
            let waiters = Array(self.waiters.values)
            self.waiters.removeAll()
            return waiters
        }
        for waiter in waiters {
            waiter.timeout?.cancel()
            waiter.continuation.resume(with: result)
        }
    }

    private func timeout(_ id: UUID) {
        let waiter = lock.withLock { waiters.removeValue(forKey: id) }
        waiter?.continuation.resume(throwing: RuntimeShutdownError.deadlineExceeded)
    }
}

final class ServerLifecyclePublisher: @unchecked Sendable {
    private let submit: @Sendable (Data) -> LifecycleWriteReceipt
    private let closeSession: @Sendable () -> Void
    private let wireBuild: String
    private let lock = NSLock()
    private var current: RuntimeReadinessEvent?
    private var terminalReceipt: LifecycleWriteReceipt?
    private var closed = false

    init(
        wireBuild: String,
        submit: @escaping @Sendable (Data) -> LifecycleWriteReceipt,
        closeSession: @escaping @Sendable () -> Void
    ) {
        self.wireBuild = wireBuild
        self.submit = submit
        self.closeSession = closeSession
    }

    convenience init(
        wireBuild: String,
        write: @escaping @Sendable (Data) async throws -> Void,
        closeSession: @escaping @Sendable () -> Void
    ) {
        self.init(wireBuild: wireBuild, submit: { payload in
            let receipt = LifecycleWriteReceipt()
            Task {
                do {
                    try await write(payload)
                    receipt.finish(.success(()))
                } catch {
                    receipt.finish(.failure(error))
                }
            }
            return receipt
        }, closeSession: closeSession)
    }

    @discardableResult
    func publish(_ payload: Data) throws -> LifecycleWriteReceipt? {
        let event = try RuntimeReadinessCodec.decodeEvent(payload)
        let publication = try lock.withLock { () throws -> (LifecycleWriteReceipt, Bool)? in
            guard !closed else { throw SessionTransportError.disconnected }
            guard event.wireBuild == wireBuild else {
                throw SocketWireBuildMismatchError(server: event.wireBuild, client: wireBuild)
            }
            if let current {
                guard event.runtimeIdentity == current.runtimeIdentity else {
                    throw RuntimeReadinessValidationError.invalidResponse(
                        "runtime identity changed on one authenticated session"
                    )
                }
                switch event.progress.sequence {
                case ..<current.progress.sequence:
                    throw RuntimeReadinessValidationError.sequenceRegression(
                        got: event.progress.sequence,
                        previous: current.progress.sequence
                    )
                case current.progress.sequence:
                    guard event.progress == current.progress else {
                        throw RuntimeReadinessValidationError.sequenceMutation(event.progress.sequence)
                    }
                    if event.progress.state == .failed || event.progress.state == .draining {
                        guard let terminalReceipt else {
                            throw SessionTransportError.invalidFrame("terminal lifecycle receipt missing")
                        }
                        return (terminalReceipt, false)
                    }
                    return nil
                default:
                    try validateReadinessTransition(from: current.progress, to: event.progress)
                }
            }
            current = event
            let receipt = submit(payload)
            if event.progress.state == .failed || event.progress.state == .draining {
                terminalReceipt = receipt
            }
            return (receipt, true)
        }
        guard let (receipt, shouldMonitor) = publication else { return nil }

        let terminal = event.progress.state == .failed || event.progress.state == .draining
        if shouldMonitor {
            Task {
                do {
                    try await receipt.wait()
                } catch {
                    failSession()
                }
            }
        }
        return terminal ? receipt : nil
    }

    func finish() {
        lock.withLock { closed = true }
    }

    private func failSession() {
        let shouldClose = lock.withLock { () -> Bool in
            guard !closed else { return false }
            closed = true
            return true
        }
        if shouldClose {
            closeSession()
        }
    }
}
