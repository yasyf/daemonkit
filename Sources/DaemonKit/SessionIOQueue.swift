import Darwin
import Foundation

func deadlineSleepNanoseconds(until deadline: Date) -> UInt64 {
    let remaining = max(0, deadline.timeIntervalSinceNow)
    let maximum = TimeInterval(UInt64.max - 1) / 1_000_000_000
    return UInt64(min(remaining, maximum) * 1_000_000_000)
}

extension DispatchQueue {
    func performIO<Output: Sendable>(
        _ operation: @escaping @Sendable () throws -> Output
    ) async throws -> Output {
        try Task.checkCancellation()
        let output = try await withCheckedThrowingContinuation { continuation in
            async {
                continuation.resume(with: Result { try operation() })
            }
        }
        try Task.checkCancellation()
        return output
    }
}

extension NSLock {
    func withLock<Output>(_ operation: () throws -> Output) rethrows -> Output {
        lock()
        defer { unlock() }
        return try operation()
    }
}

final class AsyncLatch: @unchecked Sendable {
    private struct DeadlineWaiter {
        let continuation: CheckedContinuation<Void, Error>
        let timeout: Task<Void, Never>
    }

    private let lock = NSLock()
    private var finished = false
    private var waiters: [CheckedContinuation<Void, Never>] = []
    private var deadlineWaiters: [UUID: DeadlineWaiter] = [:]

    var isFinished: Bool {
        lock.withLock { finished }
    }

    func wait() async {
        await withCheckedContinuation { continuation in
            let resume = lock.withLock {
                if finished {
                    return true
                }
                waiters.append(continuation)
                return false
            }
            if resume {
                continuation.resume()
            }
        }
    }

    func wait(deadline: Date) async throws {
        guard deadline > Date() else { throw RuntimeShutdownError.deadlineExceeded }
        try await withCheckedThrowingContinuation { continuation in
            let id = UUID()
            let resume = lock.withLock { () -> Bool in
                guard !finished else { return true }
                let timeout = Task { [weak self] in
                    let nanoseconds = deadlineSleepNanoseconds(until: deadline)
                    if nanoseconds > 0 {
                        try? await Task.sleep(nanoseconds: nanoseconds)
                    }
                    guard !Task.isCancelled else { return }
                    self?.timeout(id)
                }
                deadlineWaiters[id] = DeadlineWaiter(
                    continuation: continuation,
                    timeout: timeout
                )
                return false
            }
            if resume {
                continuation.resume()
            }
        }
    }

    func finish() {
        let pending = lock.withLock { () -> (
            [CheckedContinuation<Void, Never>],
            [DeadlineWaiter]
        ) in
            guard !finished else { return ([], []) }
            finished = true
            let pending = waiters
            waiters.removeAll()
            let deadlines = Array(deadlineWaiters.values)
            deadlineWaiters.removeAll()
            return (pending, deadlines)
        }
        for waiter in pending.0 {
            waiter.resume()
        }
        for waiter in pending.1 {
            waiter.timeout.cancel()
            waiter.continuation.resume()
        }
    }

    private func timeout(_ id: UUID) {
        let waiter = lock.withLock { deadlineWaiters.removeValue(forKey: id) }
        waiter?.continuation.resume(throwing: RuntimeShutdownError.deadlineExceeded)
    }
}

final class OwnedDescriptor: @unchecked Sendable {
    private let lock = NSLock()
    private var descriptor: Int32 = -1
    private var canceled = false

    func install(_ descriptor: Int32) throws {
        let accepted = lock.withLock {
            guard !canceled, self.descriptor == -1 else { return false }
            self.descriptor = descriptor
            return true
        }
        guard accepted else {
            Darwin.close(descriptor)
            throw CancellationError()
        }
    }

    func cancel() {
        lock.withLock {
            canceled = true
            if descriptor >= 0 {
                shutdown(descriptor, SHUT_RDWR)
            }
        }
    }

    var isCanceled: Bool {
        lock.withLock { canceled }
    }

    func releaseIfNotCanceled() throws {
        try lock.withLock {
            guard !canceled else { throw CancellationError() }
            descriptor = -1
        }
    }

    func close() {
        let descriptor = lock.withLock {
            let descriptor = self.descriptor
            self.descriptor = -1
            return descriptor
        }
        if descriptor >= 0 {
            Darwin.close(descriptor)
        }
    }
}
