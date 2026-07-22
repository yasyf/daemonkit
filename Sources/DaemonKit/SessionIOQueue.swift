import Darwin
import Foundation

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
    private let lock = NSLock()
    private var finished = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

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

    func finish() {
        let pending = lock.withLock {
            guard !finished else { return [CheckedContinuation<Void, Never>]() }
            finished = true
            let pending = waiters
            waiters.removeAll()
            return pending
        }
        for waiter in pending {
            waiter.resume()
        }
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
        let descriptor = lock.withLock {
            canceled = true
            return self.descriptor
        }
        if descriptor >= 0 {
            shutdown(descriptor, SHUT_RDWR)
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
