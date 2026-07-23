import Foundation

final class SessionWriter: @unchecked Sendable {
    private final class Entry: @unchecked Sendable {
        let id = UUID()
        let frame: SessionFrame
        let continuation: CheckedContinuation<Void, Error>
        var canceled = false
        var started = false

        init(frame: SessionFrame, continuation: CheckedContinuation<Void, Error>) {
            self.frame = frame
            self.continuation = continuation
        }
    }

    private struct FinishResult {
        let completion: Result<Void, Error>
        let next: Entry?
        let rejected: [Entry]
        let rejectionError: Error?
    }

    private let codec: SessionFrameCodec
    private let maximumPendingWrites: Int
    private let queue: DispatchQueue
    private let admissionHook: (@Sendable (SessionFrame) -> Void)?
    private let startHook: (@Sendable (SessionFrame) -> Void)?
    private let lock = NSLock()
    private var accepting = true
    private var admitted = 0
    private var waiting: [Entry] = []
    private var settlementWaiting: [Entry] = []
    private var settlementTurnUsed = false
    private var active: [UUID: Entry] = [:]
    private var terminalError: Error?

    init(
        codec: SessionFrameCodec,
        maximumPendingWrites: Int,
        label: String,
        admissionHook: (@Sendable (SessionFrame) -> Void)? = nil,
        startHook: (@Sendable (SessionFrame) -> Void)? = nil
    ) {
        self.codec = codec
        self.maximumPendingWrites = maximumPendingWrites
        self.admissionHook = admissionHook
        self.startHook = startHook
        queue = DispatchQueue(label: label)
    }

    func write(_ frame: SessionFrame) async throws {
        let cancellation = CancellationRegistration()
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                let entry = Entry(frame: frame, continuation: continuation)
                guard cancellation.install(entry.id) else {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                submit(entry, cancellation: cancellation, priority: false)
            }
        } onCancel: {
            if let id = cancellation.cancel() {
                self.cancel(id)
            }
        }
    }

    func writeCommitted(_ frame: SessionFrame) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let entry = Entry(frame: frame, continuation: continuation)
            submit(entry, cancellation: nil, priority: false)
        }
    }

    func writeSettlement(_ frame: SessionFrame) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let entry = Entry(frame: frame, continuation: continuation)
            submit(entry, cancellation: nil, priority: true)
        }
    }

    func close(with frame: SessionFrame) async throws {
        try Task.checkCancellation()
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let entry = Entry(frame: frame, continuation: continuation)
            let rejected = lock.withLock { () -> [Entry]? in
                guard accepting else { return nil }
                accepting = false
                let rejected = waiting + settlementWaiting
                waiting.removeAll()
                settlementWaiting.removeAll()
                active[entry.id] = entry
                admitted += 1
                enqueue(entry)
                return rejected
            }
            guard let rejected else {
                continuation.resume(throwing: SessionTransportError.disconnected)
                return
            }
            for entry in rejected {
                entry.continuation.resume(throwing: SessionTransportError.disconnected)
            }
        }
        try Task.checkCancellation()
    }

    func abort() {
        let rejected = lock.withLock {
            accepting = false
            let rejected = waiting + settlementWaiting
            waiting.removeAll()
            settlementWaiting.removeAll()
            return rejected
        }
        for entry in rejected {
            entry.continuation.resume(throwing: SessionTransportError.disconnected)
        }
    }

    func afterDrained(_ operation: @escaping @Sendable () -> Void) {
        queue.async(execute: operation)
    }

    func drain() async {
        await withCheckedContinuation { continuation in
            afterDrained { continuation.resume() }
        }
    }

    private func submit(_ entry: Entry, cancellation: CancellationRegistration?, priority: Bool) {
        let error = lock.withLock { () -> Error? in
            guard cancellation?.isCanceled != true else { return CancellationError() }
            guard accepting else {
                return terminalError ?? SessionTransportError.disconnected
            }
            if admitted >= 1 {
                if priority {
                    guard settlementWaiting.count < maximumPendingWrites else {
                        return SessionTransportError.invalidFrame("settlement write queue exceeded capacity")
                    }
                    settlementWaiting.append(entry)
                } else {
                    guard waiting.count < maximumPendingWrites else {
                        return SessionTransportError.invalidFrame("write queue exceeded capacity")
                    }
                    waiting.append(entry)
                }
                admissionHook?(entry.frame)
                return nil
            }
            admitted += 1
            active[entry.id] = entry
            admissionHook?(entry.frame)
            enqueue(entry)
            return nil
        }
        if let error {
            entry.continuation.resume(throwing: error)
        }
    }

    private func enqueue(_ entry: Entry) {
        queue.async { [self] in
            let preflight = lock.withLock { () -> Error? in
                if entry.canceled {
                    return CancellationError()
                }
                entry.started = true
                return terminalError
            }
            let result: Result<Void, Error>
            if let preflight {
                result = .failure(preflight)
            } else {
                startHook?(entry.frame)
                result = Result { try codec.write(entry.frame) }
            }
            finish(entry, result: result)
        }
    }

    private func finish(_ entry: Entry, result: Result<Void, Error>) {
        let outcome = lock.withLock { () -> FinishResult in
            active.removeValue(forKey: entry.id)
            admitted -= 1
            let completion = result
            if case let .failure(error) = result, !(error is CancellationError) {
                accepting = false
                terminalError = error
                let rejected = waiting + settlementWaiting
                waiting.removeAll()
                settlementWaiting.removeAll()
                return FinishResult(
                    completion: completion,
                    next: nil,
                    rejected: rejected,
                    rejectionError: error
                )
            }
            guard accepting, !settlementWaiting.isEmpty || !waiting.isEmpty else {
                if settlementWaiting.isEmpty, waiting.isEmpty {
                    settlementTurnUsed = false
                }
                return FinishResult(completion: completion, next: nil, rejected: [], rejectionError: nil)
            }
            let next: Entry
            if !settlementWaiting.isEmpty, waiting.isEmpty || !settlementTurnUsed {
                next = settlementWaiting.removeFirst()
                settlementTurnUsed = true
            } else {
                next = waiting.removeFirst()
                settlementTurnUsed = false
            }
            admitted += 1
            active[next.id] = next
            return FinishResult(completion: completion, next: next, rejected: [], rejectionError: nil)
        }
        if let next = outcome.next {
            enqueue(next)
        }
        for entry in outcome.rejected {
            entry.continuation.resume(throwing: outcome.rejectionError ?? SessionTransportError.disconnected)
        }
        entry.continuation.resume(with: outcome.completion)
    }

    private func cancel(_ id: UUID) {
        let waiter = lock.withLock { () -> Entry? in
            if let index = waiting.firstIndex(where: { $0.id == id }) {
                return waiting.remove(at: index)
            }
            if active[id]?.started == false {
                active[id]?.canceled = true
            }
            return nil
        }
        waiter?.continuation.resume(throwing: CancellationError())
    }
}

private final class CancellationRegistration: @unchecked Sendable {
    private let lock = NSLock()
    private var id: UUID?
    private var canceled = false

    func install(_ id: UUID) -> Bool {
        lock.withLock {
            guard !canceled else { return false }
            self.id = id
            return true
        }
    }

    func cancel() -> UUID? {
        lock.withLock {
            canceled = true
            return id
        }
    }

    var isCanceled: Bool {
        lock.withLock { canceled }
    }
}
