import Darwin
import Foundation

final class SessionWriter: @unchecked Sendable {
    private enum Priority {
        case ordinary
        case settlement
        case lifecycle
    }

    private final class Entry: @unchecked Sendable {
        let id = UUID()
        let frame: SessionFrame
        private let descriptorLock = NSLock()
        private var descriptorToPass: Int32?
        let descriptorDeadline: Date?
        let completion: @Sendable (Result<Void, Error>) -> Void
        let startObserver: (@Sendable () -> Void)?
        var canceled = false
        var started = false
        var superseded = false

        init(
            frame: SessionFrame,
            continuation: CheckedContinuation<Void, Error>,
            startObserver: (@Sendable () -> Void)? = nil
        ) {
            self.frame = frame
            descriptorToPass = nil
            descriptorDeadline = nil
            completion = { continuation.resume(with: $0) }
            self.startObserver = startObserver
        }

        init(
            frame: SessionFrame,
            completion: @escaping @Sendable (Result<Void, Error>) -> Void
        ) {
            self.frame = frame
            descriptorToPass = nil
            descriptorDeadline = nil
            self.completion = completion
            startObserver = nil
        }

        deinit {
            closeDescriptor()
        }

        func passedDescriptor() -> Int32? {
            descriptorLock.withLock { descriptorToPass }
        }

        func closeDescriptor() {
            let descriptor = descriptorLock.withLock { () -> Int32? in
                let descriptor = descriptorToPass
                descriptorToPass = nil
                return descriptor
            }
            if let descriptor {
                Darwin.close(descriptor)
            }
        }

        init(
            frame: SessionFrame,
            descriptorToPass: Int32,
            deadline: Date,
            continuation: CheckedContinuation<Void, Error>
        ) {
            self.frame = frame
            self.descriptorToPass = descriptorToPass
            descriptorDeadline = deadline
            completion = { continuation.resume(with: $0) }
            startObserver = nil
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
    private let selectionHook: (@Sendable (SessionFrame) -> Void)?
    private let lock = NSLock()
    private var accepting = true
    private var admitted = 0
    private var waiting: [Entry] = []
    private var settlementWaiting: [Entry] = []
    private var lifecycleWaiting: [Entry] = []
    private var settlementTurnUsed = false
    private var active: [UUID: Entry] = [:]
    private var terminalError: Error?

    init(
        codec: SessionFrameCodec,
        maximumPendingWrites: Int,
        label: String,
        admissionHook: (@Sendable (SessionFrame) -> Void)? = nil,
        startHook: (@Sendable (SessionFrame) -> Void)? = nil,
        selectionHook: (@Sendable (SessionFrame) -> Void)? = nil
    ) {
        self.codec = codec
        self.maximumPendingWrites = maximumPendingWrites
        self.admissionHook = admissionHook
        self.startHook = startHook
        self.selectionHook = selectionHook
        queue = DispatchQueue(label: label)
    }

    func write(_ frame: SessionFrame) async throws {
        try await write(frame, startObserver: nil)
    }

    func writeTracked(_ frame: SessionFrame, startObserver: @escaping @Sendable () -> Void) async throws {
        try await write(frame, startObserver: startObserver)
    }

    private func write(
        _ frame: SessionFrame,
        startObserver: (@Sendable () -> Void)?
    ) async throws {
        let cancellation = CancellationRegistration()
        try await withTaskCancellationHandler {
            try Task.checkCancellation()
            try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
                let entry = Entry(frame: frame, continuation: continuation, startObserver: startObserver)
                guard cancellation.install(entry.id) else {
                    continuation.resume(throwing: CancellationError())
                    return
                }
                submit(entry, cancellation: cancellation, priority: .ordinary)
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
            submit(entry, cancellation: nil, priority: .ordinary)
        }
    }

    func writePassingDescriptor(
        _ frame: SessionFrame,
        descriptor: Int32,
        deadline: Date
    ) async throws {
        if Task.isCancelled {
            Darwin.close(descriptor)
            throw CancellationError()
        }
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let entry = Entry(
                frame: frame,
                descriptorToPass: descriptor,
                deadline: deadline,
                continuation: continuation
            )
            submit(entry, cancellation: nil, priority: .ordinary)
        }
    }

    func writeSettlement(_ frame: SessionFrame) async throws {
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let entry = Entry(frame: frame, continuation: continuation)
            submit(entry, cancellation: nil, priority: .settlement)
        }
    }

    func writeLifecycle(_ frame: SessionFrame) async throws {
        try await enqueueLifecycle(frame).wait()
    }

    func enqueueLifecycle(_ frame: SessionFrame) -> LifecycleWriteReceipt {
        let receipt = LifecycleWriteReceipt()
        let entry = Entry(frame: frame) { receipt.finish($0) }
        submit(entry, cancellation: nil, priority: .lifecycle)
        return receipt
    }

    func close(with frame: SessionFrame) async throws {
        try Task.checkCancellation()
        try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
            let entry = Entry(frame: frame, continuation: continuation)
            let rejected = lock.withLock { () -> [Entry]? in
                guard accepting else { return nil }
                accepting = false
                let rejected = waiting + settlementWaiting + lifecycleWaiting
                waiting.removeAll()
                settlementWaiting.removeAll()
                lifecycleWaiting.removeAll()
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
                entry.closeDescriptor()
                entry.completion(.failure(SessionTransportError.disconnected))
            }
        }
        try Task.checkCancellation()
    }

    func abort() {
        let rejected = lock.withLock {
            accepting = false
            let rejected = waiting + settlementWaiting + lifecycleWaiting
            waiting.removeAll()
            settlementWaiting.removeAll()
            lifecycleWaiting.removeAll()
            return rejected
        }
        for entry in rejected {
            entry.closeDescriptor()
            entry.completion(.failure(SessionTransportError.disconnected))
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
}

private extension SessionWriter {
    private func submit(_ entry: Entry, cancellation: CancellationRegistration?, priority: Priority) {
        var displacedLifecycle: Entry?
        let error = lock.withLock { () -> Error? in
            guard cancellation?.isCanceled != true else { return CancellationError() }
            guard accepting else {
                return terminalError ?? SessionTransportError.disconnected
            }
            if admitted >= 1 {
                switch priority {
                case .lifecycle:
                    if let selected = active.values.first(where: {
                        $0.frame.kind == .lifecycle && !$0.started
                    }) {
                        selected.superseded = true
                    }
                    if let displaced = lifecycleWaiting.first {
                        lifecycleWaiting[0] = entry
                        displacedLifecycle = displaced
                    } else {
                        lifecycleWaiting.append(entry)
                    }
                case .settlement:
                    guard settlementWaiting.count < maximumPendingWrites else {
                        return SessionTransportError.invalidFrame("settlement write queue exceeded capacity")
                    }
                    settlementWaiting.append(entry)
                case .ordinary:
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
        displacedLifecycle?.completion(.success(()))
        if let error {
            entry.closeDescriptor()
            entry.completion(.failure(error))
        }
    }

    private func enqueue(_ entry: Entry) {
        queue.async { [self] in
            let preflight = lock.withLock { () -> Result<Bool, Error> in
                if entry.superseded {
                    return .success(false)
                }
                if entry.canceled {
                    return .failure(CancellationError())
                }
                entry.started = true
                if let terminalError {
                    return .failure(terminalError)
                }
                return .success(true)
            }
            let result: Result<Void, Error>
            switch preflight {
            case let .failure(error):
                result = .failure(error)
            case .success(false):
                result = .success(())
            case .success(true):
                entry.startObserver?()
                startHook?(entry.frame)
                result = Result {
                    if let descriptorToPass = entry.passedDescriptor() {
                        do {
                            try codec.write(
                                entry.frame,
                                passing: descriptorToPass,
                                deadline: entry.descriptorDeadline!,
                                onDescriptorSent: { entry.closeDescriptor() }
                            )
                        } catch {
                            if entry.passedDescriptor() == nil {
                                throw BrokerHandoffError.deliveryUnknown
                            }
                            throw error
                        }
                    } else {
                        try codec.write(entry.frame)
                    }
                }
            }
            finish(entry, result: result)
        }
    }

    private func finish(_ entry: Entry, result: Result<Void, Error>) {
        entry.closeDescriptor()
        let outcome = lock.withLock { () -> FinishResult in
            active.removeValue(forKey: entry.id)
            admitted -= 1
            let completion = result
            if case let .failure(error) = result, !(error is CancellationError) {
                accepting = false
                terminalError = error
                let rejected = waiting + settlementWaiting + lifecycleWaiting
                waiting.removeAll()
                settlementWaiting.removeAll()
                lifecycleWaiting.removeAll()
                return FinishResult(
                    completion: completion,
                    next: nil,
                    rejected: rejected,
                    rejectionError: error
                )
            }
            guard accepting, !lifecycleWaiting.isEmpty || !settlementWaiting.isEmpty || !waiting.isEmpty else {
                if lifecycleWaiting.isEmpty, settlementWaiting.isEmpty, waiting.isEmpty {
                    settlementTurnUsed = false
                }
                return FinishResult(completion: completion, next: nil, rejected: [], rejectionError: nil)
            }
            let next: Entry
            if !lifecycleWaiting.isEmpty {
                next = lifecycleWaiting.removeFirst()
            } else if !settlementWaiting.isEmpty, waiting.isEmpty || !settlementTurnUsed {
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
            selectionHook?(next.frame)
            enqueue(next)
        }
        for entry in outcome.rejected {
            entry.closeDescriptor()
            entry.completion(.failure(outcome.rejectionError ?? SessionTransportError.disconnected))
        }
        entry.completion(outcome.completion)
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
        waiter?.closeDescriptor()
        waiter?.completion(.failure(CancellationError()))
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
