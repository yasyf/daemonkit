import Foundation

/// A bounded request stream that backpressures the session reader.
public struct SocketChunkStream: AsyncSequence, Sendable {
    public typealias Element = SocketRequestChunk

    private let channel: SocketBoundedChannel<SocketRequestChunk>
    private let cancellationOperation: @Sendable () -> Void
    private let consumptionOperation: @Sendable (SocketRequestChunk) throws -> Void

    init(
        channel: SocketBoundedChannel<SocketRequestChunk>,
        cancellationOperation: @escaping @Sendable () -> Void,
        consumptionOperation: @escaping @Sendable (SocketRequestChunk) throws -> Void = { _ in }
    ) {
        self.channel = channel
        self.cancellationOperation = cancellationOperation
        self.consumptionOperation = consumptionOperation
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(
            channel: channel,
            cancellationOperation: cancellationOperation,
            consumptionOperation: consumptionOperation
        )
    }

    func maximumBufferedChunkCount() async -> Int {
        await channel.maximumBufferedElementCount()
    }

    /// Pulls the next request chunk, backpressuring the session reader until it is consumed.
    public func nextChunk() async throws -> SocketRequestChunk? {
        let chunk = try await channel.next(onCancel: cancellationOperation)
        try chunk.map(consumptionOperation)
        return chunk
    }

    public struct Iterator: AsyncIteratorProtocol {
        private let channel: SocketBoundedChannel<SocketRequestChunk>
        private let cancellationOperation: @Sendable () -> Void
        private let consumptionOperation: @Sendable (SocketRequestChunk) throws -> Void

        fileprivate init(
            channel: SocketBoundedChannel<SocketRequestChunk>,
            cancellationOperation: @escaping @Sendable () -> Void,
            consumptionOperation: @escaping @Sendable (SocketRequestChunk) throws -> Void
        ) {
            self.channel = channel
            self.cancellationOperation = cancellationOperation
            self.consumptionOperation = consumptionOperation
        }

        public mutating func next() async throws -> SocketRequestChunk? {
            let chunk = try await channel.next(onCancel: cancellationOperation)
            try chunk.map(consumptionOperation)
            return chunk
        }
    }
}

/// A bounded pushed-event stream that backpressures the session reader.
public struct SocketEventStream: AsyncSequence, Sendable {
    public typealias Element = SocketEvent

    private let channel: SocketBoundedChannel<SocketEvent>
    private let cancellationOperation: @Sendable () -> Void
    private let consumptionOperation: @Sendable (SocketEvent) throws -> Void

    init(
        channel: SocketBoundedChannel<SocketEvent>,
        cancellationOperation: @escaping @Sendable () -> Void,
        consumptionOperation: @escaping @Sendable (SocketEvent) throws -> Void = { _ in }
    ) {
        self.channel = channel
        self.cancellationOperation = cancellationOperation
        self.consumptionOperation = consumptionOperation
    }

    public func makeAsyncIterator() -> Iterator {
        Iterator(
            channel: channel,
            cancellationOperation: cancellationOperation,
            consumptionOperation: consumptionOperation
        )
    }

    /// Pulls the next pushed event, backpressuring the session reader until it is consumed.
    public func nextEvent() async throws -> SocketEvent? {
        let event = try await channel.next(onCancel: cancellationOperation)
        try event.map(consumptionOperation)
        return event
    }

    func maximumBufferedEventCount() async -> Int {
        await channel.maximumBufferedElementCount()
    }

    public struct Iterator: AsyncIteratorProtocol {
        private let channel: SocketBoundedChannel<SocketEvent>
        private let cancellationOperation: @Sendable () -> Void
        private let consumptionOperation: @Sendable (SocketEvent) throws -> Void

        fileprivate init(
            channel: SocketBoundedChannel<SocketEvent>,
            cancellationOperation: @escaping @Sendable () -> Void,
            consumptionOperation: @escaping @Sendable (SocketEvent) throws -> Void
        ) {
            self.channel = channel
            self.cancellationOperation = cancellationOperation
            self.consumptionOperation = consumptionOperation
        }

        public mutating func next() async throws -> SocketEvent? {
            let event = try await channel.next(onCancel: cancellationOperation)
            try event.map(consumptionOperation)
            return event
        }
    }
}

actor SocketBoundedChannel<Element: Sendable> {
    private struct Receiver {
        let id: UUID
        let continuation: CheckedContinuation<Element?, Error>
    }

    private struct Sender {
        let id: UUID
        let element: Element
        let continuation: CheckedContinuation<Bool, Never>
    }

    private let capacity: Int
    private var buffer: [Element] = []
    private var bufferHighWatermark = 0
    private var receivers: [Receiver] = []
    private var senders: [Sender] = []
    private var terminal: Result<Void, Error>?

    init(capacity: Int) {
        self.capacity = max(1, capacity)
    }

    func send(_ element: Element) async -> Bool {
        let id = UUID()
        return await withTaskCancellationHandler {
            guard !Task.isCancelled else { return false }
            return await send(id: id, element: element)
        } onCancel: {
            Task { await self.cancelSender(id: id) }
        }
    }

    func offer(_ element: Element) -> Bool {
        guard terminal == nil else { return false }
        if !receivers.isEmpty {
            let receiver = receivers.removeFirst()
            receiver.continuation.resume(returning: element)
            return true
        }
        guard buffer.count < capacity else { return false }
        append(element)
        return true
    }

    private func send(id: UUID, element: Element) async -> Bool {
        guard terminal == nil else { return false }
        if !receivers.isEmpty {
            let receiver = receivers.removeFirst()
            receiver.continuation.resume(returning: element)
            return true
        }
        if buffer.count < capacity {
            append(element)
            return true
        }
        return await withCheckedContinuation { continuation in
            senders.append(Sender(id: id, element: element, continuation: continuation))
        }
    }

    func next(onCancel: @escaping @Sendable () -> Void) async throws -> Element? {
        let id = UUID()
        return try await withTaskCancellationHandler {
            try Task.checkCancellation()
            return try await withCheckedThrowingContinuation { continuation in
                receive(id: id, continuation: continuation)
            }
        } onCancel: {
            Task { await self.cancelReceiver(id: id, onCancel: onCancel) }
        }
    }

    func finish() {
        finish(with: .success(()), discarding: false)
    }

    func finish(throwing error: Error) {
        finish(with: .failure(error), discarding: true)
    }

    func discard() {
        finish(with: .success(()), discarding: true)
    }

    func maximumBufferedElementCount() -> Int {
        bufferHighWatermark
    }

    private func receive(
        id: UUID,
        continuation: CheckedContinuation<Element?, Error>
    ) {
        if !buffer.isEmpty {
            let chunk = buffer.removeFirst()
            admitSender()
            continuation.resume(returning: chunk)
            return
        }
        if let terminal {
            resume(continuation, with: terminal)
            return
        }
        receivers.append(Receiver(id: id, continuation: continuation))
    }

    private func cancelReceiver(id: UUID, onCancel: @Sendable () -> Void) {
        if let index = receivers.firstIndex(where: { $0.id == id }) {
            let receiver = receivers.remove(at: index)
            receiver.continuation.resume(throwing: CancellationError())
        }
        onCancel()
    }

    private func cancelSender(id: UUID) {
        guard let index = senders.firstIndex(where: { $0.id == id }) else { return }
        let sender = senders.remove(at: index)
        sender.continuation.resume(returning: false)
    }

    private func finish(with result: Result<Void, Error>, discarding: Bool) {
        guard terminal == nil else { return }
        terminal = result
        if discarding {
            buffer.removeAll()
        }
        let terminalReceivers = buffer.isEmpty ? receivers : []
        if buffer.isEmpty {
            receivers.removeAll()
        }
        let terminalSenders = senders
        senders.removeAll()
        for sender in terminalSenders {
            sender.continuation.resume(returning: false)
        }
        for receiver in terminalReceivers {
            resume(receiver.continuation, with: result)
        }
    }

    private func append(_ element: Element) {
        buffer.append(element)
        bufferHighWatermark = max(bufferHighWatermark, buffer.count)
    }

    private func admitSender() {
        guard terminal == nil, !senders.isEmpty else { return }
        let sender = senders.removeFirst()
        append(sender.element)
        sender.continuation.resume(returning: true)
    }

    private func resume(
        _ continuation: CheckedContinuation<Element?, Error>,
        with result: Result<Void, Error>
    ) {
        switch result {
        case .success:
            continuation.resume(returning: nil)
        case let .failure(error):
            continuation.resume(throwing: error)
        }
    }
}

actor SocketCreditWindow {
    private struct Waiter {
        let id: UUID
        let continuation: CheckedContinuation<Bool, Never>
    }

    private var credits = 0
    private var waiters: [Waiter] = []
    private var closed = false

    func acquire() async -> Bool {
        let id = UUID()
        return await withTaskCancellationHandler {
            guard !Task.isCancelled else { return false }
            if credits > 0 {
                credits -= 1
                return true
            }
            guard !closed else { return false }
            return await withCheckedContinuation { continuation in
                waiters.append(Waiter(id: id, continuation: continuation))
            }
        } onCancel: {
            Task { await self.cancel(id: id) }
        }
    }

    func grant(_ count: UInt32) {
        var remaining = Int(count)
        while remaining > 0, !waiters.isEmpty {
            let waiter = waiters.removeFirst()
            waiter.continuation.resume(returning: true)
            remaining -= 1
        }
        if !closed {
            credits += remaining
        }
    }

    func close() {
        guard !closed else { return }
        closed = true
        let pending = waiters
        waiters.removeAll()
        for waiter in pending {
            waiter.continuation.resume(returning: false)
        }
    }

    private func cancel(id: UUID) {
        guard let index = waiters.firstIndex(where: { $0.id == id }) else { return }
        let waiter = waiters.remove(at: index)
        waiter.continuation.resume(returning: false)
    }
}
