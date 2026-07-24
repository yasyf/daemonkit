import Foundation

final class ClientRequestState: @unchecked Sendable {
    let results: AsyncThrowingStream<SocketTerminal, Error>
    let chunkChannel: SocketBoundedChannel<SocketRequestChunk>
    let resultContinuation: AsyncThrowingStream<SocketTerminal, Error>.Continuation
    let sender: ClientRequestSender
    let receiveLock = NSLock()
    let cancelLock = NSLock()
    let terminalLock = NSLock()
    let settlementLatch = AsyncLatch()
    let settlementHook: (@Sendable () async -> Void)?
    let settlementWaitHook: (@Sendable () async -> Void)?
    var receiveSequence = SessionSequence()
    var receiveEnded = false
    var discardingOutput = false
    var cancelSent = false
    var terminalResult: SocketTerminal?
    var terminalError: Error?
    var settlementStarted = false
    var terminalReady = false
    var cancellationTimer: Task<Void, Never>?

    init(
        streamQueueDepth: Int,
        sendEnded: Bool,
        settlementHook: (@Sendable () async -> Void)?,
        settlementWaitHook: (@Sendable () async -> Void)?,
        sendAdmissionHook: (@Sendable () async -> Void)?,
        sendHook: (@Sendable () async -> Void)?,
        sendDrainWaitHook: (@Sendable () -> Void)?
    ) {
        sender = ClientRequestSender(
            ended: sendEnded,
            admissionHook: sendAdmissionHook,
            sendHook: sendHook,
            drainWaitHook: sendDrainWaitHook
        )
        chunkChannel = SocketBoundedChannel(capacity: streamQueueDepth)
        self.settlementHook = settlementHook
        self.settlementWaitHook = settlementWaitHook
        var resultContinuation: AsyncThrowingStream<SocketTerminal, Error>.Continuation!
        results = AsyncThrowingStream(bufferingPolicy: .bufferingOldest(1)) {
            resultContinuation = $0
        }
        self.resultContinuation = resultContinuation
    }

    func finish(
        returning result: SocketTerminal,
        beforePublishing: @Sendable () async throws -> Void
    ) async throws {
        let claimed = terminalLock.withLock {
            guard !settlementStarted else { return false }
            settlementStarted = true
            return true
        }
        guard claimed else {
            await settlementWaitHook?()
            await settlementLatch.wait()
            _ = try cachedResult()
            return
        }
        cancelCancellationTimer()
        receiveLock.withLock {
            receiveEnded = true
        }
        await chunkChannel.finish()
        await settlementHook?()
        await sender.close()
        do {
            try await beforePublishing()
        } catch {
            terminalLock.withLock {
                terminalError = error
                terminalReady = true
            }
            resultContinuation.finish(throwing: error)
            settlementLatch.finish()
            throw error
        }
        terminalLock.withLock {
            terminalResult = result
            terminalReady = true
        }
        resultContinuation.yield(result)
        resultContinuation.finish()
        settlementLatch.finish()
    }

    func receive(_ frame: SessionFrame) throws -> SocketRequestChunk? {
        receiveLock.lock()
        defer { receiveLock.unlock() }
        if discardingOutput {
            return nil
        }
        guard !receiveEnded else {
            throw SessionTransportError.invalidFrame("response stream already ended")
        }
        let expected = try receiveSequence.take()
        guard frame.sequence == expected else {
            throw SessionTransportError.streamSequence(id: frame.id, got: frame.sequence, want: expected)
        }
        let ended = frame.flags.contains(.end)
        if ended {
            receiveEnded = true
        }
        return SocketRequestChunk(sequence: frame.sequence, payload: frame.payload, end: ended)
    }

    @discardableResult
    func finish(throwing error: Error) async -> Bool {
        let claimed = terminalLock.withLock {
            guard !settlementStarted else { return false }
            settlementStarted = true
            return true
        }
        guard claimed else {
            await settlementWaitHook?()
            await settlementLatch.wait()
            return false
        }
        cancelCancellationTimer()
        receiveLock.withLock {
            discardingOutput = true
            receiveEnded = true
        }
        await chunkChannel.finish(throwing: error)
        await settlementHook?()
        await sender.close()
        terminalLock.withLock {
            terminalError = error
            terminalReady = true
        }
        resultContinuation.finish(throwing: error)
        settlementLatch.finish()
        return true
    }

    func cachedResult() throws -> SocketTerminal? {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        guard terminalReady else { return nil }
        if let terminalError {
            throw terminalError
        }
        return terminalResult
    }

    func attachCancellationTimer(_ timer: Task<Void, Never>) {
        let cancel = terminalLock.withLock {
            guard !settlementStarted else { return true }
            cancellationTimer = timer
            return false
        }
        if cancel {
            timer.cancel()
        }
    }

    private func cancelCancellationTimer() {
        let timer = terminalLock.withLock {
            let timer = cancellationTimer
            cancellationTimer = nil
            return timer
        }
        timer?.cancel()
    }

    func cancelIO() async {
        receiveLock.withLock {
            receiveEnded = true
            discardingOutput = true
        }
        await chunkChannel.discard()
        await sender.close()
    }

    func isDiscardingOutput() -> Bool {
        receiveLock.withLock { discardingOutput }
    }

    func isTerminal() -> Bool {
        terminalLock.lock()
        defer { terminalLock.unlock() }
        return settlementStarted
    }
}

actor ClientRequestSender {
    private let window = SocketCreditWindow()
    private let admissionHook: (@Sendable () async -> Void)?
    private let sendHook: (@Sendable () async -> Void)?
    private let drainWaitHook: (@Sendable () -> Void)?
    private var sequence = SessionSequence()
    private var ended: Bool
    private var inFlight = 0
    private var drainWaiters: [CheckedContinuation<Void, Never>] = []
    private var turnActive = false
    private var turnWaiters: [CheckedContinuation<Bool, Never>] = []

    init(
        ended: Bool,
        admissionHook: (@Sendable () async -> Void)?,
        sendHook: (@Sendable () async -> Void)?,
        drainWaitHook: (@Sendable () -> Void)?
    ) {
        self.ended = ended
        self.admissionHook = admissionHook
        self.sendHook = sendHook
        self.drainWaitHook = drainWaitHook
    }

    func send(client: SocketClientCore, id: UInt64, payload: Data, end: Bool) async throws {
        guard !ended else {
            throw SessionTransportError.invalidFrame("request stream already ended")
        }
        inFlight += 1
        defer { finishSend() }
        await admissionHook?()
        guard await acquireTurn() else { throw CancellationError() }
        defer { releaseTurn() }
        guard await window.acquire() else { throw CancellationError() }
        guard !ended else { throw CancellationError() }
        let current = try sequence.take()
        await sendHook?()
        guard !ended else { throw CancellationError() }
        try await client.writeCommitted(SessionFrame(
            kind: .stream,
            flags: end ? .end : [],
            id: id,
            sequence: current,
            payload: payload
        ))
        if end {
            ended = true
        }
    }

    func grant(_ count: UInt32) async {
        await window.grant(count)
    }

    func close() async {
        ended = true
        await window.close()
        guard inFlight > 0 else { return }
        await withCheckedContinuation { continuation in
            drainWaiters.append(continuation)
            drainWaitHook?()
        }
    }

    private func finishSend() {
        inFlight -= 1
        guard inFlight == 0 else { return }
        let waiters = drainWaiters
        drainWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    private func acquireTurn() async -> Bool {
        if !turnActive {
            turnActive = true
            return true
        }
        return await withCheckedContinuation { turnWaiters.append($0) }
    }

    private func releaseTurn() {
        if ended {
            turnActive = false
            let waiters = turnWaiters
            turnWaiters.removeAll()
            for waiter in waiters {
                waiter.resume(returning: false)
            }
            return
        }
        if turnWaiters.isEmpty {
            turnActive = false
            return
        }
        turnWaiters.removeFirst().resume(returning: true)
    }
}
