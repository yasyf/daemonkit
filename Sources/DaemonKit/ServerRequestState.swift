import Foundation

actor ServerRequestState {
    let channel: SocketBoundedChannel<SocketRequestChunk>
    let responseWindow = SocketCreditWindow()
    private var task: Task<Void, Never>?
    private var deadlineTimer: Task<Void, Never>?
    private var requestSequence = SessionSequence()
    private var ended = false
    private var canceled = false
    private var finished = false
    private var transportError: Error?
    private var terminalSent = false
    private var terminalAcknowledged = false
    private var terminalAckWaiter: CheckedContinuation<Void, Error>?
    private var terminalAckTimer: Task<Void, Never>?
    private let receiveOfferHook: (@Sendable () async -> Void)?

    init(capacity: Int, receiveOfferHook: (@Sendable () async -> Void)? = nil) {
        channel = SocketBoundedChannel(capacity: capacity)
        self.receiveOfferHook = receiveOfferHook
    }

    func attach(_ task: Task<Void, Never>) {
        guard !canceled, !finished else {
            task.cancel()
            return
        }
        self.task = task
    }

    func attachDeadline(_ timer: Task<Void, Never>) {
        guard !canceled, !finished else {
            timer.cancel()
            return
        }
        deadlineTimer = timer
    }

    func finishInitialInput() async {
        guard !ended else { return }
        ended = true
        await channel.finish()
    }

    func receive(_ frame: SessionFrame) async {
        guard !canceled else { return }
        guard !terminalSent, !finished else { return }
        guard !ended else {
            await fail(SessionTransportError.invalidFrame("request stream already ended"))
            return
        }
        let expected: UInt32
        do {
            expected = try requestSequence.take()
        } catch {
            await fail(error)
            return
        }
        guard frame.sequence == expected else {
            ended = true
            await fail(SessionTransportError.streamSequence(
                id: frame.id,
                got: frame.sequence,
                want: expected
            ))
            return
        }
        let end = frame.flags.contains(.end)
        if end {
            ended = true
        }
        await receiveOfferHook?()
        let accepted = await channel.offer(SocketRequestChunk(
            sequence: frame.sequence,
            payload: frame.payload,
            end: end
        ))
        guard accepted else {
            if canceled || terminalSent || finished {
                return
            }
            await fail(SessionTransportError.invalidFrame("request stream exceeded granted window"))
            return
        }
        if end {
            await channel.finish()
        }
    }

    func cancel() async {
        guard !canceled else { return }
        canceled = true
        finished = true
        ended = true
        deadlineTimer?.cancel()
        deadlineTimer = nil
        task?.cancel()
        await channel.discard()
        await responseWindow.close()
    }

    func finish() {
        finished = true
        task = nil
        deadlineTimer?.cancel()
        deadlineTimer = nil
    }

    func error() -> Error? {
        transportError
    }

    func grantResponseCredits(_ count: UInt32) async {
        await responseWindow.grant(count)
    }

    func acknowledgeTerminal() -> Bool {
        guard terminalSent, !terminalAcknowledged else { return false }
        terminalAcknowledged = true
        let waiter = terminalAckWaiter
        terminalAckWaiter = nil
        terminalAckTimer?.cancel()
        terminalAckTimer = nil
        waiter?.resume()
        return true
    }

    func writeTerminal(_ write: @Sendable () async throws -> Void) async throws {
        terminalSent = true
        try await write()
    }

    func waitForTerminalAcknowledgement(timeout: TimeInterval) async throws {
        if terminalAcknowledged {
            return
        }
        try await withCheckedThrowingContinuation { continuation in
            terminalAckWaiter = continuation
            terminalAckTimer = Task {
                do {
                    try await Task.sleep(nanoseconds: SessionFrameCodec.durationNanoseconds(timeout))
                } catch {
                    return
                }
                self.expireTerminalAcknowledgement()
            }
        }
    }

    func settle() async {
        let task = task
        await close()
        await task?.value
    }

    private func fail(_ error: Error) async {
        transportError = error
        canceled = true
        finished = true
        ended = true
        deadlineTimer?.cancel()
        deadlineTimer = nil
        task?.cancel()
        await channel.finish(throwing: error)
        await responseWindow.close()
    }

    private func close() async {
        await cancel()
        let waiter = terminalAckWaiter
        terminalAckWaiter = nil
        terminalAckTimer?.cancel()
        terminalAckTimer = nil
        waiter?.resume(throwing: SessionTransportError.disconnected)
    }

    private func expireTerminalAcknowledgement() {
        guard !terminalAcknowledged else { return }
        let waiter = terminalAckWaiter
        terminalAckWaiter = nil
        terminalAckTimer = nil
        waiter?.resume(throwing: SessionTransportError.disconnected)
    }
}
