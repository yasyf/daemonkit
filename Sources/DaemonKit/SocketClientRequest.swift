import Darwin
import Foundation

struct SocketOpenFailure: Error {
    let outcome: SocketCallOutcome
    let cause: any Error
}

private final class RequestWriteMarker: @unchecked Sendable {
    private let lock = NSLock()
    private var value = false

    var started: Bool {
        lock.withLock { value }
    }

    func markStarted() {
        lock.withLock { value = true }
    }
}

extension SocketClientCore {
    /// Opens a request. Set endInput false when request chunks will follow.
    func open(
        owner: SocketClient,
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        endInput: Bool = true,
        deadline: Date? = nil
    ) async throws -> SocketCall {
        do {
            return try await openClassified(
                owner: owner,
                operation: operation,
                tenant: tenant,
                payload: payload,
                endInput: endInput,
                deadline: deadline
            )
        } catch let failure as SocketOpenFailure {
            throw failure.cause
        }
    }

    func openClassified(
        owner: SocketClient,
        operation: String,
        tenant: String,
        payload: Data,
        endInput: Bool,
        deadline: Date?
    ) async throws -> SocketCall {
        let deadlineMilliseconds = deadline.map(SessionTime.unixMilliseconds) ?? 0
        try validateRequest(
            operation: operation,
            tenant: tenant,
            payload: payload,
            endInput: endInput,
            deadlineMilliseconds: deadlineMilliseconds
        )
        let (id, state) = try reserveRequest(endInput: endInput)
        let marker = RequestWriteMarker()
        var committed = false
        do {
            try await writer.writeTracked(requestFrame(
                id: id,
                operation: operation,
                tenant: tenant,
                payload: payload,
                endInput: endInput,
                deadlineMilliseconds: deadlineMilliseconds
            )) {
                marker.markStarted()
                self.requestWriteStartHook?()
            }
            committed = true
            await openCommitHook?()
            try Task.checkCancellation()
            try await write(SessionFrame(
                kind: .window,
                id: id,
                sequence: UInt32(configuration.streamQueueDepth)
            ))
            try Task.checkCancellation()
        } catch is CancellationError {
            try await settleCanceledOpen(id: id, state: state, committed: committed)
        } catch {
            let failure = classifyOpenFailure(error, committed: committed, started: marker.started)
            fail(failure.cause)
            throw failure
        }
        return SocketCall(owner: owner, client: self, id: id, state: state)
    }

    func handoff(
        owner: SocketClient,
        descriptor: Int32,
        payload: Data,
        deadline: Date
    ) async throws -> SocketTerminal {
        var writerOwnsDescriptor = false
        defer {
            if !writerOwnsDescriptor {
                Darwin.close(descriptor)
            }
        }
        guard payload.count <= brokerHandoffMaximumPayloadBytes else {
            throw SessionTransportError.frameTooLarge(
                actual: payload.count,
                maximum: brokerHandoffMaximumPayloadBytes
            )
        }
        let deadlineMilliseconds = SessionTime.unixMilliseconds(deadline)
        try validateRequest(
            operation: brokerHandoffOperation,
            tenant: "",
            payload: payload,
            endInput: true,
            deadlineMilliseconds: deadlineMilliseconds
        )
        let (id, state) = try reserveRequest(endInput: true)
        var dispatched = false
        do {
            writerOwnsDescriptor = true
            try await writer.writePassingDescriptor(
                requestFrame(
                    id: id,
                    operation: brokerHandoffOperation,
                    tenant: "",
                    payload: payload,
                    endInput: true,
                    deadlineMilliseconds: deadlineMilliseconds
                ),
                descriptor: descriptor,
                deadline: deadline
            )
            dispatched = true
        } catch {
            _ = await state.finish(throwing: error)
            _ = remove(id)
            fail(error)
            if dispatched || error is BrokerHandoffError {
                throw BrokerHandoffError.deliveryUnknown
            }
            throw error
        }
        do {
            let call = SocketCall(
                owner: owner,
                client: self,
                id: id,
                state: state
            )
            let remaining = deadline.timeIntervalSinceNow
            guard remaining > 0 else { throw ServiceSocketClientError.deadlineExceeded }
            return try await withThrowingTaskGroup(of: SocketTerminal.self) { group in
                group.addTask { try await call.response() }
                group.addTask {
                    try await Task.sleep(for: .seconds(remaining))
                    throw ServiceSocketClientError.deadlineExceeded
                }
                guard let terminal = try await group.next() else {
                    throw SessionTransportError.disconnected
                }
                group.cancelAll()
                return terminal
            }
        } catch {
            fail(error)
            throw BrokerHandoffError.deliveryUnknown
        }
    }
}

private extension SocketClientCore {
    func validateRequest(
        operation: String,
        tenant: String,
        payload: Data,
        endInput: Bool,
        deadlineMilliseconds: Int64
    ) throws {
        guard !operation.isEmpty else {
            throw SocketOpenFailure(
                outcome: .preSendFailure,
                cause: SessionTransportError.invalidFrame("empty operation")
            )
        }
        do {
            let body = try SessionFrameCodec.encode(requestFrame(
                id: 1,
                operation: operation,
                tenant: tenant,
                payload: payload,
                endInput: endInput,
                deadlineMilliseconds: deadlineMilliseconds
            ))
            guard body.count <= configuration.maximumFrameBytes else {
                throw SessionTransportError.frameTooLarge(
                    actual: body.count,
                    maximum: configuration.maximumFrameBytes
                )
            }
        } catch let failure as SocketOpenFailure {
            throw failure
        } catch {
            throw SocketOpenFailure(outcome: .preSendFailure, cause: error)
        }
    }

    func reserveRequest(endInput: Bool) throws -> (UInt64, ClientRequestState) {
        try lock.withLock {
            guard case .open = closeState else {
                throw SocketOpenFailure(outcome: .preSendFailure, cause: SessionTransportError.disconnected)
            }
            let id = nextID
            nextID += 1
            let state = ClientRequestState(
                streamQueueDepth: configuration.streamQueueDepth,
                sendEnded: endInput,
                settlementHook: requestSettlementHook,
                settlementWaitHook: requestSettlementWaitHook,
                sendAdmissionHook: requestSendAdmissionHook,
                sendHook: requestSendHook,
                sendDrainWaitHook: requestSendDrainWaitHook
            )
            pending[id] = state
            return (id, state)
        }
    }

    func settleCanceledOpen(
        id: UInt64,
        state: ClientRequestState,
        committed: Bool
    ) async throws -> Never {
        if committed {
            await SocketCall.cancel(client: self, id: id, state: state)
        } else {
            _ = await state.finish(throwing: CancellationError())
            _ = remove(id)
        }
        throw SocketOpenFailure(
            outcome: committed ? .postSendFailure : .preSendFailure,
            cause: CancellationError()
        )
    }

    func classifyOpenFailure(_ error: any Error, committed: Bool, started: Bool) -> SocketOpenFailure {
        if let failure = error as? SocketOpenFailure {
            return failure
        }
        if committed {
            return SocketOpenFailure(outcome: .postSendFailure, cause: error)
        }
        return SocketOpenFailure(outcome: started ? .deliveryUnknown : .preSendFailure, cause: error)
    }

    func requestFrame(
        id: UInt64,
        operation: String,
        tenant: String,
        payload: Data,
        endInput: Bool,
        deadlineMilliseconds: Int64
    ) -> SessionFrame {
        SessionFrame(
            kind: .request,
            flags: endInput ? .end : [],
            id: id,
            deadlineUnixMilliseconds: deadlineMilliseconds,
            operation: operation,
            tenant: tenant,
            payload: payload
        )
    }
}
