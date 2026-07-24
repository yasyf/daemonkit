import Foundation

/// Transport proof for one raw unary call attempt.
public enum SocketCallOutcome: Equatable, Sendable {
    case delivered
    case preSendFailure
    case rejected
    case postSendFailure
    case deliveryUnknown
}

/// Result of one raw unary attempt, including its exact delivery proof.
public struct SocketCallAttempt: @unchecked Sendable {
    public let outcome: SocketCallOutcome
    public let terminal: SocketTerminal?
    public let error: (any Error)?

    init(outcome: SocketCallOutcome, terminal: SocketTerminal? = nil, error: (any Error)? = nil) {
        self.outcome = outcome
        self.terminal = terminal
        self.error = error
    }
}

/// The absolute deadline elapsed before a unary response settled.
public struct SocketCallDeadlineExceededError: Error, Equatable, Sendable {}

private final class SocketAttemptSettlement: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: Result<SocketTerminal, any Error>?

    var result: Result<SocketTerminal, any Error>? {
        lock.withLock { stored }
    }

    func finish(_ result: Result<SocketTerminal, any Error>) {
        lock.withLock {
            guard stored == nil else { return }
            stored = result
        }
    }
}

extension SocketClient {
    public struct Configuration: Sendable {
        public var maximumFrameBytes: Int
        public var streamQueueDepth: Int
        public var eventQueueDepth: Int
        public var maximumPendingWrites: Int
        public var handshakeTimeout: TimeInterval
        public var writeTimeout: TimeInterval
        public var cancellationSettlementTimeout: TimeInterval

        public init(
            maximumFrameBytes: Int = daemonKitDefaultMaximumFrameBytes,
            streamQueueDepth: Int = 16,
            eventQueueDepth: Int = 16,
            maximumPendingWrites: Int = 64,
            handshakeTimeout: TimeInterval = 10,
            writeTimeout: TimeInterval = 10,
            cancellationSettlementTimeout: TimeInterval = 5
        ) {
            self.maximumFrameBytes = maximumFrameBytes
            self.streamQueueDepth = streamQueueDepth
            self.eventQueueDepth = eventQueueDepth
            self.maximumPendingWrites = maximumPendingWrites
            self.handshakeTimeout = handshakeTimeout
            self.writeTimeout = writeTimeout
            self.cancellationSettlementTimeout = cancellationSettlementTimeout
        }
    }

    /// Events pushed by the server, bounded by ``Configuration/eventQueueDepth``.
    public var events: SocketEventStream {
        core.events
    }

    /// Server wireBuild identity established by the mandatory handshake.
    public var peerWireBuild: String {
        core.peerWireBuild
    }

    /// Acquires the authenticated exact runtime identity before lifecycle waiting.
    public func acquireRuntimeReceipt(
        expectedRuntimeBuild: String,
        deadline: Date
    ) async throws -> RuntimeProcessReceipt {
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        guard !expectedRuntimeBuild.isEmpty else {
            throw RuntimeReadinessValidationError.invalidResponse("expected runtime build is required")
        }
        let terminal = try await call(
            operation: runtimeReceiptOperation,
            payload: RuntimeReceiptCodec.encodeRequest(),
            deadline: deadline
        )
        guard terminal.error == nil, !terminal.rejected, let payload = terminal.payload else {
            throw RuntimeReceiptUnavailableError()
        }
        let receipt = try RuntimeReceiptCodec.decodeResponse(payload)
        guard receipt.runtimeIdentity.runtimeBuild == expectedRuntimeBuild else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime receipt build mismatch")
        }
        return receipt
    }

    func nextLifecycleSnapshot() async throws -> Data? {
        try await core.nextLifecycleSnapshot()
    }

    func waitUntilClosed() async {
        await core.waitUntilClosed()
    }

    /// Opens a request. Set endInput false when request chunks will follow.
    public func open(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        endInput: Bool = true,
        deadline: Date? = nil
    ) async throws -> SocketCall {
        try await core.open(
            owner: self,
            operation: operation,
            tenant: tenant,
            payload: payload,
            endInput: endInput,
            deadline: deadline
        )
    }

    /// Sends a unary request and waits for its terminal response.
    public func call(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        deadline: Date? = nil
    ) async throws -> SocketTerminal {
        let call = try await open(operation: operation, tenant: tenant, payload: payload, deadline: deadline)
        return try await call.response()
    }

    /// Performs one unary attempt and returns the exact transport outcome.
    public func attempt(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        deadline: Date
    ) async -> SocketCallAttempt {
        let call: SocketCall
        do {
            call = try await core.openClassified(
                owner: self,
                operation: operation,
                tenant: tenant,
                payload: payload,
                endInput: true,
                deadline: deadline
            )
        } catch let failure as SocketOpenFailure {
            return SocketCallAttempt(outcome: failure.outcome, error: failure.cause)
        } catch {
            return SocketCallAttempt(outcome: .preSendFailure, error: error)
        }
        let settlement = SocketAttemptSettlement()
        let response = Task {
            do {
                try await settlement.finish(.success(call.response()))
            } catch {
                settlement.finish(.failure(error))
            }
        }
        return await withTaskCancellationHandler {
            while true {
                if let result = settlement.result {
                    switch result {
                    case let .success(terminal):
                        return SocketCallAttempt(
                            outcome: terminal.rejected ? .rejected : .delivered,
                            terminal: terminal
                        )
                    case let .failure(error):
                        return SocketCallAttempt(outcome: .postSendFailure, error: error)
                    }
                }
                if Task.isCancelled {
                    return SocketCallAttempt(outcome: .postSendFailure, error: CancellationError())
                }
                if deadline <= Date() {
                    response.cancel()
                    Task { await call.cancel() }
                    return SocketCallAttempt(
                        outcome: .postSendFailure,
                        error: SocketCallDeadlineExceededError()
                    )
                }
                try? await Task.sleep(for: .milliseconds(5))
            }
        } onCancel: {
            response.cancel()
            Task { await call.cancel() }
        }
    }

    /// Sends go-away, then closes the session and fails every pending call.
    public func close() async {
        await core.close()
    }

    /// Immediately aborts the session without attempting a protocol write.
    public func abort() {
        core.abort()
    }

    var openCommitHook: (@Sendable () async -> Void)? {
        get { core.openCommitHook }
        set { core.openCommitHook = newValue }
    }

    var requestWriteStartHook: (@Sendable () -> Void)? {
        get { core.requestWriteStartHook }
        set { core.requestWriteStartHook = newValue }
    }

    var requestSettlementHook: (@Sendable () async -> Void)? {
        get { core.requestSettlementHook }
        set { core.requestSettlementHook = newValue }
    }

    var requestSettlementWaitHook: (@Sendable () async -> Void)? {
        get { core.requestSettlementWaitHook }
        set { core.requestSettlementWaitHook = newValue }
    }

    var requestSendAdmissionHook: (@Sendable () async -> Void)? {
        get { core.requestSendAdmissionHook }
        set { core.requestSendAdmissionHook = newValue }
    }

    var requestSendHook: (@Sendable () async -> Void)? {
        get { core.requestSendHook }
        set { core.requestSendHook = newValue }
    }

    var requestSendDrainWaitHook: (@Sendable () -> Void)? {
        get { core.requestSendDrainWaitHook }
        set { core.requestSendDrainWaitHook = newValue }
    }

    var receiveStreamOfferHook: (@Sendable () async -> Void)? {
        get { core.receiveStreamOfferHook }
        set { core.receiveStreamOfferHook = newValue }
    }

    var cancellationTimeoutHook: (@Sendable () async -> Void)? {
        get { core.cancellationTimeoutHook }
        set { core.cancellationTimeoutHook = newValue }
    }

    var cancellationTimeoutResultHook: (@Sendable (Bool) async -> Void)? {
        get { core.cancellationTimeoutResultHook }
        set { core.cancellationTimeoutResultHook = newValue }
    }
}
