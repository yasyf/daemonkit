import Foundation

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
