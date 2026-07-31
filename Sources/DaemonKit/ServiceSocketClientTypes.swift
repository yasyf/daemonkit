import Foundation

/// Errors raised by a generation-aware service socket client.
public enum ServiceSocketClientError: Error, Equatable, Sendable {
    case closed
    case deadlineExceeded
    case malformedAttempt
}

/// The proof required before a logical call may be repeated.
public enum ServiceSocketReplayPolicy: Equatable, Sendable {
    case provenNonDispatch
    case idempotent
}

/// RuntimeClientConfiguration configures one private connect-through-ready operation.
public struct RuntimeClientConfiguration: Sendable {
    public let path: String
    public let schema: String
    public let lane: SessionLane
    public let socket: SocketClient.Configuration
    public let onProgress: (@Sendable (PhaseSnapshot) -> Void)?

    public init(
        path: String,
        schema: String,
        lane: SessionLane = .business,
        socket: SocketClient.Configuration = .init(),
        onProgress: (@Sendable (PhaseSnapshot) -> Void)? = nil
    ) {
        self.path = path
        self.schema = schema
        self.lane = lane
        self.socket = socket
        self.onProgress = onProgress
    }
}
