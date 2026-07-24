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

/// ServiceRuntimeTarget selects exact installer fencing or explicit generic successor following.
public enum ServiceRuntimeTarget: Equatable, Sendable {
    case exact(RuntimeIdentity)
    case anyAuthenticatedSuccessor
}

/// RuntimeClientConfiguration configures one private acquire-through-ready operation.
public struct RuntimeClientConfiguration: Sendable {
    public let path: String
    public let wireBuild: String
    public let role: String
    public let noProgressTimeout: TimeInterval
    public let socket: SocketClient.Configuration
    public let onProgress: (@Sendable (ReadinessProgress) -> Void)?

    public init(
        path: String,
        wireBuild: String,
        role: String,
        noProgressTimeout: TimeInterval,
        socket: SocketClient.Configuration = .init(),
        onProgress: (@Sendable (ReadinessProgress) -> Void)? = nil
    ) {
        self.path = path
        self.wireBuild = wireBuild
        self.role = role
        self.noProgressTimeout = noProgressTimeout
        self.socket = socket
        self.onProgress = onProgress
    }
}
