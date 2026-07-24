import Foundation
import Synchronization

/// A machine-readable terminal response status.
public struct SocketResponseCode: RawRepresentable, Equatable, Hashable, Sendable {
    public let rawValue: String

    public init(rawValue: String) {
        self.rawValue = rawValue
    }

    public static let runtimeStarting = SocketResponseCode(rawValue: "runtime_starting")
    public static let runtimeDraining = SocketResponseCode(rawValue: "runtime_draining")
    public static let buildMismatch = SocketResponseCode(rawValue: "build_mismatch")
    public static let sessionCapacity = SocketResponseCode(rawValue: "session_capacity")
    public static let peerUntrusted = SocketResponseCode(rawValue: "peer_untrusted")
    public static let permissionDenied = SocketResponseCode(rawValue: "permission_denied")
    public static let invalidRequest = SocketResponseCode(rawValue: "invalid_request")
    public static let requestTooLarge = SocketResponseCode(rawValue: "request_too_large")
    public static let handoffPendingCapacity = SocketResponseCode(rawValue: "handoff_pending_capacity")
    public static let handoffReplay = SocketResponseCode(rawValue: "handoff_replay")
    public static let handoffSessionExhausted = SocketResponseCode(rawValue: "handoff_session_exhausted")
}

/// An authenticated server's typed handshake rejection.
public struct SocketHandshakeRejectionError: Error, CustomStringConvertible, Sendable {
    public let code: SocketResponseCode
    public let reason: String

    public var description: String {
        reason
    }
}

/// An exact wire-build mismatch proven by the handshake acknowledgment.
public struct SocketWireBuildMismatchError: Error, CustomStringConvertible, Equatable, Sendable {
    public let server: String
    public let client: String

    public var description: String {
        "wire: build mismatch: server=\(server.debugDescription) client=\(client.debugDescription)"
    }
}

/// The terminal envelope emitted after any response stream ends.
public struct SocketTerminal: Sendable {
    public let payload: Data?
    public let error: String?
    public let rejected: Bool
    public let code: SocketResponseCode?
    public let reason: String?
    let afterWrite: (@Sendable () -> Void)?

    public init(
        payload: Data? = nil,
        error: String? = nil,
        rejected: Bool = false,
        code: SocketResponseCode? = nil,
        reason: String? = nil
    ) {
        self.payload = payload
        self.error = error
        self.rejected = rejected
        self.code = code
        self.reason = reason
        afterWrite = nil
    }

    init(payload: Data, afterWrite: @escaping @Sendable () -> Void) {
        self.payload = payload
        error = nil
        rejected = false
        code = nil
        reason = nil
        self.afterWrite = afterWrite
    }
}

/// A pull-driven response stream whose terminal envelope is resolved after its final chunk.
struct SocketResponseStream: Sendable {
    private let nextChunkOperation: @Sendable () async throws -> Data?
    private let terminalOperation: @Sendable () async throws -> SocketTerminal
    private let cancellation: SocketResponseCancellation

    init(
        nextChunk: @escaping @Sendable () async throws -> Data?,
        terminal: @escaping @Sendable () async throws -> SocketTerminal,
        cancel: @escaping @Sendable () -> Void
    ) {
        nextChunkOperation = nextChunk
        terminalOperation = terminal
        cancellation = SocketResponseCancellation(operation: cancel)
    }

    func nextChunk() async throws -> Data? {
        try await nextChunkOperation()
    }

    func terminal() async throws -> SocketTerminal {
        try await terminalOperation()
    }

    func cancel() {
        cancellation.cancel()
    }
}

/// A response is either terminal now or a stream with a deferred terminal envelope.
enum SocketResponse: Sendable {
    case terminal(SocketTerminal)
    case stream(SocketResponseStream)
}

extension SocketResponse {
    /// Relays a client call without buffering its output or resolving its terminal envelope early.
    static func relaying(_ call: SocketCall) -> SocketResponse {
        .stream(SocketResponseStream(
            nextChunk: {
                while let chunk = try await call.chunks.nextChunk() {
                    if !chunk.end {
                        return chunk.payload
                    }
                }
                return nil
            },
            terminal: { try await call.response() },
            cancel: { Task { await call.cancel() } }
        ))
    }
}

actor SocketResponseSettlement {
    private let stream: SocketResponseStream
    private var task: Task<SocketTerminal, Error>?

    init(stream: SocketResponseStream) {
        self.stream = stream
    }

    func value() async -> Result<SocketTerminal, Error> {
        if let task {
            return await task.result
        }
        let task = Task { try await stream.terminal() }
        self.task = task
        return await task.result
    }
}

private final class SocketResponseCancellation: Sendable {
    private let operation: @Sendable () -> Void
    private let canceled = Mutex(false)

    init(operation: @escaping @Sendable () -> Void) {
        self.operation = operation
    }

    func cancel() {
        let shouldCancel = canceled.withLock { canceled in
            if canceled {
                return false
            }
            canceled = true
            return true
        }
        if shouldCancel {
            operation()
        }
    }
}
