import Foundation
import Synchronization

/// The terminal envelope emitted after any response stream ends.
public struct SocketTerminal: Sendable {
    public let payload: Data?
    public let error: String?
    public let rejected: Bool
    public let reason: String?

    public init(
        payload: Data? = nil,
        error: String? = nil,
        rejected: Bool = false,
        reason: String? = nil
    ) {
        self.payload = payload
        self.error = error
        self.rejected = rejected
        self.reason = reason
    }
}

/// A pull-driven response stream whose terminal envelope is resolved after its final chunk.
public struct SocketResponseStream: Sendable {
    private let nextChunkOperation: @Sendable () async throws -> Data?
    private let terminalOperation: @Sendable () async throws -> SocketTerminal
    private let cancellation: SocketResponseCancellation

    public init(
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
public enum SocketResponse: Sendable {
    case terminal(SocketTerminal)
    case stream(SocketResponseStream)
}

public extension SocketResponse {
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
