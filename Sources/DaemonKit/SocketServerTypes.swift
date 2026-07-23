import Darwin
import Foundation

/// Errors thrown while binding or running a ``SocketServer``.
public enum SocketServerError: Error, Sendable {
    case pathTooLong(path: String, limit: Int)
    case addressInUse(path: String)
    case socketFailed(errno: Int32)
    case bindFailed(path: String, errno: Int32)
    case chmodFailed(path: String, errno: Int32)
    case listenFailed(errno: Int32)
    case alreadyRunning
    case emptyWireBuild
}

/// The immutable OS identity captured from an accepted socket.
public struct SocketPeer: Sendable {
    public let effectiveUserID: uid_t
    public let effectiveGroupID: gid_t
}

/// One ordered request-stream chunk.
public struct SocketRequestChunk: Sendable {
    public let sequence: UInt32
    public let payload: Data
    public let end: Bool
}

/// A request admitted on a persistent session.
public struct SocketRequest: Sendable {
    public let id: UInt64
    public let operation: String
    public let tenant: String
    public let payload: Data
    public let chunks: SocketChunkStream
    public let peer: SocketPeer
    public let peerWireBuild: String
    public let session: SocketSession
}

/// A trusted persistent server session exposed to request handlers.
public final class SocketSession: @unchecked Sendable {
    weak var implementation: ServerSession?
    private let lifecycle: SocketSessionLifecycle

    init(implementation: ServerSession, lifecycle: SocketSessionLifecycle) {
        self.implementation = implementation
        self.lifecycle = lifecycle
    }

    /// Whether the authenticated peer connection remains live.
    public var isConnected: Bool {
        lifecycle.isConnected
    }

    /// Suspends until the authenticated peer connection closes.
    public func waitUntilClosed() async {
        await lifecycle.waitUntilClosed()
    }

    /// Pushes one event to the peer on the session's serialized writer.
    public func pushEvent(topic: String, payload: Data = Data()) async throws {
        guard !topic.isEmpty else {
            throw SessionTransportError.invalidFrame("empty event topic")
        }
        guard let implementation else {
            throw SessionTransportError.disconnected
        }
        try await implementation.pushEvent(topic: topic, payload: payload)
    }
}

final class SocketSessionLifecycle: @unchecked Sendable {
    private let lock = NSLock()
    private var connected = true
    private var waiters: [CheckedContinuation<Void, Never>] = []

    var isConnected: Bool {
        lock.withLock { connected }
    }

    func waitUntilClosed() async {
        await withCheckedContinuation { continuation in
            let resume = lock.withLock {
                guard connected else { return true }
                waiters.append(continuation)
                return false
            }
            if resume {
                continuation.resume()
            }
        }
    }

    func close() {
        let pending = lock.withLock {
            guard connected else { return [CheckedContinuation<Void, Never>]() }
            connected = false
            let pending = waiters
            waiters.removeAll()
            return pending
        }
        for waiter in pending {
            waiter.resume()
        }
    }
}
