import Darwin
import Foundation

/// Errors thrown while binding or running a ``SocketServer``.
enum SocketServerError: Error, Sendable {
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
struct SocketPeer: Sendable {
    let effectiveUserID: uid_t
    let effectiveGroupID: gid_t
}

/// One ordered request-stream chunk.
public struct SocketRequestChunk: Sendable {
    public let sequence: UInt32
    public let payload: Data
    public let end: Bool
}

/// A request admitted on a persistent session.
struct SocketRequest: Sendable {
    let id: UInt64
    let operation: String
    let tenant: String
    let payload: Data
    let chunks: SocketChunkStream
    let peer: SocketPeer
    let peerWireBuild: String
    let session: SocketSession
    let runtimeAdmission: RuntimeAdmissionPin?
}

/// A trusted persistent server session exposed to request handlers.
final class SocketSession: @unchecked Sendable {
    weak var implementation: ServerSession?
    private let lifecycle: SocketSessionLifecycle

    init(implementation: ServerSession, lifecycle: SocketSessionLifecycle) {
        self.implementation = implementation
        self.lifecycle = lifecycle
    }

    /// Whether the authenticated peer connection remains live.
    var isConnected: Bool {
        lifecycle.isConnected
    }

    /// Suspends until the authenticated peer connection closes.
    func waitUntilClosed() async {
        await lifecycle.waitUntilClosed()
    }

    /// Pushes one event to the peer on the session's serialized writer.
    func pushEvent(topic: String, payload: Data = Data()) async throws {
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
