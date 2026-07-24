import Darwin
import Foundation
import os
import Security

let brokerHandoffOperation = "daemon.broker-handoff.v1"
let brokerHandoffMaximumPayloadBytes = 1024
private let brokerHandoffNonceBytes = 32
let brokerHandoffMaximumDuration: TimeInterval = 5

/// BrokerHandoffError reports a fail-closed connected-socket handoff.
public enum BrokerHandoffError: Error, Equatable, Sendable {
    case invalidPayload
    case nonceGeneration(OSStatus)
    case responseRejected(SocketResponseCode?, String?)
    case responseMismatch
    case deliveryUnknown
}

private struct BrokerHandoffEnvelope: Codable, Equatable, Sendable {
    let protocolVersion: UInt16
    let nonce: String
    let runtimeIdentity: RuntimeIdentity

    enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol"
        case nonce
        case runtimeIdentity = "runtime_identity"
    }
}

enum BrokerHandoffCodec {
    static func makeRequest(identity: RuntimeIdentity) throws -> (payload: Data, nonce: Data) {
        var nonce = Data(count: brokerHandoffNonceBytes)
        let status = nonce.withUnsafeMutableBytes { bytes in
            SecRandomCopyBytes(kSecRandomDefault, brokerHandoffNonceBytes, bytes.baseAddress!)
        }
        guard status == errSecSuccess else { throw BrokerHandoffError.nonceGeneration(status) }
        return try (encode(nonce: nonce, identity: identity), nonce)
    }

    static func encode(nonce: Data, identity: RuntimeIdentity) throws -> Data {
        guard nonce.count == brokerHandoffNonceBytes,
              !identity.runtimeBuild.isEmpty,
              !identity.processGeneration.isEmpty
        else { throw BrokerHandoffError.invalidPayload }
        let payload = try canonicalEncoder().encode(BrokerHandoffEnvelope(
            protocolVersion: daemonKitSessionProtocolVersion,
            nonce: nonce.base64EncodedString(),
            runtimeIdentity: identity
        ))
        guard payload.count <= brokerHandoffMaximumPayloadBytes else {
            throw SessionTransportError.frameTooLarge(
                actual: payload.count,
                maximum: brokerHandoffMaximumPayloadBytes
            )
        }
        return payload
    }

    static func decode(_ payload: Data) throws -> (nonce: Data, identity: RuntimeIdentity) {
        guard payload.count <= brokerHandoffMaximumPayloadBytes,
              try hasExactFields(payload)
        else { throw BrokerHandoffError.invalidPayload }
        let envelope: BrokerHandoffEnvelope
        do {
            envelope = try JSONDecoder().decode(BrokerHandoffEnvelope.self, from: payload)
        } catch {
            throw BrokerHandoffError.invalidPayload
        }
        guard envelope.protocolVersion == daemonKitSessionProtocolVersion,
              let nonce = Data(base64Encoded: envelope.nonce),
              nonce.count == brokerHandoffNonceBytes,
              nonce.base64EncodedString() == envelope.nonce,
              !envelope.runtimeIdentity.runtimeBuild.isEmpty,
              !envelope.runtimeIdentity.processGeneration.isEmpty
        else { throw BrokerHandoffError.invalidPayload }
        guard try encode(nonce: nonce, identity: envelope.runtimeIdentity) == payload else {
            throw BrokerHandoffError.invalidPayload
        }
        return (nonce, envelope.runtimeIdentity)
    }

    private static func canonicalEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return encoder
    }

    private static func hasExactFields(_ payload: Data) throws -> Bool {
        guard let root = try JSONSerialization.jsonObject(with: payload) as? [String: Any],
              Set(root.keys) == ["protocol", "nonce", "runtime_identity"],
              let identity = root["runtime_identity"] as? [String: Any],
              Set(identity.keys) == ["runtime_build", "process_generation"]
        else { return false }
        return true
    }
}

private let brokerHandoffLog = Logger(
    subsystem: DaemonKit.loggingSubsystem,
    category: "BrokerSocketBridge"
)
private let descriptorInheritanceLock = NSLock()

/// BrokerSocketBridge owns the App Group listener and exposes no accepted descriptor.
public final class BrokerSocketBridge: @unchecked Sendable {
    private struct SocketNode: Equatable {
        let device: dev_t
        let inode: ino_t
    }

    private struct BoundListener {
        let descriptor: Int32
        let node: SocketNode
        let lockDescriptor: Int32
    }

    private let path: String
    private let expectedRuntimeBuild: String
    private let client: ServiceSocketClient
    private let acceptQueue = DispatchQueue(label: "com.yasyf.daemonkit.BrokerSocketBridge.accept")
    private let lock = NSLock()
    private var listener: Int32 = -1
    private var listenerNode: SocketNode?
    private var listenerLockDescriptor: Int32 = -1
    private var running = false
    private var stopped = false

    public convenience init(
        container: AppGroupContainer,
        socket: AppGroupContainer.SocketLeaf,
        daemon: RuntimeClientConfiguration,
        expectedRuntimeBuild: String
    ) throws {
        try self.init(
            path: container.socketPath(leaf: socket),
            daemon: daemon,
            expectedRuntimeBuild: expectedRuntimeBuild
        )
    }

    init(
        path: String,
        daemon: RuntimeClientConfiguration,
        expectedRuntimeBuild: String
    ) throws {
        guard !expectedRuntimeBuild.isEmpty else { throw BrokerHandoffError.invalidPayload }
        self.path = path
        self.expectedRuntimeBuild = expectedRuntimeBuild
        client = try ServiceSocketClient(
            path: daemon.path,
            wireBuild: daemon.wireBuild,
            role: daemon.role,
            noProgressTimeout: daemon.noProgressTimeout,
            configuration: daemon.socket,
            onProgress: daemon.onProgress
        )
    }

    /// Runs one bounded listener until cancellation or ``shutdown()``.
    public func run() async throws {
        guard lock.withLock({ !running && !stopped }) else {
            throw SocketServerError.alreadyRunning
        }
        let bound = try bindListener()
        let installed = lock.withLock { () -> Bool in
            guard !running, !stopped else { return false }
            running = true
            listener = bound.descriptor
            listenerNode = bound.node
            listenerLockDescriptor = bound.lockDescriptor
            return true
        }
        guard installed else {
            release(bound)
            throw SocketServerError.alreadyRunning
        }
        do {
            try await withTaskCancellationHandler {
                try await withThrowingTaskGroup(of: Void.self) { group in
                    var pending = 0
                    while !Task.isCancelled, !lock.withLock({ stopped }) {
                        if pending == 4 {
                            _ = try await group.next()
                            pending -= 1
                        }
                        let accepted = try await acceptConnection(bound.descriptor)
                        pending += 1
                        group.addTask { [client, expectedRuntimeBuild] in
                            do {
                                try await client.handoff(
                                    descriptor: accepted,
                                    expectedRuntimeBuild: expectedRuntimeBuild,
                                    parentDeadline: Date().addingTimeInterval(brokerHandoffMaximumDuration)
                                )
                            } catch {
                                brokerHandoffLog.error("connected socket handoff failed: \(String(describing: error), privacy: .public)")
                            }
                        }
                    }
                    while pending > 0 {
                        _ = try await group.next()
                        pending -= 1
                    }
                }
            } onCancel: {
                self.cancelAdmission(bound)
            }
            try Task.checkCancellation()
        } catch {
            let expectedStop = lock.withLock { stopped }
            closeListener(bound)
            await client.close()
            if expectedStop {
                return
            }
            throw error
        }
        closeListener(bound)
        await client.close()
    }

    /// Stops admission and closes the authenticated outbound session.
    public func shutdown() async {
        let owned = lock.withLock { () -> BoundListener? in
            stopped = true
            guard listener >= 0, let node = listenerNode else { return nil }
            let owned = BoundListener(
                descriptor: listener,
                node: node,
                lockDescriptor: listenerLockDescriptor
            )
            listener = -1
            listenerNode = nil
            listenerLockDescriptor = -1
            return owned
        }
        if let owned {
            release(owned)
        }
        await client.close()
    }

    private func bindListener() throws -> BoundListener {
        let lockDescriptor = try acquirePathLock()
        var descriptor: Int32 = -1
        var ownedNode: SocketNode?
        var complete = false
        defer {
            if !complete {
                if descriptor >= 0 {
                    Darwin.close(descriptor)
                }
                if let ownedNode {
                    _ = unlinkIfOwned(ownedNode)
                }
                flock(lockDescriptor, LOCK_UN)
                Darwin.close(lockDescriptor)
            }
        }
        try reclaimStaleSocket()
        var address = try makeAddress()
        descriptor = try descriptorInheritanceLock.withLock { () throws -> Int32 in
            let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
            guard descriptor >= 0 else { throw SocketServerError.socketFailed(errno: errno) }
            guard fcntl(descriptor, F_SETFD, FD_CLOEXEC) == 0 else {
                let code = errno
                Darwin.close(descriptor)
                throw SessionTransportError.systemCall(operation: "fcntl", errno: code)
            }
            return descriptor
        }
        let bound = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard bound == 0 else { throw SocketServerError.bindFailed(path: path, errno: errno) }
        guard let node = socketNode() else {
            throw SocketServerError.addressInUse(path: path)
        }
        ownedNode = node
        guard chmod(path, 0o600) == 0 else {
            throw SocketServerError.chmodFailed(path: path, errno: errno)
        }
        guard listen(descriptor, 4) == 0 else { throw SocketServerError.listenFailed(errno: errno) }
        let flags = fcntl(descriptor, F_GETFL)
        guard flags >= 0, fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0 else {
            throw SessionTransportError.systemCall(operation: "fcntl", errno: errno)
        }
        complete = true
        return BoundListener(
            descriptor: descriptor,
            node: node,
            lockDescriptor: lockDescriptor
        )
    }

    private func reclaimStaleSocket() throws {
        guard access(path, F_OK) == 0 else { return }
        guard let observed = socketNode() else {
            throw SocketServerError.addressInUse(path: path)
        }
        guard unlinkIfOwned(observed) else {
            throw SocketServerError.addressInUse(path: path)
        }
    }

    private func acquirePathLock() throws -> Int32 {
        let lockPath = path + ".lock"
        let descriptor = try descriptorInheritanceLock.withLock { () throws -> Int32 in
            let descriptor = open(lockPath, O_CREAT | O_RDWR | O_NOFOLLOW, mode_t(0o600))
            guard descriptor >= 0 else {
                throw SessionTransportError.systemCall(operation: "open", errno: errno)
            }
            guard fcntl(descriptor, F_SETFD, FD_CLOEXEC) == 0 else {
                let code = errno
                Darwin.close(descriptor)
                throw SessionTransportError.systemCall(operation: "fcntl", errno: code)
            }
            guard fchmod(descriptor, mode_t(0o600)) == 0 else {
                let code = errno
                Darwin.close(descriptor)
                throw SessionTransportError.systemCall(operation: "fchmod", errno: code)
            }
            return descriptor
        }
        guard flock(descriptor, LOCK_EX | LOCK_NB) == 0 else {
            let code = errno
            Darwin.close(descriptor)
            if code == EWOULDBLOCK || code == EAGAIN {
                throw SocketServerError.addressInUse(path: path)
            }
            throw SessionTransportError.systemCall(operation: "flock", errno: code)
        }
        return descriptor
    }

    private func acceptConnection(_ listener: Int32) async throws -> Int32 {
        try await acceptQueue.performIO {
            while true {
                if self.lock.withLock({ self.stopped }) {
                    throw CancellationError()
                }
                var readable = pollfd(fd: listener, events: Int16(POLLIN), revents: 0)
                let result = poll(&readable, 1, 250)
                if result == 0 {
                    continue
                }
                if result < 0 {
                    if errno == EINTR {
                        continue
                    }
                    throw SessionTransportError.systemCall(operation: "poll", errno: errno)
                }
                let owned = try descriptorInheritanceLock.withLock { () throws -> Int32? in
                    let accepted = accept(listener, nil, nil)
                    guard accepted >= 0 else {
                        if errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED {
                            return nil
                        }
                        throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                    }
                    let owned = fcntl(accepted, F_DUPFD_CLOEXEC, 0)
                    let code = errno
                    Darwin.close(accepted)
                    guard owned >= 0 else {
                        throw SessionTransportError.systemCall(operation: "fcntl", errno: code)
                    }
                    return owned
                }
                if let owned {
                    return owned
                }
            }
        }
    }

    private func closeListener(_ bound: BoundListener) {
        let shouldClose = lock.withLock { () -> Bool in
            guard listener == bound.descriptor,
                  listenerNode == bound.node,
                  listenerLockDescriptor == bound.lockDescriptor
            else { return false }
            listener = -1
            listenerNode = nil
            listenerLockDescriptor = -1
            stopped = true
            return true
        }
        if shouldClose {
            release(bound)
        }
    }

    private func cancelAdmission(_ bound: BoundListener) {
        let shouldClose = lock.withLock { () -> Bool in
            stopped = true
            guard listener == bound.descriptor,
                  listenerNode == bound.node,
                  listenerLockDescriptor == bound.lockDescriptor
            else { return false }
            listener = -1
            listenerNode = nil
            listenerLockDescriptor = -1
            return true
        }
        if shouldClose {
            release(bound)
        }
    }

    private func release(_ bound: BoundListener) {
        Darwin.close(bound.descriptor)
        _ = unlinkIfOwned(bound.node)
        flock(bound.lockDescriptor, LOCK_UN)
        Darwin.close(bound.lockDescriptor)
    }

    private func socketNode() -> SocketNode? {
        var status = stat()
        guard lstat(path, &status) == 0, status.st_mode & S_IFMT == S_IFSOCK else {
            return nil
        }
        return SocketNode(device: status.st_dev, inode: status.st_ino)
    }

    private func unlinkIfOwned(_ expected: SocketNode) -> Bool {
        guard let current = socketNode() else { return access(path, F_OK) != 0 }
        guard current == expected else { return false }
        return unlink(path) == 0 || errno == ENOENT
    }

    private func makeAddress() throws -> sockaddr_un {
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let capacity = MemoryLayout.size(ofValue: address.sun_path)
        let bytes = Array(path.utf8)
        guard bytes.count < capacity else {
            throw SocketServerError.pathTooLong(path: path, limit: capacity - 1)
        }
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            bytes.withUnsafeBytes { destination.copyMemory(from: $0) }
        }
        return address
    }
}
