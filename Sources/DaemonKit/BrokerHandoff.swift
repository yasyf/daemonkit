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
    case deliveryUnknown
}

private struct BrokerHandoffEnvelope: Codable, Equatable, Sendable {
    let nonce: String
}

enum BrokerHandoffCodec {
    static func makeRequest() throws -> (payload: Data, nonce: Data) {
        var nonce = Data(count: brokerHandoffNonceBytes)
        let status = nonce.withUnsafeMutableBytes { bytes in
            SecRandomCopyBytes(kSecRandomDefault, brokerHandoffNonceBytes, bytes.baseAddress!)
        }
        guard status == errSecSuccess else { throw BrokerHandoffError.nonceGeneration(status) }
        return try (encode(nonce: nonce), nonce)
    }

    static func encode(nonce: Data) throws -> Data {
        guard nonce.count == brokerHandoffNonceBytes else { throw BrokerHandoffError.invalidPayload }
        let payload = try canonicalEncoder().encode(BrokerHandoffEnvelope(
            nonce: nonce.base64EncodedString()
        ))
        guard payload.count <= brokerHandoffMaximumPayloadBytes else {
            throw SessionTransportError.frameTooLarge(
                actual: payload.count,
                maximum: brokerHandoffMaximumPayloadBytes
            )
        }
        return payload
    }

    static func decode(_ payload: Data) throws -> Data {
        guard payload.count <= brokerHandoffMaximumPayloadBytes,
              try hasExactFields(payload)
        else { throw BrokerHandoffError.invalidPayload }
        let envelope: BrokerHandoffEnvelope
        do {
            envelope = try JSONDecoder().decode(BrokerHandoffEnvelope.self, from: payload)
        } catch {
            throw BrokerHandoffError.invalidPayload
        }
        guard let nonce = Data(base64Encoded: envelope.nonce),
              nonce.count == brokerHandoffNonceBytes,
              nonce.base64EncodedString() == envelope.nonce
        else { throw BrokerHandoffError.invalidPayload }
        return nonce
    }

    private static func canonicalEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return encoder
    }

    private static func hasExactFields(_ payload: Data) throws -> Bool {
        guard let root = try JSONSerialization.jsonObject(with: payload) as? [String: Any],
              Set(root.keys) == ["nonce"]
        else { return false }
        return true
    }
}

private let brokerHandoffLog = Logger(
    subsystem: DaemonKit.loggingSubsystem,
    category: "BrokerSocketBridge"
)

private func reportBridgeFailure(_ message: String) {
    brokerHandoffLog.error("\(message, privacy: .public)")
    let line = "\(ISO8601DateFormatter().string(from: Date())) [BrokerSocketBridge] \(message)\n"
    try? FileHandle.standardError.write(contentsOf: Data(line.utf8))
}

private let descriptorInheritanceLock = NSLock()

enum BrokerDelegation: Sendable {
    case wireOp(BrokerHandoffClient)
    case channel(SpawnedChannel)

    func handoff(descriptor: Int32, parentDeadline: Date) async throws {
        switch self {
        case let .wireOp(client):
            try await client.handoff(descriptor: descriptor, parentDeadline: parentDeadline)
        case let .channel(channel):
            defer { Darwin.close(descriptor) }
            try await channel.delegate(descriptor: descriptor, deadline: parentDeadline)
        }
    }

    func close() async {
        if case let .wireOp(client) = self {
            await client.close()
        }
    }
}

actor BrokerHandoffClient {
    private struct Usage {
        var accepted = 0
        var inFlight = 0
    }

    private struct Generation: Sendable {
        let id: UInt64
        let client: Task<SocketClient, Error>
    }

    private let path: String
    private let configuration: SocketClient.Configuration
    private let stateSignal = ServiceStateSignal()
    private var generation: Generation?
    private var usage: [UInt64: Usage] = [:]
    private var nextGeneration: UInt64 = 1
    private var closed = false

    init(
        path: String,
        configuration: SocketClient.Configuration
    ) {
        self.path = path
        self.configuration = configuration
    }

    func handoff(
        descriptor: Int32,
        parentDeadline: Date
    ) async throws {
        var ownsDescriptor = true
        defer {
            if ownsDescriptor {
                Darwin.close(descriptor)
            }
        }
        let deadline = min(parentDeadline, Date().addingTimeInterval(brokerHandoffMaximumDuration))
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        while true {
            let current = try await session(deadline: deadline)
            var currentUsage = usage[current.generation.id] ?? Usage()
            if currentUsage.accepted == 256, currentUsage.inFlight == 0 {
                await retire(current.generation)
                continue
            }
            if currentUsage.inFlight == 4 || currentUsage.accepted + currentUsage.inFlight == 256 {
                try await waitForCapacity(after: stateSignal.currentRevision, deadline: deadline)
                continue
            }
            currentUsage.inFlight += 1
            usage[current.generation.id] = currentUsage
            do {
                let request = try BrokerHandoffCodec.makeRequest()
                ownsDescriptor = false
                let terminal = try await current.client.core.handoff(
                    owner: current.client,
                    descriptor: descriptor,
                    payload: request.payload,
                    deadline: deadline
                )
                guard !terminal.rejected else {
                    throw BrokerHandoffError.responseRejected(terminal.code, terminal.reason)
                }
                guard terminal.error == nil else {
                    throw BrokerHandoffError.invalidPayload
                }
                finish(generation: current.generation.id, accepted: true)
                if usage[current.generation.id]?.accepted == 256 {
                    await retire(current.generation)
                }
                return
            } catch {
                finish(generation: current.generation.id, accepted: false)
                if !ownsDescriptor {
                    await retire(current.generation)
                }
                throw error
            }
        }
    }

    func close() async {
        guard !closed else { return }
        closed = true
        stateSignal.signal()
        if let generation {
            await retire(generation)
        }
    }

    private func session(deadline: Date) async throws -> (generation: Generation, client: SocketClient) {
        guard !closed else { throw ServiceSocketClientError.closed }
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        let current: Generation
        if let generation {
            current = generation
        } else {
            var attemptConfiguration = configuration
            attemptConfiguration.handshakeTimeout = min(
                attemptConfiguration.handshakeTimeout,
                deadline.timeIntervalSinceNow
            )
            let id = nextGeneration
            nextGeneration += 1
            let path = path
            current = Generation(
                id: id,
                client: Task {
                    try await SocketClient(
                        path: path,
                        schema: "",
                        lane: .control,
                        configuration: attemptConfiguration
                    )
                }
            )
            generation = current
        }
        do {
            let client = try await current.client.value
            guard !closed, generation?.id == current.id else {
                await client.close()
                throw ServiceSocketClientError.closed
            }
            return (current, client)
        } catch {
            if generation?.id == current.id {
                generation = nil
                usage.removeValue(forKey: current.id)
                stateSignal.signal()
            }
            throw error
        }
    }

    private func retire(_ current: Generation) async {
        guard generation?.id == current.id else { return }
        generation = nil
        usage.removeValue(forKey: current.id)
        stateSignal.signal()
        if let client = try? await current.client.value {
            await client.close()
        }
    }

    private func finish(generation: UInt64, accepted: Bool) {
        guard var current = usage[generation], current.inFlight > 0 else { return }
        current.inFlight -= 1
        if accepted {
            current.accepted += 1
        }
        usage[generation] = current
        stateSignal.signal()
    }

    private func waitForCapacity(after revision: UInt64, deadline: Date) async throws {
        guard deadline > Date() else { throw ServiceSocketClientError.deadlineExceeded }
        try await withThrowingTaskGroup(of: Void.self) { group in
            group.addTask { try await self.stateSignal.wait(after: revision) }
            group.addTask {
                try await Task.sleep(until: .now + .seconds(deadline.timeIntervalSinceNow))
                throw ServiceSocketClientError.deadlineExceeded
            }
            _ = try await group.next()
            group.cancelAll()
        }
    }
}

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
    private let lifecycleClient: ServiceSocketClient
    private let delegation: BrokerDelegation
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
        lifecycle: RuntimeClientConfiguration
    ) throws {
        try self.init(
            path: container.socketPath(leaf: socket),
            lifecycle: lifecycle
        )
    }

    init(
        path: String,
        lifecycle: RuntimeClientConfiguration
    ) throws {
        self.path = path
        lifecycleClient = try ServiceSocketClient(
            connection: lifecycle.connection,
            schema: lifecycle.schema,
            lane: lifecycle.lane,
            configuration: lifecycle.socket,
            onProgress: lifecycle.onProgress
        )
        delegation = switch lifecycle.connection {
        case let .path(daemonPath):
            .wireOp(BrokerHandoffClient(path: daemonPath, configuration: lifecycle.socket))
        case let .spawned(channel):
            .channel(channel)
        }
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
                        group.addTask { [lifecycleClient, delegation] in
                            var ownsDescriptor = true
                            defer {
                                if ownsDescriptor {
                                    Darwin.close(accepted)
                                }
                            }
                            do {
                                let deadline = Date().addingTimeInterval(brokerHandoffMaximumDuration)
                                try await lifecycleClient.waitReady(deadline: deadline)
                                ownsDescriptor = false
                                try await delegation.handoff(
                                    descriptor: accepted,
                                    parentDeadline: deadline
                                )
                            } catch {
                                reportBridgeFailure("connected socket handoff failed: \(String(describing: error))")
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
            await lifecycleClient.close()
            await delegation.close()
            if expectedStop {
                return
            }
            throw error
        }
        closeListener(bound)
        await lifecycleClient.close()
        await delegation.close()
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
        await lifecycleClient.close()
        await delegation.close()
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
