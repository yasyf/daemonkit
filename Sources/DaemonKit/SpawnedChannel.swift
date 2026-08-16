import Darwin
import Foundation

/// SpawnedChannelError rejects a spawn channel that cannot be proven, spoken,
/// or served.
public enum SpawnedChannelError: Error, Equatable, Sendable {
    case alreadyClaimed
    case notSpawned
    case invalidConveyance(String)
    case untrustedDescriptor(String)
    case protocolViolation(String)
    case delegationRefused(String)
    case deadlineExceeded
    case closed
}

/// How a socket client reaches its daemon: a filesystem socket path, or a
/// connection minted over a ChannelHandoff spawn channel.
public enum SocketConnection: Sendable {
    case path(String)
    case spawned(SpawnedChannel)
}

private let spawnedNonceEnvironment = "DAEMONKIT_SPAWNED_NONCE"
private let spawnedLimitsEnvironment = "DAEMONKIT_SPAWNED_LIMITS"
private let spawnedHandoffDescriptor: Int32 = 3
private let spawnedNonceBytes = 32
private let spawnChannelMaxPayload = 1024

/// SpawnedChannel is the claimed child end of a ChannelHandoff spawn: the
/// proven fd-3 socketpair to the parent daemon, single-take, over which the
/// child mints daemon connections and delegates accepted descriptors.
public final class SpawnedChannel: @unchecked Sendable {
    /// One minted daemon connection: the received descriptor and the attach
    /// nonce its hello must echo.
    public struct MintedConnection: Sendable {
        public let descriptor: Int32
        public let nonce: Data
    }

    private static let claimGate = ClaimGate()

    private final class ClaimGate: @unchecked Sendable {
        private let lock = NSLock()
        private var taken = false

        func take() throws {
            try lock.withLock {
                guard !taken else { throw SpawnedChannelError.alreadyClaimed }
                taken = true
            }
        }

        func reset() {
            lock.withLock { taken = false }
        }
    }

    private let queue = DispatchQueue(label: "com.yasyf.daemonkit.SpawnedChannel")
    private let lock = NSLock()
    private let spawnNonce: Data
    private var descriptor: Int32

    init(descriptor: Int32, spawnNonce: Data) {
        self.descriptor = descriptor
        self.spawnNonce = spawnNonce
    }

    static func resetClaimForTesting() {
        claimGate.reset()
    }

    /// Claims the spawn channel a daemonkit parent placed at fd 3, single-take:
    /// the conveyance environment is read and unset, and the descriptor is
    /// proven to be a unix stream whose creator is this process's parent at
    /// the same UID. A child that cannot prove its channel exits, it does not
    /// negotiate.
    public static func claim() throws -> SpawnedChannel {
        try claimGate.take()
        let nonce = try readConveyance()
        let owned = fcntl(spawnedHandoffDescriptor, F_DUPFD_CLOEXEC, 0)
        guard owned >= 0 else {
            throw SpawnedChannelError.untrustedDescriptor("dup fd \(spawnedHandoffDescriptor): errno \(errno)")
        }
        do {
            try proveParentSocketpair(owned)
        } catch {
            Darwin.close(owned)
            throw error
        }
        Darwin.close(spawnedHandoffDescriptor)
        return SpawnedChannel(descriptor: owned, spawnNonce: nonce)
    }

    /// Mints one fresh daemon connection over the channel.
    public func mint(deadline: Date) async throws -> MintedConnection {
        try await queue.performIO { try self.lockedMint(deadline: deadline) }
    }

    /// Delegates a connected descriptor the child accepted — an App Group
    /// peer — into the daemon's full trust admission. The caller keeps
    /// ownership of the descriptor.
    public func delegate(descriptor: Int32, deadline: Date) async throws {
        try await queue.performIO { try self.lockedDelegate(descriptor: descriptor, deadline: deadline) }
    }

    /// Closes the channel; every later operation throws.
    public func close() {
        let owned = lock.withLock { () -> Int32 in
            let owned = descriptor
            descriptor = -1
            return owned
        }
        if owned >= 0 {
            Darwin.close(owned)
        }
    }

    deinit {
        close()
    }

    private func channelDescriptor() throws -> Int32 {
        let owned = lock.withLock { descriptor }
        guard owned >= 0 else { throw SpawnedChannelError.closed }
        return owned
    }

    private func lockedMint(deadline: Date) throws -> MintedConnection {
        let channel = try channelDescriptor()
        try sendFrame(
            on: channel,
            payload: encodeRequest(operation: "mint"),
            passing: nil,
            deadline: deadline
        )
        let response = try receiveFrame(on: channel, withRights: true, deadline: deadline)
        guard let received = response.descriptor else {
            throw SpawnedChannelError.protocolViolation("mint response carried no descriptor")
        }
        do {
            let object = try decodeObject(response.payload, exactKeys: ["nonce"])
            guard let encoded = object["nonce"] as? String,
                  let nonce = Data(base64Encoded: encoded),
                  nonce.count == spawnedNonceBytes
            else {
                throw SpawnedChannelError.protocolViolation("mint response nonce")
            }
            try proveParentSocketpair(received)
            return MintedConnection(descriptor: received, nonce: nonce)
        } catch {
            Darwin.close(received)
            throw error
        }
    }

    private func lockedDelegate(descriptor delegated: Int32, deadline: Date) throws {
        let channel = try channelDescriptor()
        try sendFrame(
            on: channel,
            payload: encodeRequest(operation: "adopt"),
            passing: delegated,
            deadline: deadline
        )
        let response = try receiveFrame(on: channel, withRights: false, deadline: deadline)
        let object = try decodeObject(response.payload, exactKeys: ["adopted", "reason"], required: ["adopted"])
        guard let adopted = object["adopted"] as? NSNumber else {
            throw SpawnedChannelError.protocolViolation("adopt response verdict")
        }
        guard adopted.boolValue else {
            throw SpawnedChannelError.delegationRefused(object["reason"] as? String ?? "")
        }
    }

    private func encodeRequest(operation: String) throws -> Data {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return try encoder.encode(SpawnChannelRequestEnvelope(
            operation: operation, nonce: spawnNonce.base64EncodedString()
        ))
    }

    private func decodeObject(
        _ payload: Data,
        exactKeys: Set<String>,
        required: Set<String>? = nil
    ) throws -> [String: Any] {
        guard let object = try? JSONSerialization.jsonObject(with: payload) as? [String: Any],
              Set(object.keys).isSubset(of: exactKeys),
              (required ?? exactKeys).isSubset(of: Set(object.keys))
        else {
            throw SpawnedChannelError.protocolViolation("response JSON")
        }
        return object
    }
}

private struct SpawnChannelRequestEnvelope: Encodable {
    let operation: String
    let nonce: String

    enum CodingKeys: String, CodingKey {
        case operation = "op"
        case nonce
    }
}

private func readConveyance() throws -> Data {
    let nonceValue = getenv(spawnedNonceEnvironment).map { String(cString: $0) }
    let limitsValue = getenv(spawnedLimitsEnvironment).map { String(cString: $0) }
    unsetenv(spawnedNonceEnvironment)
    unsetenv(spawnedLimitsEnvironment)
    guard let nonceValue, let limitsValue else {
        throw SpawnedChannelError.notSpawned
    }
    guard let nonce = decodeHex(nonceValue), nonce.count == spawnedNonceBytes else {
        throw SpawnedChannelError.invalidConveyance("nonce")
    }
    let limits = limitsValue.split(separator: ",", omittingEmptySubsequences: false)
    guard limits.count == 2,
          let maxFrame = Int(limits[0]), maxFrame >= 0,
          let concurrency = Int(limits[1]), concurrency >= 0
    else {
        throw SpawnedChannelError.invalidConveyance("limits")
    }
    return nonce
}

private func decodeHex(_ encoded: String) -> Data? {
    let characters = Array(encoded.utf8)
    guard characters.count.isMultiple(of: 2) else { return nil }
    var decoded = Data(capacity: characters.count / 2)
    var index = 0
    while index < characters.count {
        guard let high = hexNibble(characters[index]), let low = hexNibble(characters[index + 1]) else {
            return nil
        }
        decoded.append(high << 4 | low)
        index += 2
    }
    return decoded
}

private func hexNibble(_ character: UInt8) -> UInt8? {
    switch character {
    case UInt8(ascii: "0") ... UInt8(ascii: "9"):
        character - UInt8(ascii: "0")
    case UInt8(ascii: "a") ... UInt8(ascii: "f"):
        character - UInt8(ascii: "a") + 10
    case UInt8(ascii: "A") ... UInt8(ascii: "F"):
        character - UInt8(ascii: "A") + 10
    default:
        nil
    }
}

private func proveParentSocketpair(_ descriptor: Int32) throws {
    var socketType: Int32 = 0
    var length = socklen_t(MemoryLayout<Int32>.size)
    guard getsockopt(descriptor, SOL_SOCKET, SO_TYPE, &socketType, &length) == 0 else {
        throw SpawnedChannelError.untrustedDescriptor("SO_TYPE: errno \(errno)")
    }
    guard socketType == SOCK_STREAM else {
        throw SpawnedChannelError.untrustedDescriptor("socket type \(socketType)")
    }
    var address = sockaddr_storage()
    var addressLength = socklen_t(MemoryLayout<sockaddr_storage>.size)
    let named = withUnsafeMutablePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            getsockname(descriptor, $0, &addressLength)
        }
    }
    guard named == 0 else {
        throw SpawnedChannelError.untrustedDescriptor("getsockname: errno \(errno)")
    }
    guard address.ss_family == sa_family_t(AF_UNIX) else {
        throw SpawnedChannelError.untrustedDescriptor("socket family \(address.ss_family)")
    }
    var peerPID: pid_t = 0
    length = socklen_t(MemoryLayout<pid_t>.size)
    guard getsockopt(descriptor, SOL_LOCAL, LOCAL_PEERPID, &peerPID, &length) == 0 else {
        throw SpawnedChannelError.untrustedDescriptor("LOCAL_PEERPID: errno \(errno)")
    }
    var credentials = xucred()
    length = socklen_t(MemoryLayout<xucred>.size)
    guard getsockopt(descriptor, SOL_LOCAL, LOCAL_PEERCRED, &credentials, &length) == 0 else {
        throw SpawnedChannelError.untrustedDescriptor("LOCAL_PEERCRED: errno \(errno)")
    }
    guard credentials.cr_version == XUCRED_VERSION else {
        throw SpawnedChannelError.untrustedDescriptor("xucred version \(credentials.cr_version)")
    }
    guard peerPID == getppid(), credentials.cr_uid == getuid() else {
        throw SpawnedChannelError.untrustedDescriptor(
            "peer pid \(peerPID) uid \(credentials.cr_uid) is not the parent pid \(getppid()) uid \(getuid())"
        )
    }
}

private func sendFrame(on channel: Int32, payload: Data, passing delegated: Int32?, deadline: Date) throws {
    guard !payload.isEmpty, payload.count <= spawnChannelMaxPayload else {
        throw SpawnedChannelError.protocolViolation("payload length \(payload.count)")
    }
    var frame = Data(count: 4)
    frame.withUnsafeMutableBytes { bytes in
        bytes.storeBytes(of: UInt32(payload.count).bigEndian, as: UInt32.self)
    }
    frame.append(payload)
    let pollDeadline = channelPollDeadline(deadline)
    var sent = try sendLeadingSegment(on: channel, frame: frame, passing: delegated, deadline: pollDeadline)
    while sent < frame.count {
        let written = frame.withUnsafeBytes { bytes -> Int in
            Darwin.send(channel, bytes.baseAddress?.advanced(by: sent), frame.count - sent, MSG_NOSIGNAL)
        }
        if written < 0 {
            if errno == EINTR {
                continue
            }
            throw SpawnedChannelError.protocolViolation("send: errno \(errno)")
        }
        if written == 0 {
            throw SpawnedChannelError.closed
        }
        sent += written
    }
}

private func sendLeadingSegment(
    on channel: Int32,
    frame: Data,
    passing delegated: Int32?,
    deadline: UInt64?
) throws -> Int {
    let headerBytes = MemoryLayout<cmsghdr>.size
    let controlBytes = headerBytes + MemoryLayout<Int32>.size
    let control = UnsafeMutableRawPointer.allocate(
        byteCount: controlBytes,
        alignment: MemoryLayout<cmsghdr>.alignment
    )
    defer { control.deallocate() }
    if let delegated {
        control.initializeMemory(as: UInt8.self, repeating: 0, count: controlBytes)
        let header = control.assumingMemoryBound(to: cmsghdr.self)
        header.pointee.cmsg_len = UInt32(controlBytes)
        header.pointee.cmsg_level = SOL_SOCKET
        header.pointee.cmsg_type = SCM_RIGHTS
        control.advanced(by: headerBytes).storeBytes(of: delegated, as: Int32.self)
    }
    while true {
        let written = frame.withUnsafeBytes { bytes -> Int in
            var vector = iovec(
                iov_base: UnsafeMutableRawPointer(mutating: bytes.baseAddress),
                iov_len: bytes.count
            )
            return withUnsafeMutablePointer(to: &vector) { vectorPointer in
                var message = msghdr()
                message.msg_iov = vectorPointer
                message.msg_iovlen = 1
                if delegated != nil {
                    message.msg_control = control
                    message.msg_controllen = UInt32(controlBytes)
                }
                return Darwin.sendmsg(channel, &message, MSG_NOSIGNAL)
            }
        }
        if written > 0 {
            return written
        }
        if written == 0 {
            throw SpawnedChannelError.closed
        }
        if errno == EINTR {
            continue
        }
        if errno == EAGAIN || errno == EWOULDBLOCK {
            try waitForChannel(channel, events: Int16(POLLOUT), deadline: deadline)
            continue
        }
        throw SpawnedChannelError.protocolViolation("sendmsg: errno \(errno)")
    }
}

private func receiveFrame(
    on channel: Int32,
    withRights: Bool,
    deadline: Date
) throws -> (payload: Data, descriptor: Int32?) {
    let pollDeadline = channelPollDeadline(deadline)
    var received: Int32?
    do {
        var prefix = Data(count: 4)
        if withRights {
            try receiveExactly(on: channel, into: &prefix, rights: &received, deadline: pollDeadline)
        } else {
            try receiveExactly(on: channel, into: &prefix, rights: nil, deadline: pollDeadline)
        }
        let length = prefix.withUnsafeBytes { Int(UInt32(bigEndian: $0.load(as: UInt32.self))) }
        guard length > 0, length <= spawnChannelMaxPayload else {
            throw SpawnedChannelError.protocolViolation("payload length \(length)")
        }
        var payload = Data(count: length)
        var lateRights: Int32?
        try receiveExactly(on: channel, into: &payload, rights: &lateRights, deadline: pollDeadline)
        guard lateRights == nil else {
            if let lateRights {
                Darwin.close(lateRights)
            }
            throw SpawnedChannelError.protocolViolation("descriptor past frame byte zero")
        }
        return (payload, received)
    } catch {
        if let received {
            Darwin.close(received)
        }
        throw error
    }
}

private func receiveExactly(
    on channel: Int32,
    into buffer: inout Data,
    rights: UnsafeMutablePointer<Int32?>?,
    deadline: UInt64?
) throws {
    var read = 0
    let total = buffer.count
    while read < total {
        try waitForChannel(channel, events: Int16(POLLIN), deadline: deadline)
        let outcome = try buffer.withUnsafeMutableBytes { bytes -> Int in
            var vector = iovec(
                iov_base: bytes.baseAddress?.advanced(by: read),
                iov_len: total - read
            )
            return try withUnsafeMutablePointer(to: &vector) { vectorPointer in
                let controlBytes = MemoryLayout<cmsghdr>.size + MemoryLayout<Int32>.size
                let control = UnsafeMutableRawPointer.allocate(
                    byteCount: controlBytes,
                    alignment: MemoryLayout<cmsghdr>.alignment
                )
                defer { control.deallocate() }
                var message = msghdr()
                message.msg_iov = vectorPointer
                message.msg_iovlen = 1
                message.msg_control = control
                message.msg_controllen = UInt32(controlBytes)
                let count = Darwin.recvmsg(channel, &message, 0)
                if count < 0 {
                    return count
                }
                if message.msg_flags & (MSG_CTRUNC | MSG_TRUNC) != 0 {
                    throw SpawnedChannelError.protocolViolation("truncated ancillary data")
                }
                if message.msg_controllen >= UInt32(controlBytes) {
                    let header = control.assumingMemoryBound(to: cmsghdr.self)
                    guard header.pointee.cmsg_level == SOL_SOCKET,
                          header.pointee.cmsg_type == SCM_RIGHTS,
                          header.pointee.cmsg_len == UInt32(controlBytes)
                    else {
                        throw SpawnedChannelError.protocolViolation("unexpected ancillary data")
                    }
                    let passed = control.advanced(by: MemoryLayout<cmsghdr>.size).load(as: Int32.self)
                    guard fcntl(passed, F_SETFD, FD_CLOEXEC) == 0 else {
                        let code = errno
                        Darwin.close(passed)
                        throw SpawnedChannelError.protocolViolation("received descriptor: errno \(code)")
                    }
                    guard let rights, rights.pointee == nil, count > 0, read == 0 else {
                        Darwin.close(passed)
                        throw SpawnedChannelError.protocolViolation("descriptor outside frame byte zero")
                    }
                    rights.pointee = passed
                } else if message.msg_controllen != 0 {
                    throw SpawnedChannelError.protocolViolation("short ancillary data")
                }
                return count
            }
        }
        if outcome < 0 {
            if errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK {
                continue
            }
            throw SpawnedChannelError.protocolViolation("recvmsg: errno \(errno)")
        }
        if outcome == 0 {
            throw SpawnedChannelError.closed
        }
        read += outcome
    }
}

private func waitForChannel(_ channel: Int32, events: Int16, deadline: UInt64?) throws {
    while true {
        var readiness = pollfd(fd: channel, events: events, revents: 0)
        let ready = poll(&readiness, 1, SessionFrameCodec.pollTimeout(deadline: deadline, maximum: 100))
        if ready > 0 {
            return
        }
        if ready < 0, errno != EINTR, errno != EAGAIN {
            throw SpawnedChannelError.protocolViolation("poll: errno \(errno)")
        }
        if let deadline, DispatchTime.now().uptimeNanoseconds >= deadline {
            throw SpawnedChannelError.deadlineExceeded
        }
    }
}

private func channelPollDeadline(_ deadline: Date) -> UInt64? {
    guard deadline.timeIntervalSinceNow > 0 else {
        return DispatchTime.now().uptimeNanoseconds
    }
    return SessionFrameCodec.deadline(after: deadline.timeIntervalSinceNow)
}
