@testable import DaemonKit
import Darwin
import Foundation
import Testing

@Suite(.serialized, .timeLimit(.minutes(1)))
struct SpawnedChannelTests {
    private static let nonceEnvironment = "DAEMONKIT_SPAWNED_NONCE"
    private static let limitsEnvironment = "DAEMONKIT_SPAWNED_LIMITS"

    private func withConveyance(
        nonce: String?,
        limits: String?,
        _ body: () throws -> Void
    ) rethrows {
        SpawnedChannel.resetClaimForTesting()
        if let nonce {
            setenv(Self.nonceEnvironment, nonce, 1)
        } else {
            unsetenv(Self.nonceEnvironment)
        }
        if let limits {
            setenv(Self.limitsEnvironment, limits, 1)
        } else {
            unsetenv(Self.limitsEnvironment)
        }
        defer {
            unsetenv(Self.nonceEnvironment)
            unsetenv(Self.limitsEnvironment)
        }
        try body()
    }

    private func stagedPair() throws -> (local: Int32, remote: Int32) {
        var descriptors: [Int32] = [0, 0]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        return (descriptors[0], descriptors[1])
    }

    @Test
    func claimFailsClosedWithoutTheSpawnConveyance() throws {
        try withConveyance(nonce: nil, limits: nil) {
            #expect(throws: SpawnedChannelError.notSpawned) {
                _ = try SpawnedChannel.claim()
            }
        }
    }

    @Test
    func claimRejectsAMalformedNonce() throws {
        try withConveyance(nonce: "zz", limits: "0,0") {
            #expect(throws: SpawnedChannelError.invalidConveyance("nonce")) {
                _ = try SpawnedChannel.claim()
            }
        }
    }

    @Test
    func claimRejectsMalformedLimits() throws {
        let nonce = String(repeating: "ab", count: 32)
        try withConveyance(nonce: nonce, limits: "4,") {
            #expect(throws: SpawnedChannelError.invalidConveyance("limits")) {
                _ = try SpawnedChannel.claim()
            }
        }
    }

    @Test
    func claimConsumesTheConveyanceEvenWhenItRefuses() throws {
        try withConveyance(nonce: "zz", limits: "0,0") {
            #expect(throws: SpawnedChannelError.invalidConveyance("nonce")) {
                _ = try SpawnedChannel.claim()
            }
            #expect(getenv(Self.nonceEnvironment) == nil)
            #expect(getenv(Self.limitsEnvironment) == nil)
        }
    }

    @Test
    func claimRefusesADescriptorWhoseCreatorIsNotTheParent() throws {
        let nonce = String(repeating: "ab", count: 32)
        try withConveyance(nonce: nonce, limits: "0,0") {
            let pair = try stagedPair()
            defer {
                Darwin.close(pair.local)
                Darwin.close(pair.remote)
            }
            let saved = fcntl(3, F_DUPFD_CLOEXEC, 10)
            try #require(dup2(pair.local, 3) == 3)
            defer {
                if saved >= 0 {
                    _ = dup2(saved, 3)
                    Darwin.close(saved)
                } else {
                    Darwin.close(3)
                }
            }
            #expect {
                _ = try SpawnedChannel.claim()
            } throws: { error in
                guard case .untrustedDescriptor = error as? SpawnedChannelError else { return false }
                return true
            }
        }
    }

    @Test
    func aSecondClaimIsRefused() throws {
        try withConveyance(nonce: nil, limits: nil) {
            #expect(throws: SpawnedChannelError.notSpawned) {
                _ = try SpawnedChannel.claim()
            }
            #expect(throws: SpawnedChannelError.alreadyClaimed) {
                _ = try SpawnedChannel.claim()
            }
        }
    }

    @Test
    func delegateSendsTheDescriptorAndMapsTheVerdict() async throws {
        let pair = try stagedPair()
        let channel = SpawnedChannel(descriptor: pair.local, spawnNonce: Data(count: 32))
        defer {
            channel.close()
            Darwin.close(pair.remote)
        }
        let delegated = try stagedPair()
        defer {
            Darwin.close(delegated.local)
            Darwin.close(delegated.remote)
        }
        let parent = Task.detached {
            try SpawnChannelTestParent.serveOne(
                on: pair.remote,
                expectDescriptor: true,
                respond: #"{"adopted":true}"#
            )
        }
        try await channel.delegate(
            descriptor: delegated.local,
            deadline: Date().addingTimeInterval(5)
        )
        let request = try await parent.value
        #expect(request.object["op"] as? String == "adopt")
        #expect(request.object["nonce"] as? String == Data(count: 32).base64EncodedString())
        #expect(request.descriptor != nil)
        if let received = request.descriptor {
            Darwin.close(received)
        }
    }

    @Test
    func delegateThrowsTheNamedRefusal() async throws {
        let pair = try stagedPair()
        let channel = SpawnedChannel(descriptor: pair.local, spawnNonce: Data(count: 32))
        defer {
            channel.close()
            Darwin.close(pair.remote)
        }
        let delegated = try stagedPair()
        defer {
            Darwin.close(delegated.local)
            Darwin.close(delegated.remote)
        }
        let parent = Task.detached {
            try SpawnChannelTestParent.serveOne(
                on: pair.remote,
                expectDescriptor: true,
                respond: #"{"adopted":false,"reason":"daemon at capacity"}"#
            )
        }
        await #expect(throws: SpawnedChannelError.delegationRefused("daemon at capacity")) {
            try await channel.delegate(
                descriptor: delegated.local,
                deadline: Date().addingTimeInterval(5)
            )
        }
        let request = try await parent.value
        if let received = request.descriptor {
            Darwin.close(received)
        }
    }

    @Test
    func mintRefusesADescriptorWhoseCreatorIsNotTheParent() async throws {
        let pair = try stagedPair()
        let channel = SpawnedChannel(descriptor: pair.local, spawnNonce: Data(count: 32))
        defer {
            channel.close()
            Darwin.close(pair.remote)
        }
        let minted = try stagedPair()
        defer { Darwin.close(minted.local) }
        let parent = Task.detached {
            try SpawnChannelTestParent.serveOne(
                on: pair.remote,
                expectDescriptor: false,
                respond: #"{"nonce":"\#(Data(count: 32).base64EncodedString())"}"#,
                passing: minted.remote
            )
        }
        defer { Darwin.close(minted.remote) }
        await #expect {
            _ = try await channel.mint(deadline: Date().addingTimeInterval(5))
        } throws: { error in
            guard case .untrustedDescriptor = error as? SpawnedChannelError else { return false }
            return true
        }
        _ = try await parent.value
    }

    @Test
    func aSilentParentIsADeadlineNotAHang() async throws {
        let pair = try stagedPair()
        let channel = SpawnedChannel(descriptor: pair.local, spawnNonce: Data(count: 32))
        defer {
            channel.close()
            Darwin.close(pair.remote)
        }
        await #expect(throws: SpawnedChannelError.deadlineExceeded) {
            _ = try await channel.mint(deadline: Date().addingTimeInterval(0.2))
        }
    }

    @Test
    func operationsAfterCloseThrowClosed() async throws {
        let pair = try stagedPair()
        let channel = SpawnedChannel(descriptor: pair.local, spawnNonce: Data(count: 32))
        Darwin.close(pair.remote)
        channel.close()
        await #expect(throws: SpawnedChannelError.closed) {
            _ = try await channel.mint(deadline: Date().addingTimeInterval(1))
        }
    }
}

enum SpawnChannelTestParent {
    struct Request: @unchecked Sendable {
        let object: [String: Any]
        let descriptor: Int32?
    }

    static func serveOne(
        on channel: Int32,
        expectDescriptor: Bool,
        respond payload: String,
        passing descriptor: Int32? = nil
    ) throws -> Request {
        let request = try readFrame(on: channel, expectDescriptor: expectDescriptor)
        try writeFrame(on: channel, payload: Data(payload.utf8), passing: descriptor)
        return request
    }

    private static func readFrame(on channel: Int32, expectDescriptor: Bool) throws -> Request {
        var received: Int32?
        var prefix = Data(count: 4)
        try receive(on: channel, into: &prefix, rights: &received)
        let length = prefix.withUnsafeBytes { Int(UInt32(bigEndian: $0.load(as: UInt32.self))) }
        try #require(length > 0 && length <= 1024)
        var payload = Data(count: length)
        var late: Int32?
        try receive(on: channel, into: &payload, rights: &late)
        try #require(late == nil)
        try #require((received != nil) == expectDescriptor)
        let object = try #require(JSONSerialization.jsonObject(with: payload) as? [String: Any])
        return Request(object: object, descriptor: received)
    }

    private static func receive(on channel: Int32, into buffer: inout Data, rights: inout Int32?) throws {
        var read = 0
        let total = buffer.count
        while read < total {
            let outcome = buffer.withUnsafeMutableBytes { bytes -> Int in
                var vector = iovec(
                    iov_base: bytes.baseAddress?.advanced(by: read),
                    iov_len: total - read
                )
                return withUnsafeMutablePointer(to: &vector) { vectorPointer in
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
                    if count > 0, message.msg_controllen >= UInt32(controlBytes) {
                        let header = control.assumingMemoryBound(to: cmsghdr.self)
                        if header.pointee.cmsg_level == SOL_SOCKET, header.pointee.cmsg_type == SCM_RIGHTS {
                            rights = control.advanced(by: MemoryLayout<cmsghdr>.size).load(as: Int32.self)
                        }
                    }
                    return count
                }
            }
            try #require(outcome > 0)
            read += outcome
        }
    }

    private static func writeFrame(on channel: Int32, payload: Data, passing descriptor: Int32?) throws {
        var frame = Data(count: 4)
        frame.withUnsafeMutableBytes { bytes in
            bytes.storeBytes(of: UInt32(payload.count).bigEndian, as: UInt32.self)
        }
        frame.append(payload)
        let headerBytes = MemoryLayout<cmsghdr>.size
        let controlBytes = headerBytes + MemoryLayout<Int32>.size
        let control = UnsafeMutableRawPointer.allocate(
            byteCount: controlBytes,
            alignment: MemoryLayout<cmsghdr>.alignment
        )
        defer { control.deallocate() }
        if let descriptor {
            control.initializeMemory(as: UInt8.self, repeating: 0, count: controlBytes)
            let header = control.assumingMemoryBound(to: cmsghdr.self)
            header.pointee.cmsg_len = UInt32(controlBytes)
            header.pointee.cmsg_level = SOL_SOCKET
            header.pointee.cmsg_type = SCM_RIGHTS
            control.advanced(by: headerBytes).storeBytes(of: descriptor, as: Int32.self)
        }
        let written = frame.withUnsafeBytes { bytes -> Int in
            var vector = iovec(
                iov_base: UnsafeMutableRawPointer(mutating: bytes.baseAddress),
                iov_len: bytes.count
            )
            return withUnsafeMutablePointer(to: &vector) { vectorPointer in
                var message = msghdr()
                message.msg_iov = vectorPointer
                message.msg_iovlen = 1
                if descriptor != nil {
                    message.msg_control = control
                    message.msg_controllen = UInt32(controlBytes)
                }
                return Darwin.sendmsg(channel, &message, MSG_NOSIGNAL)
            }
        }
        try #require(written == frame.count)
    }
}
