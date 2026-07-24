@testable import DaemonKit
import Darwin
import Foundation
import Testing

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct BrokerHandoffTests {
        @Test func lifecycleKindRemainsExactElevenAndTwelveIsForbidden() {
            #expect(SessionFrameKind.lifecycle.rawValue == 11)
            #expect(SessionFrameKind(rawValue: 12) == nil)
            #expect(SocketResponseCode.handoffPendingCapacity.rawValue == "handoff_pending_capacity")
            #expect(SocketResponseCode.handoffReplay.rawValue == "handoff_replay")
            #expect(SocketResponseCode.handoffSessionExhausted.rawValue == "handoff_session_exhausted")
        }

        @Test func onlyPendingCapacityKeepsTheExactCarrierSession() {
            #expect(ServiceSocketClient.keepsHandoffSession(after: BrokerHandoffError.responseRejected(
                .handoffPendingCapacity,
                nil
            )))
            for code in [
                SocketResponseCode.handoffReplay,
                .handoffSessionExhausted,
                .permissionDenied,
            ] {
                #expect(!ServiceSocketClient.keepsHandoffSession(after: BrokerHandoffError.responseRejected(
                    code,
                    nil
                )))
            }
            #expect(!ServiceSocketClient.keepsHandoffSession(after: BrokerHandoffError.deliveryUnknown))
            #expect(!ServiceSocketClient.keepsHandoffSession(after: BrokerHandoffError.invalidPayload))
            #expect(!ServiceSocketClient.keepsHandoffSession(after: BrokerHandoffError.responseMismatch))
        }

        @Test func canonicalPayloadIsStrictAndNonceIsPaddedBase64() throws {
            let nonce = Data(0 ..< 32)
            let identity = try RuntimeIdentity(
                runtimeBuild: "daemonkit-v0.11.0",
                processGeneration: OwnerGeneration("0123456789abcdef0123456789abcdef")
            )
            let payload = try BrokerHandoffCodec.encode(nonce: nonce, identity: identity)
            let golden = #"{"nonce":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=","protocol":1,"# +
                #""runtime_identity":{"process_generation":"0123456789abcdef0123456789abcdef","runtime_build":"daemonkit-v0.11.0"}}"#
            #expect(payload == Data(golden.utf8))
            let decoded = try BrokerHandoffCodec.decode(payload)
            #expect(decoded.nonce == nonce)
            #expect(decoded.identity == identity)
            let acknowledgment = try BrokerHandoffCodec.encode(
                nonce: decoded.nonce,
                identity: decoded.identity
            )
            #expect(acknowledgment == payload)

            let canonicalIdentity =
                #""runtime_identity":{"process_generation":"00000000000000000000000000000001","runtime_build":"app.v1"}"#
            let malformed = [
                Data((#"{"nonce":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=","protocol":1,"protocol":1,"# + canonicalIdentity + "}").utf8),
                Data((#" {"nonce":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=","protocol":1,"# + canonicalIdentity + "}").utf8),
                Data((#"{"nonce":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8","protocol":1,"# + canonicalIdentity + "}").utf8),
                Data((#"{"extra":true,"nonce":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=","protocol":1,"# + canonicalIdentity + "}").utf8),
            ]
            for candidate in malformed {
                #expect(throws: BrokerHandoffError.invalidPayload) {
                    _ = try BrokerHandoffCodec.decode(candidate)
                }
            }
        }

        @Test func writerAttachesOneDescriptorToFirstFragmentAndClosesSenderCopy() async throws {
            var transport: [Int32] = [-1, -1]
            var peer: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &transport) == 0)
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &peer) == 0)
            defer {
                transport.forEach { Darwin.close($0) }
                peer.forEach { Darwin.close($0) }
            }
            let transferred = peer[0]
            peer[0] = -1
            let transferredIdentity = try #require(descriptorIdentity(transferred))
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: transport[0], writeTimeout: 1),
                maximumPendingWrites: 2,
                label: "broker-handoff-test"
            )
            let frame = SessionFrame(
                kind: .request,
                flags: .end,
                id: 1,
                deadlineUnixMilliseconds: 1,
                operation: brokerHandoffOperation,
                payload: Data(#"{"test":true}"#.utf8)
            )
            let sending = Task {
                try await writer.writePassingDescriptor(
                    frame,
                    descriptor: transferred,
                    deadline: Date().addingTimeInterval(1)
                )
            }

            let received = try receiveDescriptorAndFirstPrefixByte(transport[1])
            var prefix = Data([received.firstByte])
            try prefix.append(readExactly(transport[1], count: 3))
            let bodyLength = prefix.reduce(UInt32(0)) { ($0 << 8) | UInt32($1) }
            let body = try readExactly(transport[1], count: Int(bodyLength))
            try await sending.value
            #expect(try SessionFrameCodec.decode(body).operation == brokerHandoffOperation)
            #expect(fcntl(received.descriptor, F_GETFD) >= 0)
            Darwin.close(received.descriptor)
            #expect(descriptorIdentity(transferred) != transferredIdentity)

            writer.abort()
            await writer.drain()
        }

        @Test func writerClosesUnsentDescriptorWhenQueueIsClosed() async throws {
            var transport: [Int32] = [-1, -1]
            var peer: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &transport) == 0)
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &peer) == 0)
            defer {
                transport.forEach { Darwin.close($0) }
                peer.forEach { Darwin.close($0) }
            }
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: transport[0], writeTimeout: 1),
                maximumPendingWrites: 1,
                label: "broker-handoff-reject-test"
            )
            let transferred = peer[0]
            peer[0] = -1
            let transferredIdentity = try #require(descriptorIdentity(transferred))
            writer.abort()
            await #expect(throws: SessionTransportError.disconnected) {
                try await writer.writePassingDescriptor(
                    SessionFrame(kind: .request, flags: .end, id: 1, operation: brokerHandoffOperation),
                    descriptor: transferred,
                    deadline: Date().addingTimeInterval(1)
                )
            }
            #expect(descriptorIdentity(transferred) != transferredIdentity)
        }

        @Test func expiredHandoffDeadlineClosesDescriptorWithoutSendingRights() async throws {
            var transport: [Int32] = [-1, -1]
            var peer: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &transport) == 0)
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &peer) == 0)
            defer {
                transport.forEach { Darwin.close($0) }
                peer.forEach { Darwin.close($0) }
            }
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: transport[0], writeTimeout: 1),
                maximumPendingWrites: 1,
                label: "broker-handoff-expired-test"
            )
            let transferred = peer[0]
            peer[0] = -1
            let transferredIdentity = try #require(descriptorIdentity(transferred))
            await #expect(throws: SessionTransportError.self) {
                try await writer.writePassingDescriptor(
                    SessionFrame(kind: .request, flags: .end, id: 1, operation: brokerHandoffOperation),
                    descriptor: transferred,
                    deadline: Date(timeIntervalSinceNow: -1)
                )
            }
            #expect(descriptorIdentity(transferred) != transferredIdentity)
            let flags = fcntl(transport[1], F_GETFL)
            try #require(flags >= 0)
            try #require(fcntl(transport[1], F_SETFL, flags | O_NONBLOCK) == 0)
            var byte: UInt8 = 0
            #expect(Darwin.read(transport[1], &byte, 1) == -1)
            #expect(errno == EAGAIN || errno == EWOULDBLOCK)
            writer.abort()
            await writer.drain()
        }
    }
}

extension SocketTransportTests.BrokerHandoffTests {
    @Test func abortClosesDescriptorQueuedBehindBlockedWrite() async throws {
        var transport: [Int32] = [-1, -1]
        var peer: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &transport) == 0)
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &peer) == 0)
        defer {
            transport.forEach { Darwin.close($0) }
            peer.forEach { Darwin.close($0) }
        }
        var sendBuffer: Int32 = 4096
        setsockopt(
            transport[0],
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBuffer,
            socklen_t(MemoryLayout<Int32>.size)
        )
        let transportFlags = fcntl(transport[0], F_GETFL)
        try #require(transportFlags >= 0)
        try #require(fcntl(transport[0], F_SETFL, transportFlags | O_NONBLOCK) == 0)
        let firstStarted = AsyncLatch()
        let handoffAdmitted = AsyncLatch()
        let writer = SessionWriter(
            codec: SessionFrameCodec(descriptor: transport[0], writeTimeout: 0.05),
            maximumPendingWrites: 1,
            label: "broker-handoff-abort-test",
            admissionHook: { frame in
                if frame.operation == brokerHandoffOperation {
                    handoffAdmitted.finish()
                }
            },
            startHook: { frame in
                if frame.operation.isEmpty {
                    firstStarted.finish()
                }
            }
        )
        let blocking = Task {
            try await writer.write(SessionFrame(
                kind: .stream,
                id: 1,
                payload: Data(repeating: 0xA5, count: 1024 * 1024)
            ))
        }
        await firstStarted.wait()
        let transferred = peer[0]
        peer[0] = -1
        let transferredIdentity = try #require(descriptorIdentity(transferred))
        let handoff = Task {
            try await writer.writePassingDescriptor(
                SessionFrame(kind: .request, flags: .end, id: 2, operation: brokerHandoffOperation),
                descriptor: transferred,
                deadline: Date().addingTimeInterval(1)
            )
        }
        await handoffAdmitted.wait()
        writer.abort()
        await #expect(throws: SessionTransportError.disconnected) { try await handoff.value }
        #expect(descriptorIdentity(transferred) != transferredIdentity)
        await #expect(throws: SessionTransportError.self) { try await blocking.value }
        await writer.drain()
    }

    @Test func failureAfterAncillaryTransferIsDeliveryUnknownAndNeverResendsRights() async throws {
        var transport: [Int32] = [-1, -1]
        var peer: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &transport) == 0)
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &peer) == 0)
        defer {
            transport.forEach { Darwin.close($0) }
            peer.forEach { Darwin.close($0) }
        }
        var sendBuffer: Int32 = 4096
        setsockopt(
            transport[0],
            SOL_SOCKET,
            SO_SNDBUF,
            &sendBuffer,
            socklen_t(MemoryLayout<Int32>.size)
        )
        let writer = SessionWriter(
            codec: SessionFrameCodec(descriptor: transport[0], writeTimeout: 1),
            maximumPendingWrites: 1,
            label: "broker-handoff-unknown-test"
        )
        let transferred = peer[0]
        peer[0] = -1
        let transferredIdentity = try #require(descriptorIdentity(transferred))
        let sending = Task {
            try await writer.writePassingDescriptor(
                SessionFrame(
                    kind: .request,
                    flags: .end,
                    id: 1,
                    operation: brokerHandoffOperation,
                    payload: Data(repeating: 0xA5, count: 1024 * 1024)
                ),
                descriptor: transferred,
                deadline: Date().addingTimeInterval(1)
            )
        }
        let received = try receiveDescriptorAndFirstPrefixByte(transport[1])
        Darwin.close(received.descriptor)
        Darwin.close(transport[1])
        transport[1] = -1
        await #expect(throws: BrokerHandoffError.deliveryUnknown) {
            try await sending.value
        }
        #expect(descriptorIdentity(transferred) != transferredIdentity)
        writer.abort()
        await writer.drain()
    }

    @Test func bridgeCancellationClosesListenerAndRemovesSocketPath() async throws {
        let directory = try makeShortDirectory()
        defer { try? FileManager.default.removeItem(atPath: directory) }
        let path = directory + "/broker.sock"
        let bridge = try makeBridge(path: path)
        let running = Task { try await bridge.run() }
        try await waitForPath(path)
        running.cancel()
        try await running.value
        #expect(access(path, F_OK) != 0)
    }

    @Test func bridgeReclaimsStaleSocketAndDuplicateOwnerCannotProbeOrUnlink() async throws {
        let directory = try makeShortDirectory()
        defer { try? FileManager.default.removeItem(atPath: directory) }
        let path = directory + "/broker.sock"
        var address = try makeUnixAddress(path)
        let stale = socket(AF_UNIX, SOCK_STREAM, 0)
        try #require(stale >= 0)
        try #require(withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(stale, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        } == 0)
        Darwin.close(stale)

        let bridge = try makeBridge(path: path)
        let running = Task { try await bridge.run() }
        try await Task.sleep(for: .milliseconds(20))
        running.cancel()
        try await running.value
        #expect(access(path, F_OK) != 0)

        let owner = try makeBridge(path: path)
        let owning = Task { try await owner.run() }
        try await waitForPath(path)
        let before = try socketIdentity(path)
        let duplicate = try makeBridge(path: path)
        await #expect(throws: SocketServerError.self) { try await duplicate.run() }
        #expect(try socketIdentity(path) == before)
        owning.cancel()
        try await owning.value
        #expect(access(path, F_OK) != 0)
    }

    @Test func shutdownBeforeRunCannotDeleteAnotherBridgeSocket() async throws {
        let directory = try makeShortDirectory()
        defer { try? FileManager.default.removeItem(atPath: directory) }
        let path = directory + "/broker.sock"
        let owner = try makeBridge(path: path)
        let owning = Task { try await owner.run() }
        try await waitForPath(path)
        let before = try socketIdentity(path)
        let neverStarted = try makeBridge(path: path)
        await neverStarted.shutdown()
        #expect(try socketIdentity(path) == before)
        owning.cancel()
        try await owning.value
    }

    @Test func bridgeCleanupPreservesReplacementPath() async throws {
        let directory = try makeShortDirectory()
        defer { try? FileManager.default.removeItem(atPath: directory) }
        let path = directory + "/broker.sock"
        let bridge = try makeBridge(path: path)
        let running = Task { try await bridge.run() }
        try await waitForPath(path)
        try #require(unlink(path) == 0)
        try #require(FileManager.default.createFile(atPath: path, contents: Data("replacement".utf8)))
        running.cancel()
        try await running.value
        #expect(try Data(contentsOf: URL(fileURLWithPath: path)) == Data("replacement".utf8))
    }

    private func receiveDescriptorAndFirstPrefixByte(
        _ socket: Int32
    ) throws -> (descriptor: Int32, firstByte: UInt8) {
        var byte: UInt8 = 0
        var control = [UInt8](repeating: 0, count: 64)
        let received = withUnsafeMutableBytes(of: &byte) { byteBytes in
            var vector = iovec(iov_base: byteBytes.baseAddress, iov_len: 1)
            return control.withUnsafeMutableBytes { controlBytes in
                withUnsafeMutablePointer(to: &vector) { vectorPointer in
                    var message = msghdr()
                    message.msg_iov = vectorPointer
                    message.msg_iovlen = 1
                    message.msg_control = controlBytes.baseAddress
                    message.msg_controllen = UInt32(controlBytes.count)
                    return recvmsg(socket, &message, 0)
                }
            }
        }
        guard received == 1 else {
            throw SessionTransportError.systemCall(operation: "recvmsg", errno: errno)
        }
        let header = control.withUnsafeBytes {
            $0.baseAddress!.assumingMemoryBound(to: cmsghdr.self).pointee
        }
        guard header.cmsg_level == SOL_SOCKET,
              header.cmsg_type == SCM_RIGHTS,
              header.cmsg_len == UInt32(MemoryLayout<cmsghdr>.size + MemoryLayout<Int32>.size)
        else { throw SessionTransportError.invalidFrame("handoff ancillary") }
        let descriptor = control.withUnsafeBytes {
            $0.baseAddress!.advanced(by: MemoryLayout<cmsghdr>.size).load(as: Int32.self)
        }
        return (descriptor, byte)
    }

    private func makeBridge(path: String) throws -> BrokerSocketBridge {
        try BrokerSocketBridge(
            path: path,
            daemon: RuntimeClientConfiguration(
                path: path + ".daemon",
                wireBuild: "suite.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            ),
            expectedRuntimeBuild: "app.v1"
        )
    }

    private func makeShortDirectory() throws -> String {
        let path = "/tmp/dk-broker-\(UUID().uuidString.prefix(8))"
        try FileManager.default.createDirectory(atPath: path, withIntermediateDirectories: false)
        return path
    }

    private func waitForPath(_ path: String) async throws {
        for _ in 0 ..< 100 where access(path, F_OK) != 0 {
            try await Task.sleep(for: .milliseconds(1))
        }
        try #require(access(path, F_OK) == 0)
    }

    private func makeUnixAddress(_ path: String) throws -> sockaddr_un {
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let bytes = Array(path.utf8)
        try #require(bytes.count < MemoryLayout.size(ofValue: address.sun_path))
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            bytes.withUnsafeBytes { destination.copyMemory(from: $0) }
        }
        return address
    }

    private func socketIdentity(_ path: String) throws -> [UInt64] {
        var status = stat()
        try #require(lstat(path, &status) == 0)
        try #require(status.st_mode & S_IFMT == S_IFSOCK)
        return [UInt64(status.st_dev), UInt64(status.st_ino)]
    }

    private func descriptorIdentity(_ descriptor: Int32) -> [Int64]? {
        var status = stat()
        guard fstat(descriptor, &status) == 0 else { return nil }
        return [Int64(status.st_dev), Int64(status.st_ino)]
    }

    private func readExactly(_ descriptor: Int32, count: Int) throws -> Data {
        var data = Data(count: count)
        var offset = 0
        while offset < count {
            let received = data.withUnsafeMutableBytes {
                Darwin.read(descriptor, $0.baseAddress!.advanced(by: offset), count - offset)
            }
            guard received > 0 else {
                throw SessionTransportError.systemCall(operation: "read", errno: errno)
            }
            offset += received
        }
        return data
    }
}
