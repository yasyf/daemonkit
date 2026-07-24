@testable import DaemonKit
import Darwin
import Foundation
import Testing

extension SocketTransportTests.ServiceSocketClientTests {
    @Test func missingEndpointHonorsDeadlineAndCancellation() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("missing.sock").path
        let deadlineClient = try serviceTestClient(
            path: path,
            wireBuild: "service.v1",
            role: SessionPeerRole.unprotected,
            noProgressTimeout: 1
        )
        await #expect(throws: ServiceSocketClientError.deadlineExceeded) {
            try await deadlineClient.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(0.05)
            ))
        }

        let canceledClient = try serviceTestClient(
            path: path,
            wireBuild: "service.v1",
            role: SessionPeerRole.unprotected,
            noProgressTimeout: 1
        )
        let task = Task {
            try await canceledClient.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(10)
            ))
        }
        task.cancel()
        await #expect(throws: CancellationError.self) {
            try await task.value
        }
    }

    @Test func expiredDeadlineWinsBeforeOperationAndIdentityValidation() async throws {
        let client = try serviceTestClient(
            path: "/definitely/missing/daemonkit.sock",
            wireBuild: "service.v1",
            role: SessionPeerRole.unprotected,
            noProgressTimeout: 1
        )
        await #expect(throws: ServiceSocketClientError.deadlineExceeded) {
            try await client.call(ServiceSocketCall(
                operation: "",
                runtimeTarget: .exact(RuntimeIdentity(runtimeBuild: "", processGeneration: testOwnerGeneration())),
                deadline: Date().addingTimeInterval(-1)
            ))
        }
    }

    @Test func peerBuildMismatchFailsBeforeDispatch() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = serviceTestServer(path: path, wireBuild: "other.v1") { _ in
                Issue.record("mismatched build dispatched")
                return .terminal(SocketTerminal())
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            )
            cleanup.add { await client.close() }

            await #expect(throws: SocketWireBuildMismatchError(
                server: "other.v1",
                client: "service.v1"
            )) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
        }
    }

    @Test func sessionCapacityRetriesWithinTheSameNoProgressBudget() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let lifecycle = try testRuntimeController()
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                configuration: .init(maximumSessions: 1),
                runtimeLifecycle: lifecycle
            ) { _ in
                .terminal(SocketTerminal(payload: Data(#""done""#.utf8)))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let holder = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await holder.close() }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            )
            cleanup.add { await client.close() }
            let retryObserved = AsyncLatch()
            await client.setRetrySleepHook { retryObserved.finish() }

            let call = Task {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            await retryObserved.wait()
            holder.abort()
            try await Task.sleep(for: .milliseconds(10))

            let terminal = try await call.value
            #expect(terminal.payload == Data(#""done""#.utf8))
            #expect(await client.startedGenerations > 1)
        }
    }

    @Test func acknowledgmentBuildWinsOverCapacityRejection() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                configuration: .init(maximumSessions: 1)
            ) { _ in
                .terminal(SocketTerminal())
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let holder = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await holder.close() }

            await #expect(throws: SocketWireBuildMismatchError(
                server: "service.v1",
                client: "other.v1"
            )) {
                _ = try await SocketClient(
                    path: path,
                    wireBuild: "other.v1",
                    role: SessionPeerRole.unprotected
                )
            }
        }
    }

    @Test func bufferedAcknowledgmentWinsPeerCloseDuringHelloWrite() throws {
        var descriptors: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        defer {
            Darwin.close(descriptors[0])
            if descriptors[1] >= 0 {
                Darwin.close(descriptors[1])
            }
        }
        let server = SessionFrameCodec(descriptor: descriptors[1])
        try server.write(SessionFrame(
            kind: .helloAck,
            flags: .end,
            payload: SessionHandshakeCodec.encodeRejection(
                wireBuild: "service.v1",
                code: .sessionCapacity,
                reason: "wire: session capacity exhausted"
            )
        ))
        Darwin.close(descriptors[1])
        descriptors[1] = -1
        var probe: UInt8 = 0
        let sent = withUnsafeBytes(of: &probe) {
            Darwin.send(descriptors[0], $0.baseAddress, $0.count, MSG_NOSIGNAL)
        }
        try #require(sent == -1)
        try #require(errno == EPIPE)

        let client = SessionFrameCodec(descriptor: descriptors[0])
        #expect(throws: SocketWireBuildMismatchError(server: "service.v1", client: "other.v1")) {
            _ = try SocketClientCore.handshake(
                codec: client,
                wireBuild: "other.v1",
                role: SessionPeerRole.unprotected,
                timeout: 0.1
            )
        }
    }

    @Test func invalidBufferedAcknowledgmentDoesNotHideHelloWriteFailure() throws {
        var descriptors: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        defer {
            Darwin.close(descriptors[0])
            if descriptors[1] >= 0 {
                Darwin.close(descriptors[1])
            }
        }
        let server = SessionFrameCodec(descriptor: descriptors[1])
        try server.write(SessionFrame(
            kind: .helloAck,
            flags: .end,
            payload: Data(#"{"protocol":1}"#.utf8)
        ))
        Darwin.close(descriptors[1])
        descriptors[1] = -1
        var probe: UInt8 = 0
        let sent = withUnsafeBytes(of: &probe) {
            Darwin.send(descriptors[0], $0.baseAddress, $0.count, MSG_NOSIGNAL)
        }
        try #require(sent == -1)
        try #require(errno == EPIPE)

        #expect(throws: SessionTransportError.systemCall(operation: "send", errno: EPIPE)) {
            _ = try SocketClientCore.handshake(
                codec: SessionFrameCodec(descriptor: descriptors[0]),
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                timeout: 0.1
            )
        }
    }

    @Test func silentHandshakePeerHonorsPublicClientDeadline() async throws {
        let directory = try shortSocketDir()
        let path = directory.appendingPathComponent("silent.sock").path
        var address = try #require(makeAddress(path: path))
        let listener = socket(AF_UNIX, SOCK_STREAM, 0)
        try #require(listener >= 0)
        defer {
            shutdown(listener, SHUT_RDWR)
            Darwin.close(listener)
            try? FileManager.default.removeItem(at: directory)
        }
        try #require(withServiceAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
        try #require(listen(listener, 1) == 0)
        let accepted = Task {
            try await DispatchQueue(label: "com.yasyf.daemonkit.tests.silent-peer").performIO {
                let descriptor = accept(listener, nil, nil)
                guard descriptor >= 0 else {
                    throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                }
                return descriptor
            }
        }
        let started = ContinuousClock.now
        await #expect(throws: SessionTransportError.systemCall(operation: "read", errno: EAGAIN)) {
            _ = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                configuration: .init(handshakeTimeout: 0.05)
            )
        }
        #expect(started.duration(to: .now) < .milliseconds(500))
        let acceptedDescriptor = try await accepted.value
        Darwin.close(acceptedDescriptor)
    }

    @Test func handshakeCodecRejectsUnknownAndMalformedRejections() throws {
        let unknownJSON = #"{"protocol":1,"wire_build":"service.v1","rejected":true,"code":"later","reason":"no"}"#
        let malformedJSON = #"{"protocol":1,"wire_build":"service.v1","rejected":true,"code":"peer_untrusted"}"#
        let unknown = Data(unknownJSON.utf8)
        let malformed = Data(malformedJSON.utf8)

        let acknowledgment = try SessionHandshakeCodec.decodeAck(unknown)
        let code = try #require(acknowledgment.code)
        #expect(code == "later")
        #expect(throws: SessionTransportError.self) {
            _ = try SessionHandshakeCodec.decodeAck(malformed)
        }
    }

    @Test func oversizedLocalCallDoesNotPoisonTheServiceLifetime() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let lifecycle = try testRuntimeController()
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: lifecycle
            ) { request in
                .terminal(SocketTerminal(payload: request.payload))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1,
                configuration: .init(maximumFrameBytes: 512)
            )
            cleanup.add { await client.close() }

            await #expect(throws: SessionTransportError.self) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    payload: Data(repeating: 1, count: 1024),
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            let payload = Data(#""small""#.utf8)
            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                payload: payload,
                deadline: Date().addingTimeInterval(2)
            ))
            #expect(terminal.payload == payload)
            #expect(await client.startedGenerations == 1)
        }
    }
}
