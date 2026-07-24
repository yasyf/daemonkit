@testable import DaemonKit
import Darwin
import Foundation
import Testing

extension SocketTransportTests.ServiceSocketClientTests {
    @Test func fullCallDeadlineReturnsWhilePeerWithholdsTerminalResponse() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let release = AsyncLatch()
            let hangingPayload = Data(#""hang""#.utf8)
            let lifecycle = try testRuntimeController()
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: lifecycle
            ) { request in
                if request.payload == hangingPayload {
                    await release.wait()
                }
                return .terminal(SocketTerminal(payload: request.payload))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1,
                configuration: .init(cancellationSettlementTimeout: 2)
            )
            cleanup.add { await client.close() }

            let clock = ContinuousClock()
            let started = clock.now
            await #expect(throws: ServiceSocketClientError.deadlineExceeded) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    payload: hangingPayload,
                    deadline: Date().addingTimeInterval(0.05)
                ))
            }
            #expect(started.duration(to: clock.now) < .milliseconds(500))

            let payload = Data(#""settled""#.utf8)
            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                payload: payload,
                deadline: Date().addingTimeInterval(1)
            ))
            #expect(terminal.payload == payload)
            #expect(await client.startedGenerations == 1)
            release.finish()
        }
    }

    @Test func closeCancelsABlockedHandshakePromptly() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("s.sock").path
        var address = try #require(makeAddress(path: path))
        let listener = socket(AF_UNIX, SOCK_STREAM, 0)
        try #require(listener >= 0)
        defer { Darwin.close(listener) }
        try #require(withServiceAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
        try #require(listen(listener, 1) == 0)
        let peerQueue = DispatchQueue(label: "com.yasyf.daemonkit.tests.blocked-service-handshake")
        let accepted = Task {
            try await peerQueue.performIO { () throws -> Int32 in
                let peer = accept(listener, nil, nil)
                guard peer >= 0 else {
                    throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                }
                do {
                    let hello = try SessionFrameCodec(descriptor: peer).read()
                    guard hello.kind == .hello else {
                        throw SessionTransportError.handshake("expected client hello")
                    }
                    return peer
                } catch {
                    Darwin.close(peer)
                    throw error
                }
            }
        }
        let client = try serviceTestClient(
            path: path,
            wireBuild: "service.v1",
            role: SessionPeerRole.unprotected,
            noProgressTimeout: 1,
            configuration: .init(handshakeTimeout: 5)
        )
        let call = Task {
            try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(5)
            ))
        }
        let peer = try await accepted.value
        defer { Darwin.close(peer) }

        let clock = ContinuousClock()
        let started = clock.now
        await client.close()
        #expect(started.duration(to: clock.now) < .milliseconds(500))
        await #expect(throws: (any Error).self) { try await call.value }
        let termination = await client.termination.wait()
        guard case .closed = termination else {
            Issue.record("explicit close did not win the logical lifetime")
            return
        }
    }

    @Test func malformedPeerResponseTerminatesAndIsRetained() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("s.sock").path
        var address = try #require(makeAddress(path: path))
        let listener = socket(AF_UNIX, SOCK_STREAM, 0)
        try #require(listener >= 0)
        defer { Darwin.close(listener) }
        try #require(withServiceAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
        try #require(listen(listener, 1) == 0)
        let peerQueue = DispatchQueue(label: "com.yasyf.daemonkit.tests.malformed-service-peer")
        let peer = Task {
            try await peerQueue.performIO {
                let descriptor = accept(listener, nil, nil)
                guard descriptor >= 0 else {
                    throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                }
                defer { Darwin.close(descriptor) }
                let codec = SessionFrameCodec(descriptor: descriptor)
                let hello = try codec.read(timeout: 1)
                _ = try SessionHandshakeCodec.decodeHello(hello.payload)
                let acknowledgment = try SessionHandshakeCodec.encodeSuccess(
                    wireBuild: "service.v1",
                    session: Data(repeating: 3, count: 16)
                )
                try codec.write(SessionFrame(kind: .helloAck, flags: .end, payload: acknowledgment))

                let readiness = try nextRequest(codec)
                let subscribeEnvelope = Data(#"{"ack":true,"payload":{"protocol":1}}"#.utf8)
                try codec.write(SessionFrame(
                    kind: .response,
                    flags: .end,
                    id: readiness.id,
                    payload: subscribeEnvelope
                ))
                try codec.write(SessionFrame(
                    kind: .lifecycle,
                    flags: .end,
                    payload: lifecyclePayload(.starting, sequence: 1)
                ))
                try codec.write(SessionFrame(
                    kind: .lifecycle,
                    flags: .end,
                    payload: lifecyclePayload(.ready, sequence: 2)
                ))
                let business = try nextRequest(codec)
                try codec.write(SessionFrame(
                    kind: .response,
                    flags: .end,
                    id: business.id,
                    payload: Data(#"{"unexpected":true}"#.utf8)
                ))
            }
        }
        let client = try serviceTestClient(
            path: path,
            wireBuild: "service.v1",
            role: SessionPeerRole.unprotected,
            noProgressTimeout: 1
        )
        let expected = SessionTransportError.invalidFrame("response fields")

        await #expect(throws: expected) {
            try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(2)
            ))
        }
        let termination = await client.termination.wait()
        guard case let .failed(error) = termination else {
            Issue.record("malformed peer response did not terminate the logical client")
            return
        }
        #expect(error as? SessionTransportError == expected)
        await #expect(throws: expected) {
            try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(2)
            ))
        }
        try await peer.value
    }

    @Test func rawAttemptsDistinguishProvenNonDispatchFromUnknownDelivery() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = serviceTestServer(path: path, wireBuild: "service.v1") { _ in
                Issue.record("classified raw attempt dispatched")
                return .terminal(SocketTerminal())
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await client.close() }

            let invalid = await client.attempt(
                operation: "",
                deadline: Date().addingTimeInterval(2)
            )
            #expect(invalid.outcome == .preSendFailure)

            let gate = ServiceWriteGate()
            client.requestWriteStartHook = { gate.block() }
            let attempt = Task {
                await client.attempt(operation: "work", deadline: Date().addingTimeInterval(2))
            }
            await gate.started.wait()
            await server.stop()
            gate.unblock()
            let unknown = await attempt.value
            #expect(unknown.outcome == .deliveryUnknown)
        }
    }

    @Test func replayPolicyControlsUnknownDeliveryAcrossSuccessor() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let takeover = UnknownDeliveryTakeoverSequence()
            let oldLifecycle = try testRuntimeController()
            let successorLifecycle = try testRuntimeController(generation: testOwnerGeneration(2))
            let oldServer = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: oldLifecycle
            ) {
                await takeover.disconnect($0)
            }
            let successorServer = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: successorLifecycle
            ) {
                await takeover.succeed($0)
            }
            await takeover.install(old: oldServer, successor: successorServer)
            try await oldServer.start()
            cleanup.add {
                await oldServer.stop()
                await successorServer.stop()
            }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            )
            cleanup.add { await client.close() }

            await #expect(throws: SessionTransportError.disconnected) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    replay: .provenNonDispatch,
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                replay: .idempotent,
                deadline: Date().addingTimeInterval(2)
            ))
            #expect(terminal.payload == Data(#""replayed""#.utf8))
            let counts = await takeover.counts()
            #expect(counts.old == 1)
            #expect(counts.successor == 1)
        }
    }

    @Test func terminalFailureIsRetainedForLifetime() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let responses = ServiceResponseSequence([
                SocketTerminal(rejected: true, code: .buildMismatch, reason: "wrong build"),
            ])
            let lifecycle = try testRuntimeController()
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: lifecycle
            ) {
                await responses.respond(to: $0)
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            )

            await #expect(throws: ServiceSocketRejectionError.self) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            let termination = await client.termination.wait()
            guard case let .failed(error) = termination else {
                Issue.record("logical lifetime did not retain failure")
                return
            }
            #expect(error is ServiceSocketRejectionError)
            await #expect(throws: ServiceSocketRejectionError.self) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            #expect(await responses.callCount() == 1)
        }
    }
}

extension SocketTransportTests.ServiceSocketClientTests {
    @Test func localCancellationDoesNotRetireSharedGeneration() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let responses = ServiceCancellationSequence()
            let lifecycle = try testRuntimeController()
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: lifecycle
            ) {
                await responses.respond($0)
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

            let canceled = Task {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            await responses.entered.wait()
            canceled.cancel()
            responses.release.finish()
            await #expect(throws: CancellationError.self) {
                try await canceled.value
            }

            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(2)
            ))
            #expect(terminal.payload == Data(#""healthy""#.utf8))
            #expect(await client.startedGenerations == 1)
        }
    }

    @Test func terminalLifecycleWinsPeerCloseRace() async throws {
        let identity = RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: testOwnerGeneration())
        for state in [RuntimeReadinessState.failed, .draining] {
            try await withRawServicePeers(count: 1) { _, codec in
                let receipt = try nextRequest(codec)
                guard receipt.operation == runtimeReceiptOperation else {
                    throw SessionTransportError.invalidFrame("expected runtime receipt request")
                }
                try writeRawTerminal(
                    RuntimeReceiptCodec.encodeResponse(identity),
                    for: receipt,
                    to: codec
                )
                let subscription = try nextRequest(codec)
                guard subscription.operation == readinessSubscribeOperation else {
                    throw SessionTransportError.invalidFrame("expected readiness subscription")
                }
                try writeRawTerminal(readinessSubscribeAck, for: subscription, to: codec)
                try codec.write(SessionFrame(
                    kind: .lifecycle,
                    flags: .end,
                    payload: lifecyclePayload(state, sequence: 2)
                ))
            } operation: { path in
                let client = try serviceTestClient(
                    path: path,
                    wireBuild: "service.v1",
                    role: SessionPeerRole.unprotected,
                    noProgressTimeout: 1
                )
                do {
                    _ = try await client.call(ServiceSocketCall(
                        operation: "work",
                        runtimeTarget: .exact(identity),
                        deadline: Date().addingTimeInterval(2)
                    ))
                    Issue.record("terminal lifecycle was lost to peer close")
                } catch let error as RuntimeFailedError {
                    #expect(state == .failed)
                    #expect(error.snapshot.progress.state == .failed)
                } catch let error as RuntimeReadinessValidationError {
                    guard case let .draining(snapshot) = error else {
                        Issue.record("unexpected readiness validation error \(error)")
                        await client.close()
                        return
                    }
                    #expect(state == .draining)
                    #expect(snapshot.progress.state == .draining)
                }
                let generations = await client.startedGenerations
                await #expect(throws: (any Error).self) {
                    _ = try await client.call(ServiceSocketCall(
                        operation: "again",
                        runtimeTarget: .exact(identity),
                        deadline: Date().addingTimeInterval(1)
                    ))
                }
                #expect(await client.startedGenerations == generations)
                #expect(generations == 1)
                let termination = await client.termination.wait()
                guard case .failed = termination else {
                    Issue.record("terminal lifecycle was not retained for the service lifetime")
                    return
                }
            }
        }
    }

    @Test func brokerHandoffTerminalResponseCannotOutliveFixedDeadline() async throws {
        let identity = RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: testOwnerGeneration())
        let responseGate = ServiceWriteGate()
        try await withRawServicePeers(count: 1) { _, codec in
            let handoff = try nextRequest(codec)
            guard handoff.operation == brokerHandoffOperation else {
                throw SessionTransportError.invalidFrame("expected broker handoff request")
            }
            responseGate.block()
        } operation: { path in
            defer { responseGate.unblock() }
            let client = try BrokerHandoffClient(
                path: path,
                wireBuild: "service.v1",
                role: "handoff",
                configuration: .init()
            )
            var connected: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &connected) == 0)
            let passed = connected[0]
            defer { Darwin.close(connected[1]) }
            await #expect(throws: BrokerHandoffError.deliveryUnknown) {
                try await client.handoff(
                    descriptor: passed,
                    runtimeIdentity: identity,
                    parentDeadline: Date().addingTimeInterval(0.03)
                )
            }
            #expect(responseGate.started.isFinished)
            await client.close()
        }
    }

    @Test func bufferedNonterminalLifecycleIsDiscardedAfterCloseCompletes() async throws {
        for state in [RuntimeReadinessState.starting, .ready] {
            try await withRawServicePeers(count: 1) { _, codec in
                let window = try codec.read(timeout: 1)
                guard window.kind == .window else {
                    throw SessionTransportError.invalidFrame("expected initial event window")
                }
                try codec.write(SessionFrame(
                    kind: .lifecycle,
                    flags: .end,
                    payload: lifecyclePayload(state, sequence: state == .starting ? 1 : 2)
                ))
            } operation: { path in
                let client = try await SocketClient(
                    path: path,
                    wireBuild: "service.v1",
                    role: SessionPeerRole.unprotected
                )
                await client.waitUntilClosed()
                await #expect(throws: SessionTransportError.disconnected) {
                    _ = try await client.nextLifecycleSnapshot()
                }
                await client.close()
            }
        }
    }

    @Test func malformedLifecycleBeforePeerCloseIsProtocolTerminal() async throws {
        let closeGate = ServiceWriteGate()
        try await withRawServicePeers(count: 1) { _, codec in
            let subscription = try nextRequest(codec)
            guard subscription.operation == readinessSubscribeOperation else {
                throw SessionTransportError.invalidFrame("expected readiness subscription")
            }
            try writeRawTerminal(readinessSubscribeAck, for: subscription, to: codec)
            try codec.write(SessionFrame(
                kind: .lifecycle,
                flags: .end,
                payload: Data("{}".utf8)
            ))
            closeGate.block()
        } operation: { path in
            defer { closeGate.unblock() }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            )
            let expected = RuntimeReadinessValidationError.invalidResponse("readiness event fields")
            do {
                _ = try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
                Issue.record("malformed lifecycle did not fail the call")
                return
            } catch {
                guard error as? RuntimeReadinessValidationError == expected else {
                    throw error
                }
            }
            let termination = await client.termination.wait()
            guard case let .failed(error) = termination else {
                Issue.record("malformed lifecycle did not terminate the service lifetime")
                return
            }
            #expect(error as? RuntimeReadinessValidationError == expected)
        }
    }
}
