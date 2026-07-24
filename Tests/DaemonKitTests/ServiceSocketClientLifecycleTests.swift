@testable import DaemonKit
import Foundation
import Testing

extension SocketTransportTests.ServiceSocketClientTests {
    @Test func closeSettlesSocketBeforeCancelingLifecycleObserver() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("close-order.sock").path
            let lifecycle = try testRuntimeController()
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: lifecycle
            ) { _ in
                .terminal(SocketTerminal(payload: Data(#""healthy""#.utf8)))
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

            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(2)
            ))
            #expect(terminal.payload == Data(#""healthy""#.utf8))
            let recorder = ServiceCloseStepRecorder()
            await client.setCloseStepHook { recorder.append($0) }
            await client.close()

            #expect(recorder.snapshot() == [.socketSettled, .observerCanceled])
            guard case .closed = await client.termination.wait() else {
                Issue.record("explicit close retained a service failure")
                return
            }
        }
    }

    @Test func canceledReadinessDriverRetiresSubscribedSessionWithoutPolling() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let gate = OneShotRegistrationGate()
            let lifecycle = try testRuntimeController()
            lifecycle.registrationHook = { gate.register() }
            let server = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: lifecycle
            ) { _ in
                .terminal(SocketTerminal(payload: Data(#""healthy""#.utf8)))
            }
            try await server.start()
            cleanup.add {
                gate.release()
                await server.stop()
            }
            let client = try serviceTestClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected,
                noProgressTimeout: 1
            )
            cleanup.add { await client.close() }
            await client.setRetrySleepHook {
                Issue.record("readiness-driver handoff used retry polling")
            }

            let first = Task {
                try await client.call(genericServiceCall(operation: "first", deadline: Date().addingTimeInterval(2)))
            }
            await gate.firstEntered.wait()
            let second = Task {
                try await client.call(genericServiceCall(operation: "second", deadline: Date().addingTimeInterval(2)))
            }
            await Task.yield()
            first.cancel()
            await #expect(throws: CancellationError.self) { try await first.value }

            let terminal = try await second.value
            #expect(terminal.payload == Data(#""healthy""#.utf8))
            #expect(gate.callCount == 2)
            #expect(await client.startedGenerations == 2)
            gate.release()
        }
    }

    @Test func drainingRetiresTheGenerationBeforeRetry() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let (oldLifecycle, _) = try testStartingRuntimeController()
            let registered = AsyncLatch()
            oldLifecycle.registrationHook = { registered.finish() }
            let successorLifecycle = try testRuntimeController(generation: testOwnerGeneration(2))
            let oldServer = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                configuration: .init(writeTimeout: 0.05),
                runtimeLifecycle: oldLifecycle
            ) { _ in
                Issue.record("draining runtime dispatched business request")
                return .terminal(SocketTerminal())
            }
            let successorServer = serviceTestServer(
                path: path,
                wireBuild: "service.v1",
                runtimeLifecycle: successorLifecycle
            ) { _ in
                .terminal(SocketTerminal(payload: Data(#""successor""#.utf8)))
            }
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

            let call = Task {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            await registered.wait()
            try await oldLifecycle.closeIntake()
            await oldServer.stop()
            try await successorServer.start()
            let terminal = try await call.value
            #expect(terminal.payload == Data(#""successor""#.utf8))
            #expect(await client.startedGenerations >= 2)
        }
    }

    @Test func startingRejectionProvesNonDispatchAndRetriesOnSuccessor() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let responses = ServiceResponseSequence([
                SocketTerminal(rejected: true, code: .runtimeStarting, reason: "starting"),
                SocketTerminal(payload: Data(#""ready""#.utf8)),
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
            cleanup.add { await client.close() }

            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(2)
            ))
            #expect(terminal.payload == Data(#""ready""#.utf8))
            #expect(await responses.callCount() == 2)
            #expect(await client.startedGenerations == 2)
        }
    }

    @Test func malformedReadinessAttemptTerminatesLogicalLifetime() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = serviceTestServer(path: path, wireBuild: "service.v1") { _ in
                Issue.record("business request dispatched without readiness")
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

            await #expect(throws: ServiceSocketClientError.malformedAttempt) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            let termination = await client.termination.wait()
            guard case let .failed(error) = termination else {
                Issue.record("malformed readiness result was not retained")
                return
            }
            #expect(error as? ServiceSocketClientError == .malformedAttempt)
            await #expect(throws: ServiceSocketClientError.malformedAttempt) {
                try await client.call(genericServiceCall(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                ))
            }
            #expect(await client.startedGenerations == 1)
        }
    }

    @Test func untypedRejectionIsTerminal() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let responses = ServiceResponseSequence([
                SocketTerminal(rejected: true, reason: "fence mismatch"),
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
            cleanup.add { await client.close() }

            let terminal = try await client.call(genericServiceCall(
                operation: "work",
                deadline: Date().addingTimeInterval(2)
            ))
            #expect(terminal.rejected)
            #expect(terminal.reason == "fence mismatch")
            #expect(await responses.callCount() == 1)
        }
    }
}
