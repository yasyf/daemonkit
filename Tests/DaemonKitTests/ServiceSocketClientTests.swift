@testable import DaemonKit
import Foundation
import Testing

private actor ServiceResponseSequence {
    private var terminals: [SocketTerminal]
    private var calls = 0

    init(_ terminals: [SocketTerminal]) {
        self.terminals = terminals
    }

    func respond(to request: SocketRequest) -> SocketResponse {
        _ = request
        calls += 1
        return .terminal(terminals.removeFirst())
    }

    func callCount() -> Int {
        calls
    }
}

private struct ServiceTakeoverState {
    let oldCalls: Int
    let successorCalls: Int
    let oldStopped: Bool
    let successorStarted: Bool
}

private actor ServiceTakeoverSequence {
    private var oldServer: SocketServer?
    private var successorServer: SocketServer?
    private var oldCalls = 0
    private var successorCalls = 0
    private var oldStopped = false
    private var successorStarted = false

    func install(old: SocketServer, successor: SocketServer) {
        oldServer = old
        successorServer = successor
    }

    func drain(_ request: SocketRequest) -> SocketResponse {
        _ = request
        oldCalls += 1
        guard oldCalls == 1, let oldServer, let successorServer else {
            Issue.record("old generation received a replay")
            return .terminal(SocketTerminal(rejected: true, reason: "old generation replay"))
        }
        Task {
            try await Task.sleep(nanoseconds: 5_000_000)
            await oldServer.stop()
            self.markOldStopped()
            do {
                try await successorServer.start()
                self.markSuccessorStarted()
            } catch {
                Issue.record("successor start failed: \(error)")
            }
        }
        return .terminal(SocketTerminal(rejected: true, code: .serverDraining, reason: "draining"))
    }

    func succeed(_ request: SocketRequest) -> SocketResponse {
        _ = request
        successorCalls += 1
        return .terminal(SocketTerminal(payload: Data(#""successor""#.utf8)))
    }

    func state() -> ServiceTakeoverState {
        ServiceTakeoverState(
            oldCalls: oldCalls,
            successorCalls: successorCalls,
            oldStopped: oldStopped,
            successorStarted: successorStarted
        )
    }

    private func markOldStopped() {
        oldStopped = true
    }

    private func markSuccessorStarted() {
        successorStarted = true
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct ServiceSocketClientTests {
        @Test func startingRetriesOnTheSamePersistentSession() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let responses = ServiceResponseSequence([
                    SocketTerminal(rejected: true, code: .runtimeStarting, reason: "starting"),
                    SocketTerminal(payload: Data(#""ready""#.utf8)),
                ])
                let server = SocketServer(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser) {
                    await responses.respond(to: $0)
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = ServiceSocketClient(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                let terminal = try await client.call(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                )
                #expect(terminal.payload == Data(#""ready""#.utf8))
                #expect(await responses.callCount() == 2)
                #expect(await client.startedGenerations == 1)
            }
        }

        @Test func drainingRetiresTheGenerationBeforeRetry() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let takeover = ServiceTakeoverSequence()
                let oldServer = SocketServer(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser) {
                    await takeover.drain($0)
                }
                let successorServer = SocketServer(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser) {
                    await takeover.succeed($0)
                }
                await takeover.install(old: oldServer, successor: successorServer)
                try await oldServer.start()
                cleanup.add {
                    await oldServer.stop()
                    await successorServer.stop()
                }
                let client = ServiceSocketClient(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                let terminal = try await client.call(
                    operation: "work",
                    deadline: Date().addingTimeInterval(2)
                )
                #expect(terminal.payload == Data(#""successor""#.utf8))
                let state = await takeover.state()
                #expect(state.oldCalls == 1)
                #expect(state.successorCalls == 1)
                #expect(state.oldStopped)
                #expect(state.successorStarted)
                #expect(await client.startedGenerations >= 2)
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
                let server = SocketServer(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser) {
                    await responses.respond(to: $0)
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = ServiceSocketClient(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                let terminal = try await client.call(operation: "work", deadline: Date().addingTimeInterval(2))
                #expect(terminal.rejected)
                #expect(terminal.reason == "fence mismatch")
                #expect(await responses.callCount() == 1)
            }
        }

        @Test func missingEndpointHonorsDeadlineAndCancellation() async throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("missing.sock").path
            let deadlineClient = ServiceSocketClient(
                path: path,
                wireBuild: "service.v1",
                trust: .sameEffectiveUser
            )
            await #expect(throws: ServiceSocketClientError.deadlineExceeded) {
                try await deadlineClient.call(operation: "work", deadline: Date().addingTimeInterval(0.05))
            }

            let canceledClient = ServiceSocketClient(
                path: path,
                wireBuild: "service.v1",
                trust: .sameEffectiveUser
            )
            let task = Task {
                try await canceledClient.call(operation: "work", deadline: Date().addingTimeInterval(10))
            }
            task.cancel()
            await #expect(throws: CancellationError.self) {
                try await task.value
            }
        }

        @Test func peerBuildMismatchFailsBeforeDispatch() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let server = SocketServer(path: path, wireBuild: "other.v1", trust: .sameEffectiveUser) { _ in
                    Issue.record("mismatched build dispatched")
                    return .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = ServiceSocketClient(path: path, wireBuild: "service.v1", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                await #expect(throws: ServiceSocketClientError.peerWireBuild(got: "other.v1", want: "service.v1")) {
                    try await client.call(operation: "work", deadline: Date().addingTimeInterval(2))
                }
            }
        }
    }
}
