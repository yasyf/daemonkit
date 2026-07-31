@testable import DaemonKit
import Darwin
import Foundation
import Testing

@Suite(.timeLimit(.minutes(1)))
struct ServiceSocketClientTests {
    private func deadline(_ seconds: TimeInterval = 5) -> Date {
        Date().addingTimeInterval(seconds)
    }

    private func jsonObject(_ data: Data?) throws -> [String: Any] {
        let data = try #require(data)
        return try #require(try JSONSerialization.jsonObject(with: data) as? [String: Any])
    }

    @Test func businessCallRoundTripsThroughRealGoServer() async throws {
        let server = try GoWireServer()
        defer { server.shutdown() }
        let client = try businessServiceClient(server)
        defer { Task { await client.close() } }

        let terminal = try await client.call(genericServiceCall(
            operation: "test.echo.v1",
            payload: Data(#"{"value":7}"#.utf8),
            deadline: deadline()
        ))
        #expect(terminal.error == nil)
        #expect(!terminal.rejected)
        let echoed = try jsonObject(terminal.payload)
        #expect(echoed["value"] as? Int == 7)
    }

    @Test func directClientWaitsReadyThenEchoes() async throws {
        let server = try GoWireServer()
        defer { server.shutdown() }
        let client = try await SocketClient(
            path: server.socketPath,
            schema: GoWireServer.schema,
            lane: .business
        )
        defer { client.abort() }

        #expect(client.peerSchema == GoWireServer.schema)
        try await client.waitReady(deadline: deadline())
        #expect(client.phase.phase == .ready)
        let terminal = try await client.call(
            operation: "test.echo.v1",
            payload: Data(#"{"ok":true}"#.utf8),
            deadline: deadline()
        )
        #expect(try jsonObject(terminal.payload)["ok"] as? Bool == true)
    }

    @Test func waitReadyCrossesStartingToReady() async throws {
        let server = try GoWireServer(phases: "starting:300ms,ready")
        defer { server.shutdown() }
        let observed = PhaseRecorder()
        let client = try await SocketClient(
            path: server.socketPath,
            schema: GoWireServer.schema,
            lane: .business,
            onPhase: { observed.record($0.phase) }
        )
        defer { client.abort() }

        #expect(client.phase.phase == .starting)
        try await client.waitReady(deadline: deadline(10))
        #expect(client.phase.phase == .ready)
        #expect(observed.phases.contains(.ready))
        let terminal = try await client.call(
            operation: "test.echo.v1",
            payload: Data(#"{"n":1}"#.utf8),
            deadline: deadline()
        )
        #expect(terminal.error == nil)
    }

    @Test func rejectOperationSurfacesHandlerError() async throws {
        let server = try GoWireServer()
        defer { server.shutdown() }
        let client = try businessServiceClient(server)
        defer { Task { await client.close() } }

        let terminal = try await client.call(genericServiceCall(
            operation: "test.reject.v1",
            payload: Data(#"{"code":"denied","reason":"nope"}"#.utf8),
            deadline: deadline()
        ))
        let message = try #require(terminal.error)
        #expect(message.contains("nope"))
    }

    @Test func responseStreamDeliversChunksThenTerminal() async throws {
        let server = try GoWireServer()
        defer { server.shutdown() }
        let client = try await SocketClient(
            path: server.socketPath,
            schema: GoWireServer.schema,
            lane: .business
        )
        defer { client.abort() }
        try await client.waitReady(deadline: deadline())

        let call = try await client.open(
            operation: "test.chunks.v1",
            payload: Data(#"{"chunks":["a","b"],"value":{"v":9}}"#.utf8),
            deadline: deadline()
        )
        var chunks: [String] = []
        for try await chunk in call.chunks where !chunk.end {
            chunks.append(String(decoding: chunk.payload, as: UTF8.self))
        }
        #expect(chunks == ["a", "b"])
        let terminal = try await call.response()
        #expect(try jsonObject(terminal.payload)["v"] as? Int == 9)
    }

    @Test func serverEventReachesClient() async throws {
        let server = try GoWireServer()
        defer { server.shutdown() }
        let client = try await SocketClient(
            path: server.socketPath,
            schema: GoWireServer.schema,
            lane: .business
        )
        defer { client.abort() }
        try await client.waitReady(deadline: deadline())

        let received = Task { () throws -> SocketEvent? in
            for try await event in client.events {
                return event
            }
            return nil
        }
        _ = try await client.call(
            operation: "test.event.v1",
            payload: Data(#"{"topic":"lifecycle","payload":{"k":1}}"#.utf8),
            deadline: deadline()
        )
        let event = try await received.value
        #expect(event?.topic == "lifecycle")
    }

    @Test func drainingServerRejectsHandshakeWithTypedError() async throws {
        let server = try GoWireServer(phases: "draining")
        defer { server.shutdown() }
        await #expect(throws: SessionDrainingError.self) {
            _ = try await SocketClient(
                path: server.socketPath,
                schema: GoWireServer.schema,
                lane: .business
            )
        }
    }

    @Test func unacceptedSchemaRejectsThroughHandshake() async throws {
        let server = try GoWireServer()
        defer { server.shutdown() }
        do {
            _ = try await SocketClient(
                path: server.socketPath,
                schema: "other.v1",
                lane: .business
            )
            Issue.record("expected a build-mismatch rejection")
        } catch let rejection as SocketHandshakeRejectionError {
            #expect(rejection.code == .buildMismatch)
        }
    }

    @Test func noListenerDeadlineExpires() async throws {
        let client = try ServiceSocketClient(
            path: "/tmp/daemonkit-absent-\(getpid()).sock",
            schema: GoWireServer.schema,
            lane: .business
        )
        defer { Task { await client.close() } }
        await #expect(throws: ServiceSocketClientError.deadlineExceeded) {
            _ = try await client.call(genericServiceCall(
                operation: "test.echo.v1",
                deadline: Date().addingTimeInterval(0.3)
            ))
        }
    }
}

final class PhaseRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var storage: [SessionPhase] = []

    func record(_ phase: SessionPhase) {
        lock.withLock { storage.append(phase) }
    }

    var phases: [SessionPhase] {
        lock.withLock { storage }
    }
}
