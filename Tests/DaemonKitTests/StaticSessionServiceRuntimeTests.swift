@testable import DaemonKit
import Darwin
import Foundation
import Testing

private func sessionServiceConfiguration(
    maximumFrameBytes: Int = daemonKitDefaultMaximumFrameBytes,
    maximumRequestBytes: Int = 1024,
    maximumActiveRequests: Int = 8,
    maximumSessions: Int = 8,
    streamQueueDepth: Int = 4,
    maximumPendingWrites: Int = 8
) -> SessionServiceConfiguration {
    SessionServiceConfiguration(
        maximumFrameBytes: maximumFrameBytes,
        maximumRequestBytes: maximumRequestBytes,
        maximumActiveRequests: maximumActiveRequests,
        maximumSessions: maximumSessions,
        streamQueueDepth: streamQueueDepth,
        maximumPendingWrites: maximumPendingWrites,
        handshakeTimeout: 1,
        writeTimeout: 1
    )
}

private func stringService(
    path: String,
    configuration: SessionServiceConfiguration = sessionServiceConfiguration(),
    operation: String = "echo",
    tenant: String = "",
    handle: @escaping @Sendable (String) async -> String = { $0 }
) throws -> StaticSessionServiceRuntime<String, String> {
    try StaticSessionServiceRuntime(
        path: path,
        wireBuild: "service.v1",
        runtimeBuild: "service-build.v1",
        role: "dev.yasyf.test.client.v1",
        trust: .sameEffectiveUser,
        configuration: configuration,
        handler: SessionServiceHandler(
            operation: operation,
            tenant: tenant,
            codec: SessionServiceCodec(
                decodeRequest: { String(decoding: $0, as: UTF8.self) },
                encodeResponse: { try JSONEncoder().encode($0) }
            ),
            handle: handle
        )
    )
}

@Suite(.serialized)
struct StaticSessionServiceRuntimeTests {
    @Test func typedRouteStartsReadyAndReleasesSocketForRestart() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("typed.sock").path
        let runtime = try stringService(path: path) { "reply:\($0)" }

        await #expect(throws: Error.self) {
            _ = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "dev.yasyf.test.client.v1"
            )
        }
        try await runtime.start(deadline: Date().addingTimeInterval(2))
        let client = try await SocketClient(
            path: path,
            wireBuild: "service.v1",
            role: "dev.yasyf.test.client.v1"
        )
        let terminal = try await client.call(operation: "echo", payload: Data("one".utf8))
        #expect(try terminal.payload.map { try JSONDecoder().decode(String.self, from: $0) } == "reply:one")
        await client.close()
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))

        let replacement = try stringService(path: path)
        try await replacement.start(deadline: Date().addingTimeInterval(2))
        try await replacement.shutdown(deadline: Date().addingTimeInterval(2))
    }

    @Test func exactHandshakeAndRouteRejectBeforeTypedDispatch() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("exact.sock").path
        let calls = LockedCounter()
        let runtime = try stringService(path: path, operation: "only", tenant: "tenant-a") { value in
            calls.increment()
            return value
        }
        try await runtime.start(deadline: Date().addingTimeInterval(2))
        defer { Task { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) } }

        await #expect(throws: SocketHandshakeRejectionError.self) {
            _ = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "dev.yasyf.test.wrong.v1"
            )
        }
        await #expect(throws: SocketWireBuildMismatchError.self) {
            _ = try await SocketClient(
                path: path,
                wireBuild: "wrong.v1",
                role: "dev.yasyf.test.client.v1"
            )
        }
        let client = try await SocketClient(
            path: path,
            wireBuild: "service.v1",
            role: "dev.yasyf.test.client.v1"
        )
        let wrongOperation = try await client.call(operation: "other", tenant: "tenant-a")
        #expect(wrongOperation.rejected)
        #expect(wrongOperation.code == .permissionDenied)
        let wrongTenant = try await client.call(operation: "only", tenant: "tenant-b")
        #expect(wrongTenant.rejected)
        #expect(wrongTenant.code == .permissionDenied)
        #expect(calls.value == 0)
        await client.close()
    }

    @Test func wrongEffectiveUserIsRejectedDuringHandshake() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("uid.sock").path
        let server = SocketServer(
            path: path,
            wireBuild: "service.v1",
            sessionPolicy: SocketServer.SessionPolicy(
                effectiveUserID: geteuid() &+ 1,
                role: "dev.yasyf.test.client.v1",
                operation: "echo",
                tenant: ""
            )
        ) { _ in
            Issue.record("untrusted peer reached handler")
            return .terminal(SocketTerminal(payload: Data()))
        }
        try await server.start()
        defer { Task { await server.stop() } }

        do {
            _ = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "dev.yasyf.test.client.v1"
            )
            Issue.record("untrusted peer connected")
        } catch let error as SocketHandshakeRejectionError {
            #expect(error.code == .peerUntrusted)
        }
    }

    @Test func boundedRequestAndDecodeFailuresNeverDispatch() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("bounds.sock").path
        let calls = LockedCounter()
        let runtime = try StaticSessionServiceRuntime(
            path: path,
            wireBuild: "service.v1",
            runtimeBuild: "service-build.v1",
            role: "dev.yasyf.test.client.v1",
            trust: .sameEffectiveUser,
            configuration: sessionServiceConfiguration(maximumRequestBytes: 4),
            handler: SessionServiceHandler(
                operation: "number",
                tenant: "",
                codec: SessionServiceCodec<Int, Int>(
                    decodeRequest: { data in
                        guard let value = Int(String(decoding: data, as: UTF8.self)) else {
                            throw CocoaError(.coderInvalidValue)
                        }
                        return value
                    },
                    encodeResponse: { Data(String($0).utf8) }
                ),
                handle: { value in
                    calls.increment()
                    return value
                }
            )
        )
        try await runtime.start(deadline: Date().addingTimeInterval(2))
        defer { Task { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) } }
        let client = try await SocketClient(
            path: path,
            wireBuild: "service.v1",
            role: "dev.yasyf.test.client.v1"
        )
        defer { Task { await client.close() } }

        let tooLarge = try await client.call(operation: "number", payload: Data("12345".utf8))
        #expect(tooLarge.rejected)
        #expect(tooLarge.code == .requestTooLarge)
        let invalid = try await client.call(operation: "number", payload: Data("no".utf8))
        #expect(invalid.rejected)
        #expect(invalid.code == .invalidRequest)
        #expect(calls.value == 0)
    }

    @Test func streamedRequestIsAggregatedWithinBound() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("stream.sock").path
            let runtime = try stringService(path: path)
            try await runtime.start(deadline: Date().addingTimeInterval(2))
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "dev.yasyf.test.client.v1"
            )
            cleanup.add { await client.close() }
            let call = try await client.open(
                operation: "echo",
                payload: Data("a".utf8),
                endInput: false
            )
            try await call.sendChunk(Data("b".utf8))
            try await call.closeSend()
            let terminal = try await call.response()
            #expect(try terminal.payload.map { try JSONDecoder().decode(String.self, from: $0) } == "ab")
        }
    }

    @Test func shutdownCancelsHandlerAndRejectsFurtherAdmission() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("cancel.sock").path
        let entered = AsyncLatch()
        let cancelled = AsyncLatch()
        let runtime = try stringService(path: path) { value in
            entered.finish()
            do {
                try await Task.sleep(for: .seconds(30))
            } catch is CancellationError {
                cancelled.finish()
            } catch {}
            return value
        }
        try await runtime.start(deadline: Date().addingTimeInterval(2))
        let client = try await SocketClient(
            path: path,
            wireBuild: "service.v1",
            role: "dev.yasyf.test.client.v1"
        )
        let call = Task { try await client.call(operation: "echo", payload: Data("wait".utf8)) }
        await entered.wait()
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))
        await cancelled.wait()
        _ = await call.result
        await client.close()
    }

    @Test func startIsSingleUse() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("single.sock").path
        let runtime = try stringService(path: path)
        try await runtime.start(deadline: Date().addingTimeInterval(2))
        await #expect(throws: SessionServiceRuntimeError.startAlreadyIssued) {
            try await runtime.start(deadline: Date().addingTimeInterval(2))
        }
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))
        if case .draining = await runtime.wait() {} else {
            Issue.record("runtime did not retain draining result")
        }
    }
}

private final class LockedCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    var value: Int { lock.withLock { count } }

    func increment() {
        lock.withLock { count += 1 }
    }
}
