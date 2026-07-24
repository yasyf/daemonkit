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

    @Test func publicServiceClientAcquiresReceiptReadinessAndCallsTypedRoute() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("service-client.sock").path
        let runtime = try stringService(path: path) { "reply:\($0)" }
        try await runtime.start(deadline: Date().addingTimeInterval(2))
        let client = try ServiceSocketClient(
            path: path,
            wireBuild: "service.v1",
            role: "dev.yasyf.test.client.v1",
            noProgressTimeout: 1
        )
        let terminal = try await client.call(ServiceSocketCall(
            operation: "echo",
            payload: Data("public".utf8),
            runtimeTarget: .exact(runtime.identity),
            deadline: Date().addingTimeInterval(2)
        ))
        #expect(try terminal.payload.map { try JSONDecoder().decode(String.self, from: $0) } == "reply:public")
        await client.close()
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))
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

    @Test func laterStreamOverflowRejectsAndSameSessionRemainsUsable() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("stream-overflow.sock").path
            let calls = LockedCounter()
            let runtime = try stringService(
                path: path,
                configuration: sessionServiceConfiguration(maximumRequestBytes: 4)
            ) { value in
                calls.increment()
                return value
            }
            try await runtime.start(deadline: Date().addingTimeInterval(2))
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "dev.yasyf.test.client.v1"
            )
            cleanup.add { await client.close() }

            let overflow = try await client.open(
                operation: "echo",
                payload: Data("12".utf8),
                endInput: false
            )
            try await overflow.sendChunk(Data("34".utf8))
            try await overflow.sendChunk(Data("5".utf8))
            try? await overflow.closeSend()
            let rejected = try await overflow.response()
            #expect(rejected.rejected)
            #expect(rejected.code == .requestTooLarge)
            #expect(calls.value == 0)

            let exact = try await client.call(operation: "echo", payload: Data("1234".utf8))
            #expect(try exact.payload.map { try JSONDecoder().decode(String.self, from: $0) } == "1234")
            #expect(calls.value == 1)
        }
    }

    @Test func cancellationWhileAwaitingStreamNeverDispatchesAndSessionRecovers() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("stream-cancel.sock").path
            let calls = LockedCounter()
            let runtime = try stringService(path: path) { value in
                calls.increment()
                return value
            }
            try await runtime.start(deadline: Date().addingTimeInterval(2))
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "dev.yasyf.test.client.v1"
            )
            cleanup.add { await client.close() }

            let canceled = try await client.open(operation: "echo", endInput: false)
            await canceled.cancel()
            _ = try? await canceled.response()
            #expect(calls.value == 0)

            let healthy = try await client.call(operation: "echo", payload: Data("ok".utf8))
            #expect(try healthy.payload.map { try JSONDecoder().decode(String.self, from: $0) } == "ok")
            #expect(calls.value == 1)
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

    @Test func timedOutShutdownRetainsOneSettlementOwnerAndCanBeRejoined() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("retry.sock").path
        let entered = AsyncLatch()
        let release = AsyncLatch()
        let runtime = try stringService(path: path) { value in
            entered.finish()
            await release.wait()
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

        await #expect(throws: RuntimeShutdownError.deadlineExceeded) {
            try await runtime.shutdown(deadline: Date().addingTimeInterval(0.02))
        }
        #expect(FileManager.default.fileExists(atPath: path))
        release.finish()
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))
        #expect(!FileManager.default.fileExists(atPath: path))
        _ = await call.result
        await client.close()

        let replacement = try stringService(path: path)
        try await replacement.start(deadline: Date().addingTimeInterval(2))
        try await replacement.shutdown(deadline: Date().addingTimeInterval(2))
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

    @Test func shutdownBeforeStartRetainsDrainingResult() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let runtime = try stringService(path: directory.appendingPathComponent("idle.sock").path)
        try await runtime.shutdown(deadline: Date().addingTimeInterval(2))
        if case .draining = await runtime.wait() {} else {
            Issue.record("idle shutdown did not retain draining result")
        }
    }

    @Test func bindFailureRetainsFailedResult() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("occupied.sock").path
        try Data("occupied".utf8).write(to: URL(fileURLWithPath: path))
        let runtime = try stringService(path: path)
        await #expect(throws: SocketServerError.self) {
            try await runtime.start(deadline: Date().addingTimeInterval(2))
        }
        if case .failed = await runtime.wait() {} else {
            Issue.record("bind failure did not retain failed result")
        }
    }

    @Test func postBindDeadlineCleansListenerBeforeReturning() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("deadline.sock").path
        let runtime = try DaemonRuntime(
            path: path,
            wireBuild: "service.v1",
            identity: RuntimeIdentity(
                runtimeBuild: "service-build.v1",
                processGeneration: testOwnerGeneration()
            ),
            handler: RuntimeHandlerSpec { _ in .terminal(SocketTerminal()) }
        )
        let entered = AsyncLatch()
        let release = AsyncLatch()
        runtime.serverStartCommitHook = {
            entered.finish()
            await release.wait()
        }
        let start = Task { try await runtime.begin(deadline: Date().addingTimeInterval(0.02)) }
        await entered.wait()
        try await Task.sleep(for: .milliseconds(30))
        release.finish()
        await #expect(throws: RuntimeShutdownError.deadlineExceeded) {
            _ = try await start.value
        }
        #expect(!FileManager.default.fileExists(atPath: path))
        if case .failed = await runtime.wait() {} else {
            Issue.record("post-bind deadline did not retain failed result")
        }

        let replacement = try stringService(path: path)
        try await replacement.start(deadline: Date().addingTimeInterval(2))
        try await replacement.shutdown(deadline: Date().addingTimeInterval(2))
    }
}

private final class LockedCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    var value: Int {
        lock.withLock { count }
    }

    func increment() {
        lock.withLock { count += 1 }
    }
}
