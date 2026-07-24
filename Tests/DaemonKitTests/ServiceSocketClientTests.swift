@testable import DaemonKit
import Darwin
import Foundation
import Testing

let readinessSubscribeOperation = "test.runtime.readiness.subscribe"
let readinessSubscribeAck = Data(#"{"protocol":1}"#.utf8)

func serviceTestClient(
    path: String,
    wireBuild: String,
    role: String,
    noProgressTimeout: TimeInterval,
    configuration: SocketClient.Configuration = .init(),
    onProgress: (@Sendable (ReadinessProgress) -> Void)? = nil
) throws -> ServiceSocketClient {
    try ServiceSocketClient(
        path: path,
        wireBuild: wireBuild,
        role: role,
        readinessOperation: readinessSubscribeOperation,
        noProgressTimeout: noProgressTimeout,
        configuration: configuration,
        onProgress: onProgress
    )
}

func serviceTestServer(
    path: String,
    wireBuild: String,
    configuration: SocketServer.Configuration = .init(),
    runtimeLifecycle: RuntimeLifecycleController? = nil,
    handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
) -> SocketServer {
    guard let runtimeLifecycle else {
        return SocketServer(path: path, wireBuild: wireBuild, configuration: configuration) { request in
            guard request.operation == readinessSubscribeOperation else {
                return await handler(request)
            }
            return .terminal(SocketTerminal(error: "wire: runtime lifecycle is not configured"))
        }
    }
    return SocketServer(
        path: path,
        wireBuild: wireBuild,
        configuration: configuration,
        runtimeLifecycle: runtimeLifecycle,
        controlOperations: [readinessSubscribeOperation]
    ) { request in
        guard request.operation == readinessSubscribeOperation else {
            return await handler(request)
        }
        do {
            try RuntimeReadinessCodec.decodeSubscribeAck(request.payload)
            guard let session = request.session.implementation else {
                throw SessionTransportError.disconnected
            }
            runtimeLifecycle.register(session)
            Task {
                await request.session.waitUntilClosed()
                runtimeLifecycle.unregister(session)
            }
            return try .terminal(SocketTerminal(
                payload: RuntimeReadinessCodec.encodeSubscribe()
            ) {
                runtimeLifecycle.activate(session)
            })
        } catch {
            return .terminal(SocketTerminal(error: String(describing: error)))
        }
    }
}

func genericServiceCall(
    operation: String,
    tenant: String = "",
    payload: Data = Data(),
    replay: ServiceSocketReplayPolicy = .provenNonDispatch,
    deadline: Date
) -> ServiceSocketCall {
    ServiceSocketCall(
        operation: operation,
        tenant: tenant,
        payload: payload,
        replay: replay,
        runtimeTarget: .anyAuthenticatedSuccessor,
        deadline: deadline
    )
}

func lifecyclePayload(
    _ state: RuntimeReadinessState,
    sequence: UInt64,
    generation: OwnerGeneration = testOwnerGeneration()
) -> Data {
    let json = #"{"progress":{"detail":"","sequence":\#(sequence),"state":"\#(state.rawValue)"},"protocol":1,"# +
        #""runtime_identity":{"process_generation":"\#(generation.value)","runtime_build":"app.v1"},"wire_build":"service.v1"}"#
    return Data(json.utf8)
}

func testRuntimeController(
    generation: OwnerGeneration = testOwnerGeneration()
) throws -> RuntimeLifecycleController {
    let controller = try liveRuntimeController(
        wireBuild: "service.v1",
        runtimeIdentity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: generation)
    )
    let activation = try controller.beginActivation()
    try commitTestRuntime(controller, activation: activation)
    return controller
}

func testStartingRuntimeController(
    generation: OwnerGeneration = testOwnerGeneration()
) throws -> (RuntimeLifecycleController, RuntimeActivation) {
    let controller = try liveRuntimeController(
        wireBuild: "service.v1",
        runtimeIdentity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: generation)
    )
    return try (controller, controller.beginActivation())
}

func commitTestRuntime(
    _ controller: RuntimeLifecycleController,
    activation: RuntimeActivation
) throws {
    let slot = RuntimePublicationSlot<Void>(controller: controller)
    let publication = try slot.stage(activation, value: ())
    try activation.commitReady(publication)
}

func withServiceAddress<Result>(
    _ address: inout sockaddr_un,
    _ body: (UnsafePointer<sockaddr>, socklen_t) -> Result
) -> Result {
    withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            body($0, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
}

func nextRequest(_ codec: SessionFrameCodec) throws -> SessionFrame {
    while true {
        let frame = try codec.read(timeout: 1)
        if frame.kind == .request {
            return frame
        }
    }
}

func withRawServicePeers<Result>(
    count: Int,
    serve: @escaping @Sendable (Int, SessionFrameCodec) throws -> Void,
    operation: (String) async throws -> Result
) async throws -> Result {
    let directory = try shortSocketDir()
    let path = directory.appendingPathComponent("raw.sock").path
    var address = try #require(makeAddress(path: path))
    let listener = socket(AF_UNIX, SOCK_STREAM, 0)
    try #require(listener >= 0)
    try #require(withServiceAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
    try #require(listen(listener, Int32(count)) == 0)
    let peer = Task {
        try await DispatchQueue(label: "com.yasyf.daemonkit.tests.raw-service-peer").performIO {
            for index in 0 ..< count {
                let descriptor = accept(listener, nil, nil)
                guard descriptor >= 0 else {
                    throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                }
                do {
                    let codec = SessionFrameCodec(descriptor: descriptor)
                    let hello = try codec.read(timeout: 1)
                    _ = try SessionHandshakeCodec.decodeHello(hello.payload)
                    try codec.write(SessionFrame(
                        kind: .helloAck,
                        flags: .end,
                        payload: SessionHandshakeCodec.encodeSuccess(
                            wireBuild: "service.v1",
                            session: Data(repeating: UInt8(index + 1), count: 16)
                        )
                    ))
                    try serve(index, codec)
                    Darwin.close(descriptor)
                } catch {
                    Darwin.close(descriptor)
                    throw error
                }
            }
        }
    }
    do {
        let result = try await operation(path)
        try await peer.value
        Darwin.close(listener)
        try? FileManager.default.removeItem(at: directory)
        return result
    } catch {
        shutdown(listener, SHUT_RDWR)
        Darwin.close(listener)
        peer.cancel()
        _ = try? await peer.value
        try? FileManager.default.removeItem(at: directory)
        throw error
    }
}

func writeRawTerminal(
    _ payload: Data,
    for request: SessionFrame,
    to codec: SessionFrameCodec
) throws {
    let object = try JSONSerialization.jsonObject(with: payload)
    let envelope = try JSONSerialization.data(withJSONObject: ["payload": object])
    try codec.write(SessionFrame(kind: .response, flags: .end, id: request.id, payload: envelope))
}

final class ServiceWriteGate: @unchecked Sendable {
    let started = AsyncLatch()
    private let release = DispatchSemaphore(value: 0)

    func block() {
        started.finish()
        release.wait()
    }

    func unblock() {
        release.signal()
    }
}

final class OneShotRegistrationGate: @unchecked Sendable {
    let firstEntered = AsyncLatch()
    private let lock = NSLock()
    private let releaseFirst = DispatchSemaphore(value: 0)
    private var calls = 0

    func register() {
        let shouldBlock = lock.withLock {
            calls += 1
            return calls == 1
        }
        guard shouldBlock else { return }
        firstEntered.finish()
        releaseFirst.wait()
    }

    func release() {
        releaseFirst.signal()
    }

    var callCount: Int {
        lock.withLock { calls }
    }
}

actor ServiceResponseSequence {
    private var terminals: [SocketTerminal]
    private var calls = 0

    init(_ terminals: [SocketTerminal]) {
        self.terminals = terminals
    }

    func respond(to _: SocketRequest) -> SocketResponse {
        calls += 1
        return .terminal(terminals.removeFirst())
    }

    func callCount() -> Int {
        calls
    }
}

struct ServiceTakeoverState {
    let oldCalls: Int
    let successorCalls: Int
}

actor ServiceTakeoverSequence {
    let drainObserved = AsyncLatch()
    private var oldCalls = 0
    private var successorCalls = 0

    func drain(_: SocketRequest) -> SocketResponse {
        oldCalls += 1
        drainObserved.finish()
        return .terminal(SocketTerminal(rejected: true, code: .runtimeDraining, reason: "draining"))
    }

    func succeed(_: SocketRequest) -> SocketResponse {
        successorCalls += 1
        return .terminal(SocketTerminal(payload: Data(#""successor""#.utf8)))
    }

    func state() -> ServiceTakeoverState {
        ServiceTakeoverState(
            oldCalls: oldCalls,
            successorCalls: successorCalls
        )
    }
}

actor UnknownDeliveryTakeoverSequence {
    private var oldServer: SocketServer?
    private var successorServer: SocketServer?
    private var oldCalls = 0
    private var successorCalls = 0

    func install(old: SocketServer, successor: SocketServer) {
        oldServer = old
        successorServer = successor
    }

    func disconnect(_ request: SocketRequest) -> SocketResponse {
        oldCalls += 1
        guard oldCalls == 1, let oldServer, let successorServer else {
            Issue.record("old generation received an unexpected replay")
            return .terminal(SocketTerminal(error: "unexpected replay"))
        }
        let session = request.session
        session.implementation?.close()
        Task {
            await session.waitUntilClosed()
            await oldServer.stop()
            do {
                try await successorServer.start()
            } catch {
                Issue.record("successor start failed: \(error)")
            }
        }
        return .terminal(SocketTerminal(payload: Data(#""lost""#.utf8)))
    }

    func succeed(_: SocketRequest) -> SocketResponse {
        successorCalls += 1
        return .terminal(SocketTerminal(payload: Data(#""replayed""#.utf8)))
    }

    func counts() -> (old: Int, successor: Int) {
        (oldCalls, successorCalls)
    }
}

actor ServiceCancellationSequence {
    let entered = AsyncLatch()
    let release = AsyncLatch()
    private var calls = 0

    func respond(_: SocketRequest) async -> SocketResponse {
        calls += 1
        switch calls {
        case 1:
            entered.finish()
            await release.wait()
            return .terminal(SocketTerminal(payload: Data(#""canceled""#.utf8)))
        case 2:
            return .terminal(SocketTerminal(payload: Data(#""healthy""#.utf8)))
        default:
            Issue.record("unexpected cancellation test call \(calls)")
            return .terminal(SocketTerminal(error: "unexpected call"))
        }
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct ServiceSocketClientTests {
        @Test func startingLifecycleAdvancesOnTheSamePersistentSession() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let responses = ServiceResponseSequence([
                    SocketTerminal(payload: Data(#""ready""#.utf8)),
                ])
                let (lifecycle, activation) = try testStartingRuntimeController()
                let registered = AsyncLatch()
                lifecycle.registrationHook = { registered.finish() }
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
                await client.setRetrySleepHook {
                    Issue.record("connected lifecycle readiness used retry polling")
                }

                let call = Task {
                    try await client.call(genericServiceCall(
                        operation: "work",
                        deadline: Date().addingTimeInterval(2)
                    ))
                }
                await registered.wait()
                try commitTestRuntime(lifecycle, activation: activation)
                let terminal = try await call.value
                #expect(terminal.payload == Data(#""ready""#.utf8))
                #expect(await responses.callCount() == 1)
                #expect(await client.startedGenerations == 1)
            }
        }

        @Test func concurrentCallsShareOneLifecycleSubscriptionPerGeneration() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let responses = ServiceResponseSequence((0 ..< 12).map { index in
                    SocketTerminal(payload: Data("\(index)".utf8))
                })
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

                try await withThrowingTaskGroup(of: SocketTerminal.self) { group in
                    for _ in 0 ..< 12 {
                        group.addTask {
                            try await client.call(genericServiceCall(
                                operation: "work",
                                deadline: Date().addingTimeInterval(2)
                            ))
                        }
                    }
                    for try await terminal in group {
                        #expect(terminal.payload != nil)
                    }
                }
                #expect(await responses.callCount() == 12)
                #expect(await client.startedGenerations == 1)
            }
        }

        @Test func canceledReadinessDriverWakesNextCallerWithoutPolling() async throws {
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
                    try await client.call(genericServiceCall(
                        operation: "first",
                        deadline: Date().addingTimeInterval(2)
                    ))
                }
                await gate.firstEntered.wait()
                let second = Task {
                    try await client.call(genericServiceCall(
                        operation: "second",
                        deadline: Date().addingTimeInterval(2)
                    ))
                }
                await Task.yield()
                first.cancel()
                await #expect(throws: CancellationError.self) { try await first.value }

                let terminal = try await second.value
                #expect(terminal.payload == Data(#""healthy""#.utf8))
                #expect(gate.callCount == 2)
                #expect(await client.startedGenerations == 1)
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
}

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

    @Test func handshakeCodecRejectsUnknownAndMalformedRejections() throws {
        let unknown = Data(#"{"protocol":1,"wire_build":"service.v1","rejected":true,"code":"later","reason":"no"}"#.utf8)
        let malformed = Data(#"{"protocol":1,"wire_build":"service.v1","rejected":true,"code":"peer_untrusted"}"#.utf8)

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
