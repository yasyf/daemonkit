@testable import DaemonKit
import Darwin
import Foundation
import Testing

let readinessSubscribeOperation = "test.runtime.readiness.subscribe"
let readinessSubscribeAck = Data(#"{"protocol":1}"#.utf8)

func lifecyclePayload(
    _ state: RuntimeReadinessState,
    sequence: UInt64,
    generation: OwnerGeneration = testOwnerGeneration()
) -> Data {
    let progress = #"{"progress":{"detail":"","sequence":\#(sequence),"state":"\#(state.rawValue)"},"protocol":1,"#
    let runtime = #""runtime_identity":{"process_generation":"\#(generation.value)","runtime_build":"app.v1"},"#
    let json = progress + runtime + #""wire_build":"service.v1"}"#
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

final class ServiceCloseStepRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var steps: [ServiceSocketCloseStep] = []

    func append(_ step: ServiceSocketCloseStep) {
        lock.withLock { steps.append(step) }
    }

    func snapshot() -> [ServiceSocketCloseStep] {
        lock.withLock { steps }
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
    }
}
