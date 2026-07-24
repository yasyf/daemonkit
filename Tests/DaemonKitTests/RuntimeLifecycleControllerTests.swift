@testable import DaemonKit
import Foundation
import Testing

private func encodedJSONString(_ value: String) -> Data {
    (try? JSONEncoder().encode(value)) ?? Data("null".utf8)
}

func liveRuntimeController(
    wireBuild: String,
    runtimeIdentity: RuntimeIdentity
) throws -> RuntimeLifecycleController {
    let controller = try RuntimeLifecycleController(
        wireBuild: wireBuild,
        runtimeIdentity: runtimeIdentity
    )
    controller.markServerLive()
    return controller
}

private actor LifecycleHandlerProbe {
    private var operations: [String] = []

    func handle(_ request: SocketRequest) -> SocketResponse {
        operations.append(request.operation)
        return .terminal(SocketTerminal(payload: Data(#""done""#.utf8)))
    }

    func snapshot() -> [String] {
        operations
    }
}

private final class StringSlotProbe: @unchecked Sendable {
    private let lock = NSLock()
    private var stored: RuntimePublicationSlot<String>?

    var slot: RuntimePublicationSlot<String> {
        get { lock.withLock { stored! } }
        set { lock.withLock { stored = newValue } }
    }
}

private final class ActivationCounter: @unchecked Sendable {
    private let lock = NSLock()
    private var count = 0

    var value: Int {
        lock.withLock { count }
    }

    func increment() {
        lock.withLock { count += 1 }
    }
}

private final class SecondRegistrationGate: @unchecked Sendable {
    let secondEntered = AsyncLatch()
    private let lock = NSLock()
    private let releaseSecond = DispatchSemaphore(value: 0)
    private var attempts = 0

    func enter() {
        let isSecond = lock.withLock {
            attempts += 1
            return attempts == 2
        }
        guard isSecond else { return }
        secondEntered.finish()
        releaseSecond.wait()
    }

    func release() {
        releaseSecond.signal()
    }
}

private enum TestRuntimeFailure: Error {
    case failed
}

private enum TestUnexpectedServerFailure: Error {
    case failed
}

private func captureRuntimeResult(
    _ operation: @escaping @Sendable () async throws -> Void
) async -> Result<Void, any Error> {
    do {
        try await operation()
        return .success(())
    } catch {
        return .failure(error)
    }
}

private actor RetainedRequestProbe {
    private var request: SocketRequest?

    func retain(_ request: SocketRequest) -> SocketResponse {
        self.request = request
        return .terminal(SocketTerminal(payload: Data("true".utf8)))
    }

    func pinnedValue(in slot: RuntimePublicationSlot<String>) -> String? {
        guard let request else { return nil }
        return slot.loadPinned(from: request)
    }
}

private actor BlockingRequestProbe {
    private var request: SocketRequest?
    let entered = AsyncLatch()
    let release = AsyncLatch()

    func handle(_ request: SocketRequest) async -> SocketResponse {
        self.request = request
        entered.finish()
        await release.wait()
        return .terminal(SocketTerminal(payload: Data("true".utf8)))
    }

    func pinnedValue(in slot: RuntimePublicationSlot<String>) -> String? {
        guard let request else { return nil }
        return slot.loadPinned(from: request)
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct RuntimeLifecycleControllerTests {
        private let identity = RuntimeIdentity(
            runtimeBuild: "app.v1",
            processGeneration: testOwnerGeneration()
        )

        @Test func progressIsCopiedBoundedIdempotentAndSequenced() throws {
            let controller = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            let activation = try controller.beginActivation()
            var detail = Data("first".utf8)
            try activation.updateProgress(detail)
            detail[detail.startIndex] = 0x78
            #expect(controller.currentEvent().progress.sequence == 2)
            #expect(controller.currentEvent().progress.detail == Data("first".utf8))

            try activation.updateProgress(Data("first".utf8))
            #expect(controller.currentEvent().progress.sequence == 2)
            try activation.updateProgress(Data("second".utf8))
            #expect(controller.currentEvent().progress.sequence == 3)
            #expect(throws: RuntimeReadinessValidationError.self) {
                try activation.updateProgress(Data(repeating: 1, count: 4097))
            }
        }

        @Test func beginStartsInStartingAndCommitAtomicallyPublishesAndAdmits() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("runtime.sock").path
                let handler = LifecycleHandlerProbe()
                let slotProbe = StringSlotProbe()
                let runtime = try DaemonRuntime(
                    path: path,
                    wireBuild: "service.v1",
                    identity: identity,
                    handler: RuntimeHandlerSpec { request in
                        let value = slotProbe.slot.loadPinned(from: request) ?? "missing"
                        _ = await handler.handle(request)
                        return .terminal(SocketTerminal(payload: encodedJSONString(value)))
                    }
                )
                let slot: RuntimePublicationSlot<String> = runtime.newPublicationSlot()
                slotProbe.slot = slot
                cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
                let begin = Task { try await runtime.begin(deadline: Date().addingTimeInterval(2)) }
                let activation = try await begin.value
                begin.cancel()
                #expect(!activation.context.isCancelled)

                let client = try await SocketClient(
                    path: path,
                    wireBuild: "service.v1",
                    role: SessionPeerRole.unprotected
                )
                cleanup.add { await client.close() }
                let before = try await client.call(
                    operation: "work",
                    deadline: Date().addingTimeInterval(1)
                )
                #expect(before.rejected)
                #expect(before.code == .runtimeStarting)
                #expect(await handler.snapshot().isEmpty)

                let publication = try slot.stage(activation, value: "published")
                #expect(slot.load() == nil)
                try activation.commitReady(publication)
                #expect(slot.load() == "published")
                #expect(throws: RuntimeActivationError.self) {
                    try activation.commitReady(publication)
                }

                let after = try await client.call(
                    operation: "work",
                    deadline: Date().addingTimeInterval(1)
                )
                let expected = try JSONEncoder().encode("published")
                #expect(after.payload == expected)
                #expect(await handler.snapshot() == ["work"])
            }
        }

        @Test func admittedRequestKeepsPinnedValueAcrossDrain() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("pin.sock").path
                let entered = AsyncLatch()
                let release = AsyncLatch()
                let slotProbe = StringSlotProbe()
                let runtime = try DaemonRuntime(
                    path: path,
                    wireBuild: "service.v1",
                    identity: identity,
                    handler: RuntimeHandlerSpec { request in
                        let before = slotProbe.slot.loadPinned(from: request)
                        entered.finish()
                        await release.wait()
                        let after = slotProbe.slot.loadPinned(from: request)
                        return .terminal(SocketTerminal(
                            payload: encodedJSONString("\(before ?? "nil")/\(after ?? "nil")")
                        ))
                    }
                )
                let slot: RuntimePublicationSlot<String> = runtime.newPublicationSlot()
                slotProbe.slot = slot
                cleanup.add {
                    release.finish()
                    try? await runtime.shutdown(deadline: Date().addingTimeInterval(2))
                }
                let activation = try await runtime.begin(deadline: Date().addingTimeInterval(2))
                try activation.commitReady(slot.stage(activation, value: "stable"))
                let client = try await SocketClient(
                    path: path,
                    wireBuild: "service.v1",
                    role: SessionPeerRole.unprotected
                )
                cleanup.add { await client.close() }

                let admitted = Task {
                    try await client.call(operation: "work", deadline: Date().addingTimeInterval(2))
                }
                await entered.wait()
                try await runtime.closeIntake()
                #expect(slot.load() == nil)
                release.finish()
                let response = try await admitted.value
                let expected = try JSONEncoder().encode("stable/stable")
                #expect(response.payload == expected)

                let rejected = try await client.call(
                    operation: "later",
                    deadline: Date().addingTimeInterval(1)
                )
                #expect(rejected.rejected)
                #expect(rejected.code == .runtimeDraining)
            }
        }

        @Test func drainInvalidatesStagedPublicationAndCancelsActivation() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let runtime = try DaemonRuntime(
                    path: directory.appendingPathComponent("drain.sock").path,
                    wireBuild: "service.v1",
                    identity: identity,
                    handler: RuntimeHandlerSpec { _ in .terminal(SocketTerminal()) }
                )
                let slot: RuntimePublicationSlot<String> = runtime.newPublicationSlot()
                cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
                let activation = try await runtime.begin(deadline: Date().addingTimeInterval(2))
                let publication = try slot.stage(activation, value: "hidden")
                try await runtime.closeIntake()
                #expect(activation.context.isCancelled)
                #expect(slot.load() == nil)
                #expect(throws: RuntimeActivationError.staleActivation) {
                    try activation.commitReady(publication)
                }
            }
        }

        @Test func serverTerminationFencesCommitBeforeReadyPublication() async throws {
            let controller = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            let activation = try controller.beginActivation()
            let slot = RuntimePublicationSlot<String>(controller: controller)
            let publication = try slot.stage(activation, value: "orphan")
            controller.markServerTerminal()
            #expect(activation.context.isCancelled)
            #expect(controller.currentEvent().progress.state == .failed)
            #expect(throws: RuntimeActivationError.staleActivation) {
                try activation.commitReady(publication)
            }
            #expect(slot.load() == nil)
            guard case let .failed(cause) = await controller.wait() else {
                Issue.record("server termination did not fail runtime")
                return
            }
            #expect(cause is RuntimeServerTerminatedError)
        }

        @Test func serverTerminalNormalizationIsStateDependent() async throws {
            let ready = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            let readyActivation = try ready.beginActivation()
            let readySlot = RuntimePublicationSlot<String>(controller: ready)
            try readyActivation.commitReady(readySlot.stage(readyActivation, value: "ready"))
            guard case let .failed(readyCause) = ready.markServerTerminal(TestUnexpectedServerFailure.failed)
            else {
                Issue.record("unexpected Ready exit did not fail")
                return
            }
            #expect(readyCause is TestUnexpectedServerFailure)
            #expect(ready.currentEvent().progress.state == .failed)

            let cleanDrain = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            _ = try cleanDrain.beginActivation()
            try await cleanDrain.closeIntake()
            guard case .draining = cleanDrain.markServerTerminal() else {
                Issue.record("clean server exit after Drain was not clean")
                return
            }

            let cancelledDrain = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            _ = try cancelledDrain.beginActivation()
            try await cancelledDrain.closeIntake()
            guard case .draining = cancelledDrain.markServerTerminal(CancellationError()) else {
                Issue.record("cancelled server exit after Drain was not clean")
                return
            }

            let brokenDrain = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            _ = try brokenDrain.beginActivation()
            try await brokenDrain.closeIntake()
            guard case let .failed(drainCause) = brokenDrain.markServerTerminal(TestUnexpectedServerFailure.failed)
            else {
                Issue.record("unexpected server error after Drain was discarded")
                return
            }
            #expect(drainCause is TestUnexpectedServerFailure)
            guard case let .failed(retainedDrainCause) = await brokenDrain.wait() else {
                Issue.record("unexpected server error after Drain was not retained")
                return
            }
            #expect(retainedDrainCause is TestUnexpectedServerFailure)
            #expect(brokenDrain.currentEvent().progress.state == .draining)

            let failed = try liveRuntimeController(
                wireBuild: "service.v1",
                runtimeIdentity: identity
            )
            let failedActivation = try failed.beginActivation()
            try await failedActivation.fail(TestRuntimeFailure.failed)
            guard case let .failed(originalCause) = failed.markServerTerminal(TestUnexpectedServerFailure.failed)
            else {
                Issue.record("Failed runtime lost its result at server exit")
                return
            }
            #expect(originalCause is TestRuntimeFailure)
        }
    }
}

extension SocketTransportTests.RuntimeLifecycleControllerTests {
    @Test func swiftRuntimeOwnsProtectedReadinessBeforeHandler() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("server-terminal.sock").path
            let handler = LifecycleHandlerProbe()
            let runtime = try DaemonRuntime(
                path: path,
                wireBuild: "service.v1",
                identity: identity,
                handler: RuntimeHandlerSpec { request in await handler.handle(request) }
            )
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            _ = try await runtime.begin(deadline: Date().addingTimeInterval(2))
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "readiness-controller.v1"
            )
            cleanup.add { await client.close() }
            let terminal = try await client.call(
                operation: runtimeReadinessSubscribeOperation,
                payload: RuntimeReadinessCodec.encodeSubscribe(),
                deadline: Date().addingTimeInterval(2)
            )
            #expect(!terminal.rejected)
            let expected = try RuntimeReadinessCodec.encodeSubscribe()
            #expect(terminal.payload == expected)
            #expect(await handler.snapshot().isEmpty)
        }
    }

    @Test func duplicateReadinessCannotReplaceTerminalSettlementOwner() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("duplicate-readiness.sock").path
            let activationGate = OneShotRegistrationGate()
            let registrationGate = SecondRegistrationGate()
            let handler = LifecycleHandlerProbe()
            let runtime = try DaemonRuntime(
                path: path,
                wireBuild: "service.v1",
                identity: identity,
                handler: RuntimeHandlerSpec { request in await handler.handle(request) }
            )
            runtime.controller.registrationAttemptHook = { registrationGate.enter() }
            runtime.controller.activationHook = { activationGate.register() }
            cleanup.add {
                registrationGate.release()
                activationGate.release()
                try? await runtime.shutdown(deadline: Date().addingTimeInterval(2))
            }
            _ = try await runtime.begin(deadline: Date().addingTimeInterval(2))
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "readiness-controller.v1"
            )
            cleanup.add { await client.close() }

            let first = try await client.call(
                operation: runtimeReadinessSubscribeOperation,
                payload: RuntimeReadinessCodec.encodeSubscribe(),
                deadline: Date().addingTimeInterval(2)
            )
            #expect(!first.rejected)
            await activationGate.firstEntered.wait()

            let duplicateCall = Task {
                try await client.call(
                    operation: runtimeReadinessSubscribeOperation,
                    payload: RuntimeReadinessCodec.encodeSubscribe(),
                    deadline: Date().addingTimeInterval(2)
                )
            }
            await registrationGate.secondEntered.wait()

            let drain = Task { try await runtime.controller.closeIntake() }
            while runtime.controller.currentEvent().progress.state != .draining {
                await Task.yield()
            }
            registrationGate.release()
            let duplicate = try await duplicateCall.value
            #expect(duplicate.rejected)
            #expect(duplicate.code == .readinessSubscriptionExists)
            #expect(duplicate.reason == "wire: readiness subscription already registered")
            #expect(runtime.controller.subscriberCount == 1)
            #expect(await handler.snapshot().isEmpty)

            activationGate.release()
            try await drain.value
        }
    }

    @Test func activeReadinessSubscriptionRejectsDuplicateWithoutReactivation() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("active-duplicate-readiness.sock").path
            let handler = LifecycleHandlerProbe()
            let runtime = try DaemonRuntime(
                path: path,
                wireBuild: "service.v1",
                identity: identity,
                handler: RuntimeHandlerSpec { request in await handler.handle(request) }
            )
            let activations = ActivationCounter()
            runtime.controller.activationHook = { activations.increment() }
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            _ = try await runtime.begin(deadline: Date().addingTimeInterval(2))
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: "readiness-controller.v1"
            )
            cleanup.add { await client.close() }

            let first = try await client.call(
                operation: runtimeReadinessSubscribeOperation,
                payload: RuntimeReadinessCodec.encodeSubscribe(),
                deadline: Date().addingTimeInterval(2)
            )
            #expect(!first.rejected)
            let duplicate = try await client.call(
                operation: runtimeReadinessSubscribeOperation,
                payload: RuntimeReadinessCodec.encodeSubscribe(),
                deadline: Date().addingTimeInterval(2)
            )
            #expect(duplicate.rejected)
            #expect(duplicate.code == .readinessSubscriptionExists)
            #expect(duplicate.reason == "wire: readiness subscription already registered")
            #expect(activations.value == 1)
            #expect(runtime.controller.subscriberCount == 1)
            #expect(await handler.snapshot().isEmpty)
        }
    }

    @Test func failureRetainsCauseLocallyAndLastExplicitDetailOnWire() async throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        try activation.updateProgress(Data("loading".utf8))
        try await activation.fail(TestRuntimeFailure.failed)
        #expect(activation.context.isCancelled)
        let event = controller.currentEvent()
        #expect(event.progress.state == .failed)
        #expect(event.progress.detail == Data("loading".utf8))
        guard case let .failed(cause) = await controller.wait() else {
            Issue.record("failure did not retain a local cause")
            return
        }
        #expect(cause is TestRuntimeFailure)
    }

    @Test func failAndDrainHaveOneAtomicTerminalWinner() async throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        async let failure = captureRuntimeResult {
            try await activation.fail(TestRuntimeFailure.failed)
        }
        async let drainage = captureRuntimeResult {
            try await controller.closeIntake()
        }
        let (failureResult, drainageResult) = await (failure, drainage)
        #expect(activation.context.isCancelled)
        let event = controller.currentEvent()
        switch event.progress.state {
        case .failed:
            if case .failure = failureResult {
                Issue.record("winning failure was rejected")
            }
            if case .failure = drainageResult {
                Issue.record("drain after failure was not idempotent")
            }
            guard case .failed = await controller.wait() else {
                Issue.record("failed lifecycle lost its local cause")
                return
            }
        case .draining:
            if case .success = failureResult {
                Issue.record("failure committed after drain")
            }
            if case .failure = drainageResult {
                Issue.record("winning drain was rejected")
            }
            guard case .draining = await controller.wait() else {
                Issue.record("draining lifecycle retained the wrong terminal")
                return
            }
        default:
            Issue.record("concurrent terminal transition left runtime nonterminal")
        }
    }

    @Test func statusReporterCopiesValidatesAndDoesNotAdvanceLifecycle() throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        let reporter = activation.statusReporter()
        var detail = Data("healthy".utf8)
        try reporter.update(HealthStatus(state: .healthy, detail: detail))
        detail[detail.startIndex] = 0x78
        let first = controller.statusSnapshot()
        #expect(first.health == HealthStatus(state: .healthy, detail: Data("healthy".utf8)))
        #expect(controller.currentEvent().progress.sequence == 1)

        try reporter.update(HealthStatus(state: .healthy, detail: Data("healthy".utf8)))
        #expect(controller.currentEvent().progress.sequence == 1)
        #expect(State(rawValue: "unknown") == nil)
        #expect(throws: RuntimeReadinessValidationError.self) {
            try reporter.update(HealthStatus(state: .degraded, detail: Data(repeating: 1, count: 4097)))
        }
    }

    @Test func activitiesAreReadyOnlyIdempotentAndForceReleasedAtTerminal() async throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        let reporter = activation.statusReporter()
        #expect(throws: RuntimeActivationError.runtimeNotReady) {
            _ = try reporter.beginActivity()
        }
        let slot = RuntimePublicationSlot<Void>(controller: controller)
        try activation.commitReady(slot.stage(activation, value: ()))
        let first = try reporter.beginActivity()
        let second = try reporter.beginActivity()
        #expect(controller.statusSnapshot().busy)
        #expect(controller.statusSnapshot().activities == 2)
        try first.release()
        try first.release()
        #expect(controller.statusSnapshot().activities == 1)

        try await controller.closeIntake()
        #expect(controller.statusSnapshot().activities == 0)
        #expect(throws: RuntimeActivationError.publicationStale) {
            try reporter.update(HealthStatus(state: .failed))
        }
        try second.release()
        try second.release()
    }

    @Test func statusReporterIsRuntimeGenerationFenced() throws {
        let first = try liveRuntimeController(wireBuild: "service.v1", runtimeIdentity: identity)
        let second = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: testOwnerGeneration(2))
        )
        let reporter = try first.beginActivation().statusReporter()
        let wrong = StatusReporter(controller: second, activationID: reporter.activationID)
        #expect(throws: RuntimeActivationError.publicationStale) {
            try wrong.update(HealthStatus(state: .healthy))
        }
    }

    @Test func publicationIsRuntimeAndGenerationFenced() throws {
        let first = try liveRuntimeController(wireBuild: "service.v1", runtimeIdentity: identity)
        let second = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: testOwnerGeneration(2))
        )
        let firstActivation = try first.beginActivation()
        let secondActivation = try second.beginActivation()
        let firstSlot = RuntimePublicationSlot<String>(controller: first)
        let publication = try firstSlot.stage(firstActivation, value: "first")
        #expect(throws: RuntimeActivationError.staleActivation) {
            _ = try firstSlot.stage(secondActivation, value: "wrong")
        }
        #expect(throws: RuntimeActivationError.invalidPublication) {
            try secondActivation.commitReady(publication)
        }
        try firstActivation.commitReady(publication)
        #expect(firstSlot.load() == "first")
    }

    @Test func publicationSlotPreservesVisibleTypedNil() throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        let slot = RuntimePublicationSlot<Int?>(controller: controller)
        try activation.commitReady(slot.stage(activation, value: nil))
        let loaded = slot.load()
        guard case .some(.none) = loaded else {
            Issue.record("typed nil publication was not present")
            return
        }
    }

    @Test func finalSequenceIsReservedForTerminalInsteadOfReady() async throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        let slot = RuntimePublicationSlot<String>(controller: controller)
        let publication = try slot.stage(activation, value: "never-visible")
        controller.setStartingSequenceForTesting(.max - 1)

        #expect(throws: RuntimeLifecycleSequenceExhaustedError.self) {
            try activation.commitReady(publication)
        }
        #expect(activation.context.isCancelled)
        #expect(slot.load() == nil)
        let event = controller.currentEvent()
        #expect(event.progress.sequence == .max)
        #expect(event.progress.state == .failed)
        guard case let .failed(cause) = await controller.wait() else {
            Issue.record("sequence exhaustion did not retain terminal failure")
            return
        }
        #expect(cause is RuntimeLifecycleSequenceExhaustedError)
        guard case let .rejected(code, _) = controller.admitBusiness() else {
            Issue.record("sequence exhaustion left business admission open")
            return
        }
        #expect(code == .runtimeDraining)
    }

    @Test func alreadyMaxSequenceSealsStageWithoutWrappingToZero() async throws {
        let controller = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let activation = try controller.beginActivation()
        let slot = RuntimePublicationSlot<String>(controller: controller)
        controller.setStartingSequenceForTesting(.max)
        #expect(throws: RuntimeLifecycleSequenceExhaustedError.self) {
            _ = try slot.stage(activation, value: "never-staged")
        }
        #expect(activation.context.isCancelled)
        let event = controller.currentEvent()
        #expect(event.progress.sequence == .max)
        #expect(event.progress.state == .failed)
        guard case .failed = await controller.wait() else {
            Issue.record("already-max sequence did not terminalize")
            return
        }
    }
}

extension SocketTransportTests.RuntimeLifecycleControllerTests {
    @Test func maxMinusOneStillAllowsOneTerminalTransition() async throws {
        let failureController = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: identity
        )
        let failureActivation = try failureController.beginActivation()
        failureController.setStartingSequenceForTesting(.max - 1)
        try await failureActivation.fail(TestRuntimeFailure.failed)
        #expect(failureController.currentEvent().progress.sequence == .max)
        #expect(failureController.currentEvent().progress.state == .failed)

        let drainController = try liveRuntimeController(
            wireBuild: "service.v1",
            runtimeIdentity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: testOwnerGeneration(2))
        )
        let drainActivation = try drainController.beginActivation()
        drainController.setStartingSequenceForTesting(.max - 1)
        try await drainController.closeIntake()
        #expect(drainActivation.context.isCancelled)
        #expect(drainController.currentEvent().progress.sequence == .max)
        #expect(drainController.currentEvent().progress.state == .draining)
    }

    @Test func requestPublicationLeaseIsRevokedAtExactSettlement() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("lease.sock").path
            let probe = RetainedRequestProbe()
            let revoked = AsyncLatch()
            let runtime = try DaemonRuntime(
                path: path,
                wireBuild: "service.v1",
                identity: identity,
                handler: RuntimeHandlerSpec { request in await probe.retain(request) }
            )
            runtime.controller.admissionRevocationHook = { revoked.finish() }
            let slot: RuntimePublicationSlot<String> = runtime.newPublicationSlot()
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            let activation = try await runtime.begin(deadline: Date().addingTimeInterval(2))
            try activation.commitReady(slot.stage(activation, value: "lease"))
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await client.close() }

            let response = try await client.call(
                operation: "work",
                deadline: Date().addingTimeInterval(1)
            )
            #expect(response.payload == Data("true".utf8))
            await revoked.wait()
            #expect(await probe.pinnedValue(in: slot) == nil)
            #expect(runtime.statusSnapshot().admissions == 0)
            #expect(!runtime.statusSnapshot().busy)
        }
    }

    @Test func invalidHandlerPayloadSettlesWithFallbackInsteadOfOrphaningCall() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("invalid-response.sock").path
            let server = SocketServer(
                path: path,
                wireBuild: "service.v1"
            ) { _ in
                .terminal(SocketTerminal(payload: Data("not-json".utf8)))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await client.close() }

            let response = try await client.call(
                operation: "invalid",
                deadline: Date().addingTimeInterval(1)
            )
            #expect(response.error == "wire: invalid handler response")
        }
    }

    @Test func incompleteShutdownRetainsPublicationAndListenerOwnership() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("incomplete.sock").path
        let probe = BlockingRequestProbe()
        let runtime = try DaemonRuntime(
            path: path,
            wireBuild: "service.v1",
            identity: identity,
            handler: RuntimeHandlerSpec { request in await probe.handle(request) }
        )
        let slot: RuntimePublicationSlot<String> = runtime.newPublicationSlot()
        let activation = try await runtime.begin(deadline: Date().addingTimeInterval(2))
        try activation.commitReady(slot.stage(activation, value: "retained"))
        let client = try await SocketClient(
            path: path,
            wireBuild: "service.v1",
            role: SessionPeerRole.unprotected
        )
        let call = Task {
            try await client.call(operation: "hang", deadline: Date().addingTimeInterval(2))
        }
        await probe.entered.wait()
        #expect(runtime.statusSnapshot().admissions == 1)
        #expect(runtime.statusSnapshot().busy)

        await #expect(throws: RuntimeShutdownError.deadlineExceeded) {
            try await runtime.shutdown(deadline: Date().addingTimeInterval(0.02))
        }
        #expect(await probe.pinnedValue(in: slot) == "retained")
        #expect(runtime.statusSnapshot().admissions == 1)
        let successor = SocketServer(
            path: path,
            wireBuild: "service.v1"
        ) { _ in .terminal(SocketTerminal(payload: Data("true".utf8))) }
        await #expect(throws: SocketServerError.self) {
            try await successor.start()
        }
        await #expect(throws: RuntimeActivationError.activationAlreadyIssued) {
            _ = try await runtime.begin(deadline: Date().addingTimeInterval(2))
        }

        probe.release.finish()
        await client.close()
        _ = await call.result
    }

    @Test func swiftRuntimeOwnsProtectedRuntimeReceiptBeforeHandler() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("reserved.sock").path
            let handler = LifecycleHandlerProbe()
            let runtime = try DaemonRuntime(
                path: path,
                wireBuild: "service.v1",
                identity: identity,
                handler: RuntimeHandlerSpec { request in await handler.handle(request) }
            )
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            _ = try await runtime.begin(deadline: Date().addingTimeInterval(2))
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await client.close() }
            let receipt = try await client.acquireRuntimeReceipt(
                expectedRuntimeBuild: "app.v1",
                deadline: Date().addingTimeInterval(1)
            )
            #expect(receipt.runtimeIdentity == identity)
            #expect(await handler.snapshot().isEmpty)
        }
    }

    @Test func protectedControlsRejectStreamedInputBeforeAdmission() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("control-unary.sock").path
            let runtime = try DaemonRuntime(
                path: path,
                wireBuild: "service.v1",
                identity: identity,
                handler: RuntimeHandlerSpec { _ in .terminal(SocketTerminal()) }
            )
            cleanup.add { try? await runtime.shutdown(deadline: Date().addingTimeInterval(2)) }
            _ = try await runtime.begin(deadline: Date().addingTimeInterval(2))
            let client = try await SocketClient(
                path: path,
                wireBuild: "service.v1",
                role: SessionPeerRole.unprotected
            )
            cleanup.add { await client.close() }

            for operation in [runtimeReceiptOperation, runtimeReadinessSubscribeOperation] {
                let streamed = try await client.open(
                    operation: operation,
                    payload: RuntimeReadinessCodec.encodeSubscribe(),
                    endInput: false
                )
                let terminal = try await streamed.response()
                #expect(terminal.rejected)
                #expect(terminal.code == .invalidRequest)
            }
            #expect(runtime.controller.subscriberCount == 0)

            let receipt = try await client.acquireRuntimeReceipt(
                expectedRuntimeBuild: "app.v1",
                deadline: Date().addingTimeInterval(1)
            )
            #expect(receipt.runtimeIdentity == identity)
        }
    }
}
