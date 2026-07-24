@testable import DaemonKit
import Foundation
import Testing

private final class TestReadinessClock: @unchecked Sendable {
    private let lock = NSLock()
    private var nanoseconds: UInt64 = 0

    var clock: RuntimeReadinessClock {
        RuntimeReadinessClock { self.lock.withLock { self.nanoseconds } }
    }

    func advance(seconds: TimeInterval) {
        lock.withLock {
            nanoseconds += UInt64(seconds * 1_000_000_000)
        }
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct RuntimeReadinessTests {
        @Test func wireSchemaUsesExactSubscriptionAndNestedLifecycleSnapshot() throws {
            let request = try RuntimeReadinessCodec.encodeSubscribe()
            #expect(String(data: request, encoding: .utf8) == #"{"protocol":1}"#)
            try RuntimeReadinessCodec.decodeSubscribeAck(request)

            let valid = #"{"protocol":1,"wire_build":"suite.v1","# +
                #""runtime_identity":{"runtime_build":"app.v1","process_generation":"boot-1"},"# +
                #""progress":{"sequence":1,"state":"runtime_starting","detail":""}}"#
            let event = try RuntimeReadinessCodec.decodeEvent(Data(valid.utf8))
            #expect(event.runtimeIdentity == RuntimeIdentity(
                runtimeBuild: "app.v1",
                processGeneration: "boot-1"
            ))
            #expect(event.progress == ReadinessProgress(sequence: 1, state: .starting, detail: Data()))

            #expect(throws: RuntimeReadinessValidationError.self) {
                let legacy = #"{"protocol":1,"wire_build":"suite.v1","# +
                    #""runtime_identity":{"runtime_build":"app.v1","process_generation":"boot-1"},"# +
                    #""progress":{"sequence":1,"state":"runtime_starting","detail":""},"legacy":true}"#
                _ = try RuntimeReadinessCodec.decodeEvent(Data(legacy.utf8))
            }
        }

        @Test func initialSnapshotDoesNotResetButLaterProgressDoes() throws {
            let clock = TestReadinessClock()
            let tracker = makeTracker(clock: clock)
            clock.advance(seconds: 9)
            #expect(try !tracker.observe(event(sequence: 1), allowSuccessor: false))
            #expect(tracker.deadlineNanoseconds == 10_000_000_000)

            clock.advance(seconds: 0.5)
            #expect(try !tracker.observe(
                event(sequence: 3, detail: "gap"),
                allowSuccessor: false
            ))
            #expect(tracker.deadlineNanoseconds == 19_500_000_000)
            clock.advance(seconds: 8.5)
            #expect(try tracker.observe(
                event(sequence: 4, state: .ready),
                allowSuccessor: false
            ))
        }

        @Test func unchangedMutationRegressionAndLateProgressAreExact() throws {
            let clock = TestReadinessClock()
            let tracker = makeTracker(expected: RuntimeIdentity(
                runtimeBuild: "app.v1",
                processGeneration: "boot-1"
            ), clock: clock)
            let initial = event(sequence: 2, detail: "one")
            _ = try tracker.observe(initial, allowSuccessor: false)
            clock.advance(seconds: 9)
            _ = try tracker.observe(initial, allowSuccessor: false)
            #expect(throws: RuntimeReadinessValidationError.sequenceMutation(2)) {
                _ = try tracker.observe(
                    event(sequence: 2, detail: "changed"),
                    allowSuccessor: false
                )
            }
            #expect(throws: RuntimeReadinessValidationError.sequenceRegression(got: 1, previous: 2)) {
                _ = try tracker.observe(
                    event(sequence: 1, detail: "old"),
                    allowSuccessor: false
                )
            }
            clock.advance(seconds: 1)
            #expect(throws: ReadinessNoProgressError.self) {
                _ = try tracker.observe(
                    event(sequence: 3, detail: "late"),
                    allowSuccessor: false
                )
            }
        }

        @Test func failedDrainingAndSuccessorIdentityAreTyped() throws {
            let failed = makeTracker()
            #expect(throws: RuntimeFailedError.self) {
                _ = try failed.observe(
                    event(sequence: 1, state: .failed, detail: "boom"),
                    allowSuccessor: false
                )
            }

            let draining = makeTracker()
            #expect(throws: RuntimeReadinessValidationError.draining(RuntimeLifecycleSnapshot(
                identity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: "boot-1"),
                progress: ReadinessProgress(sequence: 1, state: .draining, detail: Data("drain".utf8))
            ))) {
                _ = try draining.observe(
                    event(sequence: 1, state: .draining, detail: "drain"),
                    allowSuccessor: false
                )
            }

            let successorClock = TestReadinessClock()
            let successor = makeTracker(clock: successorClock)
            _ = try successor.observe(event(sequence: 4), allowSuccessor: true)
            successorClock.advance(seconds: 9)
            _ = try successor.observe(
                event(sequence: 1, processGeneration: "boot-2"),
                allowSuccessor: true
            )
            #expect(successor.snapshot?.identity.processGeneration == "boot-2")
            #expect(successor.deadlineNanoseconds == 10_000_000_000)

            let pinned = makeTracker(expected: RuntimeIdentity(
                runtimeBuild: "app.v1",
                processGeneration: "boot-1"
            ))
            _ = try pinned.observe(event(sequence: 1), allowSuccessor: false)
            #expect(throws: RuntimeReadinessValidationError.runtimeIdentity(
                got: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: "boot-2"),
                want: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: "boot-1")
            )) {
                _ = try pinned.observe(
                    event(sequence: 1, processGeneration: "boot-2"),
                    allowSuccessor: false
                )
            }
        }

        @Test func lifecycleTransitionsAreClosedAcrossCoalescedSequenceGaps() throws {
            let starting = makeTracker()
            _ = try starting.observe(event(sequence: 1), allowSuccessor: false)
            #expect(try starting.observe(
                event(sequence: 9, state: .ready),
                allowSuccessor: false
            ))

            let ready = makeTracker()
            _ = try ready.observe(
                event(sequence: 1, state: .ready),
                allowSuccessor: false
            )
            #expect(throws: RuntimeReadinessValidationError.invalidResponse(
                "runtime lifecycle transition runtime_ready -> runtime_starting"
            )) {
                _ = try ready.observe(
                    event(sequence: 7, state: .starting),
                    allowSuccessor: false
                )
            }
            #expect(throws: RuntimeReadinessValidationError.draining(RuntimeLifecycleSnapshot(
                identity: RuntimeIdentity(runtimeBuild: "app.v1", processGeneration: "boot-1"),
                progress: ReadinessProgress(sequence: 8, state: .draining, detail: Data())
            ))) {
                _ = try ready.observe(
                    event(sequence: 8, state: .draining),
                    allowSuccessor: false
                )
            }

            let failed = makeTracker()
            #expect(throws: RuntimeFailedError.self) {
                _ = try failed.observe(
                    event(sequence: 2, state: .failed),
                    allowSuccessor: false
                )
            }
            #expect(throws: RuntimeReadinessValidationError.invalidResponse(
                "runtime lifecycle advanced after terminal state runtime_failed"
            )) {
                _ = try failed.observe(
                    event(sequence: 11, state: .ready),
                    allowSuccessor: false
                )
            }
        }

        @Test func monotonicSemanticProgressExtendsButHeartbeatDoesNotAcross75Seconds() throws {
            let progressClock = TestReadinessClock()
            let progressing = makeTracker(timeout: 60, clock: progressClock)
            let initial = event(sequence: 1)
            _ = try progressing.observe(initial, allowSuccessor: false)
            progressClock.advance(seconds: 50)
            _ = try progressing.observe(event(sequence: 4, detail: "advanced"), allowSuccessor: false)
            progressClock.advance(seconds: 25)
            try progressing.checkDeadline()

            let heartbeatClock = TestReadinessClock()
            let heartbeat = makeTracker(timeout: 60, clock: heartbeatClock)
            _ = try heartbeat.observe(initial, allowSuccessor: false)
            heartbeatClock.advance(seconds: 50)
            _ = try heartbeat.observe(initial, allowSuccessor: false)
            heartbeatClock.advance(seconds: 25)
            #expect(throws: ReadinessNoProgressError.self) {
                try heartbeat.checkDeadline()
            }
        }

        @Test func clientSequenceValidatorRejectsInvalidFramesBeforeCoalescing() throws {
            let validator = RuntimeLifecycleSequenceValidator()
            let starting = try JSONEncoder().encode(event(sequence: 1))
            let ready = try JSONEncoder().encode(event(sequence: 5, state: .ready))
            #expect(try validator.accept(starting))
            #expect(try !validator.accept(starting))
            #expect(try validator.accept(ready))
            #expect(throws: RuntimeReadinessValidationError.invalidResponse(
                "runtime lifecycle transition runtime_ready -> runtime_starting"
            )) {
                try validator.accept(JSONEncoder().encode(event(sequence: 9)))
            }
        }

        private func makeTracker(
            expected: RuntimeIdentity? = nil,
            timeout: TimeInterval = 10,
            clock: TestReadinessClock = TestReadinessClock()
        ) -> RuntimeProgressTracker {
            RuntimeProgressTracker(
                wireBuild: "suite.v1",
                expected: expected,
                noProgressTimeout: timeout,
                clock: clock.clock
            )
        }

        private func event(
            sequence: UInt64,
            state: RuntimeReadinessState = .starting,
            processGeneration: String = "boot-1",
            detail: String = ""
        ) -> RuntimeReadinessEvent {
            RuntimeReadinessEvent(
                protocolVersion: daemonKitSessionProtocolVersion,
                wireBuild: "suite.v1",
                runtimeIdentity: RuntimeIdentity(
                    runtimeBuild: "app.v1",
                    processGeneration: processGeneration
                ),
                progress: ReadinessProgress(
                    sequence: sequence,
                    state: state,
                    detail: Data(detail.utf8)
                )
            )
        }
    }
}
