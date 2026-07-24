@testable import DaemonKit
import Darwin
import Foundation
import Testing

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct ServerLifecyclePublisherTests {
        @Test func blockedWriteCoalescesToLatestWithoutBlockingPublisher() async throws {
            var descriptors: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
            defer {
                Darwin.close(descriptors[0])
                Darwin.close(descriptors[1])
            }
            var sendBuffer: Int32 = 4096
            setsockopt(
                descriptors[0],
                SOL_SOCKET,
                SO_SNDBUF,
                &sendBuffer,
                socklen_t(MemoryLayout<Int32>.size)
            )
            let firstStarted = AsyncLatch()
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 2),
                maximumPendingWrites: 2,
                label: "lifecycle-coalescing-test",
                startHook: { frame in
                    if frame.kind == .stream {
                        firstStarted.finish()
                    }
                }
            )
            let publisher = ServerLifecyclePublisher(
                wireBuild: "service.v1",
                submit: { payload in
                    writer.enqueueLifecycle(SessionFrame(kind: .lifecycle, flags: .end, payload: payload))
                },
                closeSession: { Issue.record("healthy lifecycle publisher closed") }
            )
            let blocking = Task {
                try await writer.write(SessionFrame(
                    kind: .stream,
                    id: 1,
                    payload: Data(repeating: 0xA5, count: 512 * 1024)
                ))
            }
            await firstStarted.wait()

            _ = try publisher.publish(payload(sequence: 1, state: .starting))
            _ = try publisher.publish(payload(sequence: 2, state: .starting))
            _ = try publisher.publish(payload(sequence: 7, state: .ready))

            let readDescriptor = descriptors[1]
            let frames = try await DispatchQueue(label: "lifecycle-coalescing-read").performIO {
                let codec = SessionFrameCodec(descriptor: readDescriptor)
                return try (0 ..< 2).map { _ in try codec.read(timeout: 2) }
            }
            try await blocking.value
            #expect(frames.map(\.kind) == [.stream, .lifecycle])
            let latest = try RuntimeReadinessCodec.decodeEvent(frames[1].payload)
            #expect(latest.progress.sequence == 7)
            #expect(latest.progress.state == .ready)
            publisher.finish()
            writer.abort()
            await writer.drain()
        }

        @Test func nonReadingSessionTimesOutWithoutClosingHealthySession() async throws {
            var slow: [Int32] = [-1, -1]
            var healthy: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &slow) == 0)
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &healthy) == 0)
            defer {
                slow.forEach { Darwin.close($0) }
                healthy.forEach { Darwin.close($0) }
            }
            var sendBuffer: Int32 = 4096
            let slowFlags = fcntl(slow[0], F_GETFL)
            try #require(slowFlags >= 0)
            try #require(fcntl(slow[0], F_SETFL, slowFlags | O_NONBLOCK) == 0)
            setsockopt(
                slow[0],
                SOL_SOCKET,
                SO_SNDBUF,
                &sendBuffer,
                socklen_t(MemoryLayout<Int32>.size)
            )
            let slowWriteStarted = AsyncLatch()
            let slowClosed = AsyncLatch()
            let slowWriter = SessionWriter(
                codec: SessionFrameCodec(descriptor: slow[0], writeTimeout: 0.05),
                maximumPendingWrites: 2,
                label: "slow-lifecycle-test",
                startHook: { frame in
                    if frame.kind == .stream {
                        slowWriteStarted.finish()
                    }
                }
            )
            let healthyWriter = SessionWriter(
                codec: SessionFrameCodec(descriptor: healthy[0], writeTimeout: 1),
                maximumPendingWrites: 2,
                label: "healthy-lifecycle-test"
            )
            let slowPublisher = ServerLifecyclePublisher(
                wireBuild: "service.v1",
                submit: { payload in
                    slowWriter.enqueueLifecycle(SessionFrame(kind: .lifecycle, flags: .end, payload: payload))
                },
                closeSession: { slowClosed.finish() }
            )
            let healthyPublisher = ServerLifecyclePublisher(
                wireBuild: "service.v1",
                submit: { payload in
                    healthyWriter.enqueueLifecycle(SessionFrame(kind: .lifecycle, flags: .end, payload: payload))
                },
                closeSession: { Issue.record("healthy lifecycle session closed") }
            )
            let blocking = Task {
                try await slowWriter.write(SessionFrame(
                    kind: .stream,
                    id: 1,
                    payload: Data(repeating: 0xA5, count: 512 * 1024)
                ))
            }
            await slowWriteStarted.wait()

            _ = try slowPublisher.publish(payload(sequence: 1, state: .starting))
            _ = try healthyPublisher.publish(payload(sequence: 1, state: .starting))
            let healthyCodec = SessionFrameCodec(descriptor: healthy[1])
            let first = try healthyCodec.read(timeout: 1)
            #expect(first.kind == .lifecycle)
            _ = try healthyPublisher.publish(payload(sequence: 2, state: .starting))
            let second = try healthyCodec.read(timeout: 1)
            #expect(try RuntimeReadinessCodec.decodeEvent(second.payload).progress.sequence == 2)

            await slowClosed.wait()
            await #expect(throws: SessionTransportError.self) { try await blocking.value }
            #expect(throws: SessionTransportError.disconnected) {
                _ = try slowPublisher.publish(payload(sequence: 2, state: .draining))
            }
            slowWriter.abort()
            healthyWriter.abort()
            await slowWriter.drain()
            await healthyWriter.drain()
        }

        @Test func selectedLifecycleIsReplacedBeforeCodecWrite() async throws {
            var descriptors: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
            defer {
                Darwin.close(descriptors[0])
                Darwin.close(descriptors[1])
            }
            let ordinaryStarted = AsyncLatch()
            let releaseOrdinary = DispatchSemaphore(value: 0)
            let lifecycleSelected = AsyncLatch()
            let releaseSelection = DispatchSemaphore(value: 0)
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 1),
                maximumPendingWrites: 2,
                label: "lifecycle-selection-race-test",
                startHook: { frame in
                    if frame.kind == .stream {
                        ordinaryStarted.finish()
                        releaseOrdinary.wait()
                    }
                },
                selectionHook: { frame in
                    if frame.kind == .lifecycle, !lifecycleSelected.isFinished {
                        lifecycleSelected.finish()
                        releaseSelection.wait()
                    }
                }
            )
            let publisher = ServerLifecyclePublisher(
                wireBuild: "service.v1",
                submit: { payload in
                    writer.enqueueLifecycle(SessionFrame(kind: .lifecycle, flags: .end, payload: payload))
                },
                closeSession: { Issue.record("lifecycle selection race closed session") }
            )
            let ordinary = Task {
                try await writer.write(SessionFrame(kind: .stream, id: 1, payload: Data("ordinary".utf8)))
            }
            await ordinaryStarted.wait()
            _ = try publisher.publish(payload(sequence: 1, state: .starting))
            releaseOrdinary.signal()
            await lifecycleSelected.wait()

            _ = try publisher.publish(payload(sequence: 2, state: .ready))
            releaseSelection.signal()

            let readDescriptor = descriptors[1]
            let frames = try await DispatchQueue(label: "lifecycle-selection-race-read").performIO {
                let codec = SessionFrameCodec(descriptor: readDescriptor)
                return try (0 ..< 2).map { _ in try codec.read(timeout: 1) }
            }
            try await ordinary.value
            #expect(frames.map(\.kind) == [.stream, .lifecycle])
            let event = try RuntimeReadinessCodec.decodeEvent(frames[1].payload)
            #expect(event.progress.sequence == 2)
            #expect(event.progress.state == .ready)
            writer.abort()
            await writer.drain()
        }

        @Test func terminalSnapshotIsSticky() throws {
            let publisher = ServerLifecyclePublisher(
                wireBuild: "service.v1",
                write: { _ in },
                closeSession: { Issue.record("terminal publisher closed") }
            )
            _ = try publisher.publish(payload(sequence: 1, state: .starting))
            let terminal = try #require(try publisher.publish(payload(sequence: 2, state: .failed)))
            let duplicate = try #require(try publisher.publish(payload(sequence: 2, state: .failed)))
            #expect(terminal === duplicate)
            #expect(throws: RuntimeReadinessValidationError.invalidResponse(
                "runtime lifecycle advanced after terminal state runtime_failed"
            )) {
                _ = try publisher.publish(payload(sequence: 9, state: .ready))
            }
            publisher.finish()
        }

        @Test func receiptWaitHonorsAbsoluteDeadlineWithoutWaitingForWriterTimeout() async throws {
            let receipt = LifecycleWriteReceipt()
            let started = Date()
            await #expect(throws: RuntimeShutdownError.deadlineExceeded) {
                try await receipt.wait(deadline: started.addingTimeInterval(0.02))
            }
            #expect(Date().timeIntervalSince(started) < 0.5)

            receipt.finish(.success(()))
            try await receipt.wait(deadline: Date().addingTimeInterval(0.02))
        }

        private func payload(sequence: UInt64, state: RuntimeReadinessState) -> Data {
            let json = #"{"progress":{"detail":"","sequence":\#(sequence),"state":"\#(state.rawValue)"},"protocol":1,"# +
                #""runtime_identity":{"process_generation":"boot-1","runtime_build":"app.v1"},"wire_build":"service.v1"}"#
            return Data(json.utf8)
        }
    }
}
