@testable import DaemonKit
import Darwin
import Foundation
import Testing

private func setNonblocking(_ descriptor: Int32) throws {
    let flags = fcntl(descriptor, F_GETFL)
    try #require(flags >= 0)
    try #require(fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) == 0)
}

private final class SocketFrameResult: @unchecked Sendable {
    private let lock = NSLock()
    private var result: Result<SessionFrame, Error>?

    func store(_ result: Result<SessionFrame, Error>) {
        lock.lock()
        self.result = result
        lock.unlock()
    }

    func get() -> Result<SessionFrame, Error>? {
        lock.lock()
        defer { lock.unlock() }
        return result
    }
}

@Suite(.serialized)
struct SessionReadinessTests {
    @Test func strictExecutorNonblockingReadWaitsForReadiness() throws {
        var descriptors: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        defer {
            close(descriptors[0])
            close(descriptors[1])
        }
        try setNonblocking(descriptors[0])
        let reader = SessionFrameCodec(descriptor: descriptors[0])
        let writer = SessionFrameCodec(descriptor: descriptors[1])
        let expected = SessionFrame(kind: .event, flags: .end, operation: "ready")
        let finished = DispatchSemaphore(value: 0)
        DispatchQueue.global().asyncAfter(deadline: .now() + .milliseconds(50)) {
            try? writer.write(expected)
            finished.signal()
        }

        let received = try reader.read(timeout: 1)
        #expect(received.kind == .event)
        #expect(received.operation == "ready")
        #expect(finished.wait(timeout: .now() + 1) == .success)
    }

    @Test func strictExecutorNonblockingWriteWaitsForReadiness() throws {
        var descriptors: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        defer {
            close(descriptors[0])
            close(descriptors[1])
        }
        try setNonblocking(descriptors[0])
        var bufferBytes: Int32 = 1024
        try #require(setsockopt(
            descriptors[0],
            SOL_SOCKET,
            SO_SNDBUF,
            &bufferBytes,
            socklen_t(MemoryLayout<Int32>.size)
        ) == 0)
        let writer = SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 2)
        let reader = SessionFrameCodec(descriptor: descriptors[1])
        let payload = Data(repeating: 0x5A, count: 2 * 1024 * 1024)
        let received = SocketFrameResult()
        let finished = DispatchSemaphore(value: 0)
        DispatchQueue.global().asyncAfter(deadline: .now() + .milliseconds(50)) {
            received.store(Result { try reader.read(timeout: 2) })
            finished.signal()
        }

        try writer.write(SessionFrame(kind: .event, flags: .end, operation: "large", payload: payload))
        #expect(finished.wait(timeout: .now() + 3) == .success)
        let frame = try #require(received.get()).get()
        #expect(frame.operation == "large")
        #expect(frame.payload == payload)
    }

    @Test func strictExecutorNonblockingReadPreservesDeadline() throws {
        var descriptors: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        defer {
            close(descriptors[0])
            close(descriptors[1])
        }
        try setNonblocking(descriptors[0])
        let codec = SessionFrameCodec(descriptor: descriptors[0])
        let start = ContinuousClock.now
        do {
            _ = try codec.read(timeout: 0.05)
            Issue.record("expected the readiness deadline to expire")
        } catch let error as SessionTransportError {
            #expect(error == .systemCall(operation: "read", errno: EAGAIN))
        }
        #expect(start.duration(to: .now) >= .milliseconds(40))
    }

    @Test func strictExecutorNonblockingWritePreservesDeadline() throws {
        var descriptors: [Int32] = [-1, -1]
        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
        defer {
            close(descriptors[0])
            close(descriptors[1])
        }
        try setNonblocking(descriptors[0])
        var bufferBytes: Int32 = 1024
        try #require(setsockopt(
            descriptors[0],
            SOL_SOCKET,
            SO_SNDBUF,
            &bufferBytes,
            socklen_t(MemoryLayout<Int32>.size)
        ) == 0)
        let codec = SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 0.05)
        let payload = Data(repeating: 0x5A, count: 2 * 1024 * 1024)
        let start = ContinuousClock.now
        do {
            try codec.write(SessionFrame(kind: .event, flags: .end, operation: "large", payload: payload))
            Issue.record("expected the readiness deadline to expire")
        } catch let error as SessionTransportError {
            #expect(error == .systemCall(operation: "send", errno: EAGAIN))
        }
        #expect(start.duration(to: .now) >= .milliseconds(40))
    }
}
