@testable import DaemonKit
import Darwin
import Foundation
import Testing

private func withAddress<Result>(
    _ address: inout sockaddr_un,
    _ body: (UnsafePointer<sockaddr>, socklen_t) -> Result
) -> Result {
    withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
            body($0, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
}

private func leaveStaleSocket(at path: String) {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0, var address = makeAddress(path: path) else { return }
    _ = withAddress(&address) { Darwin.bind(descriptor, $0, $1) }
    close(descriptor)
}

private func legacyLineIsRejected(at path: String) async throws -> Bool {
    let queue = DispatchQueue(label: "com.yasyf.daemonkit.tests.legacy-client")
    return try await queue.performIO {
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0, var address = makeAddress(path: path) else { return false }
        defer { close(descriptor) }
        guard withAddress(&address, { connect(descriptor, $0, $1) }) == 0 else { return false }
        var timeout = timeval(tv_sec: 1, tv_usec: 0)
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        let legacy = Data("{\"op\":\"health\"}\n".utf8)
        _ = legacy.withUnsafeBytes { write(descriptor, $0.baseAddress, $0.count) }
        var byte: UInt8 = 0
        return read(descriptor, &byte, 1) == 0
    }
}

private final class ContinuationGate: @unchecked Sendable {
    private let lock = NSLock()
    private var continuation: CheckedContinuation<Void, Never>?

    func wait() async {
        await withCheckedContinuation { continuation in
            lock.lock()
            self.continuation = continuation
            lock.unlock()
        }
    }

    func release() {
        let pending = lock.withLock {
            let pending = self.continuation
            self.continuation = nil
            return pending
        }
        pending?.resume()
    }
}

private actor PullChunks {
    private var chunks: ArraySlice<Data>

    init(_ chunks: [Data]) {
        self.chunks = ArraySlice(chunks)
    }

    func next() -> Data? {
        chunks.popFirst()
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SocketServerTests {
        @Test func frameV1MatchesSharedGoGolden() throws {
            let repository = URL(fileURLWithPath: #filePath)
                .deletingLastPathComponent()
                .deletingLastPathComponent()
                .deletingLastPathComponent()
            let fixture = try JSONSerialization.jsonObject(
                with: Data(contentsOf: repository.appendingPathComponent("wire/testdata/frame-v1.json"))
            ) as? [String: String]
            let hex = try #require(fixture?["hex"])
            var encoded = Data()
            let body = try SessionFrameCodec.encode(SessionFrame(
                kind: .request,
                flags: .end,
                id: 42,
                sequence: 0,
                deadlineUnixMilliseconds: 1_700_000_000_123,
                operation: "mutate",
                tenant: "acct-18",
                payload: Data(#"{"value":1}"#.utf8)
            ))
            var length = UInt32(body.count).bigEndian
            withUnsafeBytes(of: &length) { encoded.append(contentsOf: $0) }
            encoded.append(body)
            #expect(encoded.map { String(format: "%02x", $0) }.joined() == hex)
        }

        @Test func concurrentClosedPeerWritesReturnEPIPEWithoutSIGPIPE() async throws {
            try await withThrowingTaskGroup(of: Void.self) { group in
                for _ in 0 ..< 32 {
                    group.addTask {
                        var descriptors: [Int32] = [-1, -1]
                        try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
                        close(descriptors[1])
                        defer { close(descriptors[0]) }

                        let codec = SessionFrameCodec(
                            descriptor: descriptors[0],
                            maximumFrameBytes: daemonKitDefaultMaximumFrameBytes
                        )
                        do {
                            try codec.write(SessionFrame(kind: .goAway, flags: .end))
                            Issue.record("expected the closed peer write to fail")
                        } catch let error as SessionTransportError {
                            #expect(error == .systemCall(operation: "send", errno: EPIPE))
                        }
                    }
                }
                try await group.waitForAll()
            }
        }

        @Test func swiftServerAcknowledgesGoCompatibleGoAway() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let server = SocketServer(path: path, wireBuild: "interop.v1") { _ in
                    .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }

                let queue = DispatchQueue(label: "com.yasyf.daemonkit.tests.go-client-fixture")
                try await queue.performIO {
                    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
                    guard descriptor >= 0, var address = makeAddress(path: path) else {
                        throw SessionTransportError.systemCall(operation: "socket", errno: errno)
                    }
                    defer { Darwin.close(descriptor) }
                    guard withAddress(&address, { connect(descriptor, $0, $1) }) == 0 else {
                        throw SessionTransportError.systemCall(operation: "connect", errno: errno)
                    }
                    let codec = SessionFrameCodec(descriptor: descriptor)
                    let hello = try JSONEncoder().encode(SessionWireIdentity(
                        protocolVersion: daemonKitSessionProtocolVersion,
                        wireBuild: "interop.v1"
                    ))
                    try codec.write(SessionFrame(kind: .hello, flags: .end, payload: hello))
                    let acknowledgment = try codec.read(timeout: 1)
                    #expect(acknowledgment.kind == .helloAck)
                    _ = try SessionHandshakeCodec.decodeAck(acknowledgment.payload)
                    try codec.write(SessionFrame(kind: .window, sequence: 1))
                    try codec.write(SessionFrame(kind: .goAway, flags: .end))
                    let goAway = try codec.read(timeout: 1)
                    #expect(goAway.kind == .goAway)
                    #expect(goAway.flags == .end)
                }
            }
        }

        @Test func swiftClientWaitsForGoCompatibleGoAwayAcknowledgment() async throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            var address = try #require(makeAddress(path: path))
            let listener = socket(AF_UNIX, SOCK_STREAM, 0)
            try #require(listener >= 0)
            defer { Darwin.close(listener) }
            try #require(withAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
            try #require(listen(listener, 1) == 0)
            let queue = DispatchQueue(label: "com.yasyf.daemonkit.tests.go-server-fixture")
            let peer = Task {
                try await queue.performIO {
                    let descriptor = accept(listener, nil, nil)
                    guard descriptor >= 0 else {
                        throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                    }
                    defer { Darwin.close(descriptor) }
                    let codec = SessionFrameCodec(descriptor: descriptor)
                    let hello = try codec.read(timeout: 1)
                    #expect(hello.kind == .hello)
                    _ = try SessionHandshakeCodec.decodeHello(hello.payload)
                    let acknowledgment = try SessionHandshakeCodec.encodeSuccess(
                        wireBuild: "interop.v1",
                        session: Data(repeating: 7, count: 16)
                    )
                    try codec.write(SessionFrame(kind: .helloAck, flags: .end, payload: acknowledgment))
                    let window = try codec.read(timeout: 1)
                    #expect(window.kind == .window)
                    let goAway = try codec.read(timeout: 1)
                    #expect(goAway.kind == .goAway)
                    try codec.write(SessionFrame(kind: .goAway, flags: .end))
                }
            }
            let client = try await SocketClient(
                path: path,
                wireBuild: "interop.v1",
                configuration: .init(writeTimeout: 1)
            )
            await client.close()
            try await peer.value
        }

        @Test func persistentSessionMultiplexesEventsAndStreams() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let server = SocketServer(path: path, wireBuild: "server-test") { request in
                    if request.operation == "stream" {
                        try? await request.session.pushEvent(topic: "changed", payload: Data(request.tenant.utf8))
                        let chunks = PullChunks([Data("a".utf8), Data("b".utf8)])
                        return .stream(SocketResponseStream(
                            nextChunk: { await chunks.next() },
                            terminal: { SocketTerminal(payload: Data(#""done""#.utf8)) },
                            cancel: {}
                        ))
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "server-test")
                cleanup.add { await client.close() }
                #expect(client.peerWireBuild == "server-test")

                try await withThrowingTaskGroup(of: Void.self) { group in
                    for index in 0 ..< 20 {
                        group.addTask {
                            let payload = Data("\(index)".utf8)
                            let result = try await client.call(operation: "echo", payload: payload)
                            #expect(result.payload == payload)
                            #expect(result.error == nil)
                        }
                    }
                    try await group.waitForAll()
                }

                let call = try await client.open(operation: "stream", tenant: "acct-18")
                var streamPayloads: [Data] = []
                for try await chunk in call.chunks where !chunk.end {
                    streamPayloads.append(chunk.payload)
                }
                let result = try await call.response()
                #expect(streamPayloads == [Data("a".utf8), Data("b".utf8)])
                #expect(result.payload == Data(#""done""#.utf8))
                for try await event in client.events {
                    #expect(event.topic == "changed")
                    #expect(event.payload == Data("acct-18".utf8))
                    break
                }
            }
        }
    }
}

extension SocketTransportTests.SocketServerTests {
    @Test func persistentSessionSurvivesPastHandshakeTimeout() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = SocketServer(
                path: path,
                wireBuild: "server-test",
                configuration: .init(handshakeTimeout: 0.1)
            ) { _ in
                .terminal(SocketTerminal(payload: Data(#""pong""#.utf8)))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try await SocketClient(
                path: path,
                wireBuild: "server-test",
                configuration: .init(handshakeTimeout: 0.1)
            )
            cleanup.add { await client.close() }
            try await Task.sleep(for: .milliseconds(300))
            let result = try await client.call(operation: "ping")
            #expect(result.payload == Data(#""pong""#.utf8))
        }
    }

    @Test func rejectsStructurallyInvalidKindFields() throws {
        let invalid = [
            SessionFrame(kind: .cancel, flags: .end, id: 1, sequence: 1),
            SessionFrame(kind: .cancel, flags: .end, id: 1, payload: Data("x".utf8)),
            SessionFrame(kind: .event, flags: .end, sequence: 1, operation: "changed"),
            SessionFrame(kind: .event, flags: .end, operation: "changed", tenant: "acct-18"),
            SessionFrame(kind: .lifecycle, flags: .end),
            SessionFrame(kind: .lifecycle, flags: .end, id: 1, payload: Data("x".utf8)),
            SessionFrame(kind: .lifecycle, flags: .end, operation: "changed", payload: Data("x".utf8)),
            SessionFrame(kind: .goAway, flags: .end, payload: Data("x".utf8)),
            SessionFrame(kind: .acknowledgment, flags: .end, id: 1),
            SessionFrame(kind: .acknowledgment, flags: .end, id: 1, payload: Data(repeating: 0, count: 15)),
            SessionFrame(
                kind: .acknowledgment,
                flags: .end,
                id: 1,
                operation: "mutate",
                payload: Data(repeating: 0, count: 16)
            ),
        ]
        for frame in invalid {
            #expect(throws: SessionTransportError.self) { try SessionFrameCodec.encode(frame) }
        }
        _ = try SessionFrameCodec.encode(SessionFrame(
            kind: .event,
            flags: .end,
            operation: "changed",
            payload: Data("payload".utf8)
        ))
        _ = try SessionFrameCodec.encode(SessionFrame(
            kind: .acknowledgment,
            flags: .end,
            id: 1,
            payload: Data(repeating: 0, count: 16)
        ))
        #expect(SessionFrameKind.lifecycle.rawValue == 11)
        _ = try SessionFrameCodec.encode(SessionFrame(
            kind: .lifecycle,
            flags: .end,
            payload: Data("snapshot".utf8)
        ))
    }

    @Test func canceledCallMustSettleWithinBound() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let gate = ContinuationGate()
            let server = SocketServer(path: path, wireBuild: "server-test") { _ in
                await gate.wait()
                return .terminal(SocketTerminal(payload: Data("null".utf8)))
            }
            try await server.start()
            let client = try await SocketClient(
                path: path,
                wireBuild: "server-test",
                configuration: .init(cancellationSettlementTimeout: 0.05)
            )
            cleanup.add { await client.close() }
            let call = try await client.open(operation: "wait")
            await call.cancel()
            await #expect(throws: SessionTransportError.cancellationDidNotSettle) {
                try await call.response()
            }
            await #expect(throws: SessionTransportError.self) {
                try await call.sendChunk(Data("late".utf8))
            }
            gate.release()
            await client.close()
            await server.stop()
        }
    }

    @Test func requestInputStreamIsOrdered() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = SocketServer(path: path, wireBuild: "server-test") { request in
                var values: [String] = []
                do {
                    for try await chunk in request.chunks where !chunk.end {
                        values.append(String(data: chunk.payload, encoding: .utf8) ?? "")
                    }
                } catch {
                    return .terminal(SocketTerminal(error: String(describing: error)))
                }
                return .terminal(SocketTerminal(payload: try? JSONEncoder().encode(values)))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            let client = try await SocketClient(path: path, wireBuild: "server-test")
            cleanup.add { await client.close() }
            let call = try await client.open(operation: "collect", endInput: false)
            try await call.sendChunk(Data("one".utf8))
            try await call.sendChunk(Data("two".utf8))
            try await call.closeSend()
            let result = try await call.response()
            let values = try JSONDecoder().decode([String].self, from: #require(result.payload))
            #expect(values == ["one", "two"])
        }
    }

    @Test func mismatchedBuildFailsDuringHandshakeBeforeMutation() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = SocketServer(path: path, wireBuild: "new-build") { _ in
                Issue.record("mismatched-build mutation handler must not run")
                return .terminal(SocketTerminal(payload: Data("true".utf8)))
            }
            try await server.start()
            cleanup.add { await server.stop() }
            await #expect(throws: SocketWireBuildMismatchError(
                server: "new-build",
                client: "old-build"
            )) {
                _ = try await SocketClient(path: path, wireBuild: "old-build")
            }
        }
    }

    @Test func rejectsLegacyLFClientAndOversizedFrame() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            let server = SocketServer(
                path: path,
                wireBuild: "server-test",
                configuration: .init(maximumFrameBytes: 64)
            ) { _ in .terminal(SocketTerminal(payload: Data("null".utf8))) }
            try await server.start()
            cleanup.add { await server.stop() }
            #expect(try await legacyLineIsRejected(at: path))
        }
    }

    @Test func codecRejectsPartialForeignAndOversizedFrames() throws {
        let body = try SessionFrameCodec.encode(SessionFrame(
            kind: .hello,
            flags: .end,
            payload: Data("{}".utf8)
        ))
        var foreign = body
        foreign[4] = 0
        foreign[5] = 2
        #expect(throws: SessionTransportError.self) { _ = try SessionFrameCodec.decode(foreign) }
        #expect(throws: SessionTransportError.self) { _ = try SessionFrameCodec.decode(Data("short".utf8)) }
    }

    @Test func chmodsReclaimsAndRefusesLivePeer() async throws {
        try await withAsyncCleanup { cleanup in
            let directory = try shortSocketDir()
            cleanup.add { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            leaveStaleSocket(at: path)
            let server = SocketServer(path: path, wireBuild: "server-test") { _ in
                .terminal(SocketTerminal())
            }
            try await server.start()
            cleanup.add { await server.stop() }
            var status = stat()
            #expect(stat(path, &status) == 0)
            #expect((status.st_mode & 0o777) == 0o600)

            let intruder = SocketServer(path: path, wireBuild: "intruder") { _ in
                .terminal(SocketTerminal())
            }
            await #expect(throws: SocketServerError.self) { try await intruder.start() }
        }
    }

    @Test func cleanShutdownUnlinksAndRejectsOverlongPath() async throws {
        let directory = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: directory) }
        let path = directory.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path, wireBuild: "server-test") { _ in
            .terminal(SocketTerminal())
        }
        try await server.start()
        await server.stop()
        #expect(!FileManager.default.fileExists(atPath: path))

        let longPath = "/tmp/" + String(repeating: "a", count: 200) + ".sock"
        let invalid = SocketServer(path: longPath, wireBuild: "server-test") { _ in
            .terminal(SocketTerminal())
        }
        await #expect(throws: SocketServerError.self) { try await invalid.start() }
    }
}
