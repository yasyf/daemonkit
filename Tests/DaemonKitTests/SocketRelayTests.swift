@testable import DaemonKit
import Darwin
import Foundation
import Testing

private func relaySocketDirectory() throws -> URL {
    let suffix = UInt32.random(in: 0 ..< 0xFFFF)
    let directory = URL(fileURLWithPath: "/tmp/dk-relay-\(getpid())-\(suffix)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory
}

private actor CountedChunks {
    private let count: Int
    private let size: Int
    private var index = 0
    private var activePulls = 0
    private var maximumActivePulls = 0

    init(count: Int, size: Int) {
        self.count = count
        self.size = size
    }

    func next() -> Data? {
        guard index < count else { return nil }
        activePulls += 1
        maximumActivePulls = max(maximumActivePulls, activePulls)
        let payload = Data(repeating: UInt8(truncatingIfNeeded: index), count: size)
        index += 1
        activePulls -= 1
        return payload
    }

    func snapshot() -> (produced: Int, maximumActivePulls: Int) {
        (index, maximumActivePulls)
    }
}

private actor CancelableChunks {
    private var sentFirst = false
    private var canceled = false
    private var terminalSettled = false
    private var cancellationWaiters: [CheckedContinuation<Void, Never>] = []

    func next() async throws -> Data? {
        if !sentFirst {
            sentFirst = true
            return Data("first".utf8)
        }
        await waitForCancellation()
        throw CancellationError()
    }

    func cancel() {
        guard !canceled else { return }
        canceled = true
        let waiters = cancellationWaiters
        cancellationWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }

    func terminal() async -> SocketTerminal {
        await waitForCancellation()
        terminalSettled = true
        return SocketTerminal(payload: Data(#"{"upstream":"settled"}"#.utf8))
    }

    func settled() -> Bool {
        terminalSettled
    }

    private func waitForCancellation() async {
        if canceled {
            return
        }
        await withCheckedContinuation { continuation in
            cancellationWaiters.append(continuation)
        }
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SocketRelayTests {
        @Test func multiMegabyteRelayUsesBoundedPullsAndPreservesTerminal() async throws {
            let directory = try relaySocketDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let upstreamPath = directory.appendingPathComponent("up.sock").path
            let relayPath = directory.appendingPathComponent("relay.sock").path
            let chunkCount = 128
            let chunkSize = 64 * 1024
            let chunks = CountedChunks(count: chunkCount, size: chunkSize)
            let expectedTerminal = SocketTerminal(
                payload: Data(#"{"etag":"catalog-v1"}"#.utf8),
                error: "upstream detail",
                rejected: true,
                reason: "terminal metadata"
            )
            let upstream = SocketServer(path: upstreamPath, build: "relay-test", trust: .testingUIDOnly) { _ in
                .stream(SocketResponseStream(
                    nextChunk: { await chunks.next() },
                    terminal: { expectedTerminal },
                    cancel: {}
                ))
            }
            try upstream.start()
            defer { upstream.stop() }
            let upstreamClient = try SocketClient(
                path: upstreamPath,
                build: "relay-test",
                configuration: .init(streamQueueDepth: 2)
            )
            defer { upstreamClient.close() }
            let relay = try makeRelay(path: relayPath, upstream: upstreamClient)
            defer { relay.stop() }
            let client = try SocketClient(
                path: relayPath,
                build: "relay-test",
                configuration: .init(streamQueueDepth: 2)
            )
            defer { client.close() }

            let call = try client.open(operation: "catalog.open_at", tenant: "acct-18")
            let received = try await consume(call, chunkSize: chunkSize)
            let terminal = try await call.response()
            let snapshot = await chunks.snapshot()
            #expect(received.chunks == chunkCount)
            #expect(received.bytes == chunkCount * chunkSize)
            #expect(await call.chunks.maximumBufferedChunkCount() <= 2)
            #expect(snapshot.produced == chunkCount)
            #expect(snapshot.maximumActivePulls == 1)
            #expect(terminal.payload == expectedTerminal.payload)
            #expect(terminal.error == expectedTerminal.error)
            #expect(terminal.rejected == expectedTerminal.rejected)
            #expect(terminal.reason == expectedTerminal.reason)
        }

        @Test func canceledRelayCancelsUpstreamAndAwaitsSettlement() async throws {
            let directory = try relaySocketDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let upstreamPath = directory.appendingPathComponent("up.sock").path
            let relayPath = directory.appendingPathComponent("relay.sock").path
            let chunks = CancelableChunks()
            let upstream = SocketServer(path: upstreamPath, build: "relay-test", trust: .testingUIDOnly) { _ in
                .stream(SocketResponseStream(
                    nextChunk: { try await chunks.next() },
                    terminal: { await chunks.terminal() },
                    cancel: { Task { await chunks.cancel() } }
                ))
            }
            try upstream.start()
            defer { upstream.stop() }
            let upstreamClient = try SocketClient(path: upstreamPath, build: "relay-test")
            defer { upstreamClient.close() }
            let relay = try makeRelay(path: relayPath, upstream: upstreamClient)
            defer { relay.stop() }
            let client = try SocketClient(path: relayPath, build: "relay-test")
            defer { client.close() }

            let call = try client.open(operation: "catalog.open_at")
            var iterator = call.chunks.makeAsyncIterator()
            let first = try await iterator.next()
            #expect(first?.payload == Data("first".utf8))
            let waiting = Task {
                var iterator = call.chunks.makeAsyncIterator()
                return try await iterator.next()
            }
            await Task.yield()
            waiting.cancel()
            await #expect(throws: CancellationError.self) {
                try await waiting.value
            }
            let terminal = try await call.response()
            #expect(terminal.error == "wire: request canceled")
            #expect(await chunks.settled())
        }

        @Test func unreadStreamDoesNotBlockUnrelatedResponse() async throws {
            let directory = try relaySocketDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("server.sock").path
            let unread = CountedChunks(count: 128, size: 64 * 1024)
            let server = SocketServer(path: path, build: "relay-test", trust: .testingUIDOnly) { request in
                if request.operation == "unread" {
                    return .stream(SocketResponseStream(
                        nextChunk: { await unread.next() },
                        terminal: { SocketTerminal(payload: Data("true".utf8)) },
                        cancel: {}
                    ))
                }
                return .terminal(SocketTerminal(payload: request.payload))
            }
            try server.start()
            defer { server.stop() }
            let client = try SocketClient(
                path: path,
                build: "relay-test",
                configuration: .init(streamQueueDepth: 1)
            )
            defer { client.close() }

            let blocked = try client.open(operation: "unread")
            try await Task.sleep(for: .milliseconds(20))
            let echo = try await client.call(operation: "echo", payload: Data(#"{"ok":true}"#.utf8))
            #expect(echo.payload == Data(#"{"ok":true}"#.utf8))
            blocked.cancel()
            let terminal = try await blocked.response()
            #expect(terminal.error == "wire: request canceled")
        }

        @Test func emptyStreamSettlesWithSingleCredit() async throws {
            let directory = try relaySocketDirectory()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("server.sock").path
            let server = SocketServer(path: path, build: "relay-test", trust: .testingUIDOnly) { _ in
                .stream(SocketResponseStream(
                    nextChunk: { nil },
                    terminal: { SocketTerminal(payload: Data("true".utf8)) },
                    cancel: {}
                ))
            }
            try server.start()
            defer { server.stop() }
            let client = try SocketClient(
                path: path,
                build: "relay-test",
                configuration: .init(streamQueueDepth: 1)
            )
            defer { client.close() }
            let call = try client.open(operation: "empty")
            var chunks = 0
            for try await _ in call.chunks {
                chunks += 1
            }
            let terminal = try await call.response()
            #expect(chunks == 0)
            #expect(terminal.payload == Data("true".utf8))
        }

        private func makeRelay(path: String, upstream: SocketClient) throws -> SocketServer {
            let relay = SocketServer(path: path, build: "relay-test", trust: .testingUIDOnly) { request in
                do {
                    return try .relaying(upstream.open(
                        operation: request.operation,
                        tenant: request.tenant,
                        payload: request.payload
                    ))
                } catch {
                    return .terminal(SocketTerminal(error: String(describing: error)))
                }
            }
            try relay.start()
            return relay
        }

        private func consume(_ call: SocketCall, chunkSize: Int) async throws -> (chunks: Int, bytes: Int) {
            var receivedChunks = 0
            var receivedBytes = 0
            for try await chunk in call.chunks where !chunk.end {
                #expect(chunk.payload == Data(
                    repeating: UInt8(truncatingIfNeeded: receivedChunks),
                    count: chunkSize
                ))
                receivedChunks += 1
                receivedBytes += chunk.payload.count
                try await Task.sleep(for: .microseconds(100))
            }
            return (receivedChunks, receivedBytes)
        }
    }
}
