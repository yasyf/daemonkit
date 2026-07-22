@testable import DaemonKit
import Darwin
import Foundation
import Testing

private struct UploadResult: Codable {
    let chunkCount: Int
    let byteCount: Int
    let highWatermark: Int
}

private actor RequestSettlementProbe {
    private var settled = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func markSettled() async {
        settled = true
        let pending = waiters
        waiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
    }

    func wait() async {
        if settled {
            return
        }
        await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SocketRequestStreamTests {
        @Test func slowUploadConsumerBackpressuresWithoutDroppingChunks() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try socketDirectory()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let depth = 2
                let server = SocketServer(
                    path: path,
                    build: "server-test",
                    configuration: .init(streamQueueDepth: depth),
                    trust: .sameEffectiveUser
                ) { request in
                    var chunkCount = 0
                    var byteCount = 0
                    do {
                        for try await chunk in request.chunks {
                            chunkCount += 1
                            byteCount += chunk.payload.count
                            try await Task.sleep(for: .microseconds(250))
                        }
                    } catch {
                        return .terminal(SocketTerminal(error: String(describing: error)))
                    }
                    let result = await UploadResult(
                        chunkCount: chunkCount,
                        byteCount: byteCount,
                        highWatermark: request.chunks.maximumBufferedChunkCount()
                    )
                    return .terminal(SocketTerminal(payload: try? JSONEncoder().encode(result)))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, build: "server-test", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                let payload = Data(repeating: 0xA5, count: 64 * 1024)
                let expectedChunks = 128
                let call = try await client.open(operation: "upload", endInput: false)
                for _ in 0 ..< expectedChunks {
                    try await call.sendChunk(payload)
                }
                try await call.closeSend()
                let terminal = try await call.response()
                let result = try JSONDecoder().decode(UploadResult.self, from: #require(terminal.payload))

                #expect(terminal.error == nil)
                #expect(result.chunkCount == expectedChunks + 1)
                #expect(result.byteCount == expectedChunks * payload.count)
                #expect(result.highWatermark <= depth)
            }
        }

        @Test func cancellationReapsUploadHandlerBeforeNextAdmission() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try socketDirectory()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let probe = RequestSettlementProbe()
                let server = SocketServer(
                    path: path,
                    build: "server-test",
                    configuration: .init(maximumActiveRequests: 1, streamQueueDepth: 1),
                    trust: .sameEffectiveUser
                ) { request in
                    if request.operation == "upload" {
                        do {
                            for try await _ in request.chunks {}
                        } catch {}
                        await probe.markSettled()
                    }
                    return .terminal(SocketTerminal(payload: Data("true".utf8)))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, build: "server-test", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                let upload = try await client.open(operation: "upload", endInput: false)
                await upload.cancel()
                let canceled = try await upload.response()
                #expect(canceled.error == "wire: request canceled")
                await probe.wait()

                let next = try await client.open(operation: "next")
                let terminal = try await next.response()
                #expect(terminal.error == nil)
                #expect(terminal.rejected == false)
            }
        }

        @Test func blockedUploadDoesNotBlockUnrelatedResponse() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try socketDirectory()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let server = SocketServer(
                    path: path,
                    build: "server-test",
                    configuration: .init(streamQueueDepth: 1),
                    trust: .sameEffectiveUser
                ) { request in
                    if request.operation == "upload" {
                        do {
                            try await Task.sleep(for: .seconds(60))
                        } catch {}
                        return .terminal(SocketTerminal(error: "canceled"))
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, build: "server-test", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }

                let upload = try await client.open(operation: "upload", endInput: false)
                try await upload.sendChunk(Data("one".utf8))
                let blockedSend = Task { try await upload.sendChunk(Data("two".utf8)) }
                try await Task.sleep(for: .milliseconds(20))
                let echo = try await client.call(operation: "echo", payload: Data(#"{"ok":true}"#.utf8))
                #expect(echo.payload == Data(#"{"ok":true}"#.utf8))
                await upload.cancel()
                await #expect(throws: Error.self) {
                    try await blockedSend.value
                }
                _ = try await upload.response()
            }
        }

        private func socketDirectory() throws -> URL {
            let directory = URL(fileURLWithPath: "/tmp/dkr-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            return directory
        }
    }
}
