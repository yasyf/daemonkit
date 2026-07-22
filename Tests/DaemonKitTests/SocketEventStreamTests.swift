@testable import DaemonKit
import Darwin
import Foundation
import Testing

private func eventSocketDirectory() throws -> URL {
    let suffix = UInt32.random(in: 0 ..< 0xFFFF)
    let directory = URL(fileURLWithPath: "/tmp/dk-events-\(getpid())-\(suffix)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SocketEventStreamTests {
        @Test func slowConsumerBackpressuresWithoutDroppingEvents() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try eventSocketDirectory()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("events.sock").path
                let eventCount = 128
                let eventSize = 64 * 1024
                let server = SocketServer(path: path, build: "event-test", trust: .sameEffectiveUser) { request in
                    if request.operation == "echo" {
                        return .terminal(SocketTerminal(payload: request.payload))
                    }
                    do {
                        for index in 0 ..< eventCount {
                            try await request.session.pushEvent(
                                topic: "convergence",
                                payload: Data(repeating: UInt8(truncatingIfNeeded: index), count: eventSize)
                            )
                        }
                        return .terminal(SocketTerminal(payload: Data("true".utf8)))
                    } catch {
                        return .terminal(SocketTerminal(error: String(describing: error)))
                    }
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(
                    path: path,
                    build: "event-test",
                    configuration: .init(eventQueueDepth: 2),
                    trust: .sameEffectiveUser
                )
                cleanup.add { await client.close() }
                let events = client.events
                let response = Task { try await client.call(operation: "emit") }
                try await Task.sleep(for: .milliseconds(20))
                let echo = try await client.call(operation: "echo", payload: Data(#"{"ok":true}"#.utf8))
                #expect(echo.payload == Data(#"{"ok":true}"#.utf8))

                var iterator = events.makeAsyncIterator()
                for index in 0 ..< eventCount {
                    let event = try #require(await iterator.next())
                    #expect(event.topic == "convergence")
                    #expect(event.payload == Data(repeating: UInt8(truncatingIfNeeded: index), count: eventSize))
                    try await Task.sleep(for: .microseconds(100))
                }
                let terminal = try await response.value
                #expect(terminal.payload == Data("true".utf8))
                #expect(await events.maximumBufferedEventCount() <= 2)
            }
        }

        @Test func canceledConsumerSettlesAndClosesSession() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try eventSocketDirectory()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("events.sock").path
                let server = SocketServer(path: path, build: "event-test", trust: .sameEffectiveUser) { _ in
                    .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, build: "event-test", trust: .sameEffectiveUser)
                let waiting = Task {
                    var iterator = client.events.makeAsyncIterator()
                    return try await iterator.next()
                }
                await Task.yield()
                waiting.cancel()
                await #expect(throws: CancellationError.self) {
                    try await waiting.value
                }
                await #expect(throws: SessionTransportError.disconnected) {
                    try await client.open(operation: "after-cancel")
                }
            }
        }
    }
}
