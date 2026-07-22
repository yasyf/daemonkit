@testable import DaemonKit
import Darwin
import Foundation
import Testing

private actor SessionCapture {
    private var session: SocketSession?
    private var waiters: [CheckedContinuation<SocketSession, Never>] = []

    func record(_ session: SocketSession) {
        self.session = session
        let pending = waiters
        waiters.removeAll()
        for waiter in pending {
            waiter.resume(returning: session)
        }
    }

    func value() async -> SocketSession {
        if let session {
            return session
        }
        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
        }
    }
}

private struct LifecycleFixture {
    let directory: URL
    let path: String
    let server: SocketServer
    let capture: SessionCapture

    func cleanup() async {
        await server.stop()
        try? FileManager.default.removeItem(at: directory)
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SocketSessionLifecycleTests {
        @Test func acceptedSessionClosesExactlyOnPeerDisconnect() async throws {
            let fixture = try await makeFixture()
            let client = try await SocketClient(path: fixture.path, build: "server-test", trust: .sameEffectiveUser)
            _ = try await client.call(operation: "capture")
            let session = await fixture.capture.value()
            #expect(session.isConnected)
            await client.close()
            await session.waitUntilClosed()
            await fixture.cleanup()
            #expect(!session.isConnected)
            await session.waitUntilClosed()
        }

        @Test func acceptedSessionClosesOnServerStop() async throws {
            try await withAsyncCleanup { cleanup in
                let fixture = try await makeFixture()
                let client = try await SocketClient(path: fixture.path, build: "server-test", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                _ = try await client.call(operation: "capture")
                let session = await fixture.capture.value()
                #expect(session.isConnected)
                await fixture.server.stop()
                await session.waitUntilClosed()
                #expect(!session.isConnected)
                await client.close()
                await fixture.cleanup()
            }
        }

        private func makeFixture() async throws -> LifecycleFixture {
            let directory = URL(fileURLWithPath: "/tmp/dkl-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            let path = directory.appendingPathComponent("s.sock").path
            let capture = SessionCapture()
            let server = SocketServer(path: path, build: "server-test", trust: .sameEffectiveUser) { request in
                await capture.record(request.session)
                return .terminal(SocketTerminal(payload: Data("true".utf8)))
            }
            try await server.start()
            return LifecycleFixture(directory: directory, path: path, server: server, capture: capture)
        }
    }
}
