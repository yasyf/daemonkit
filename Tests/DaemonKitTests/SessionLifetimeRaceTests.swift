@testable import DaemonKit
import Darwin
import Foundation
import Testing

let cooperativeStrict = ProcessInfo.processInfo.environment["LIBDISPATCH_COOPERATIVE_POOL_STRICT"] == "1"

private actor LifetimeGate {
    private var entered = false
    private var entryWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        entered = true
        let pending = entryWaiters
        entryWaiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
        await withCheckedContinuation { releaseWaiters.append($0) }
    }

    func waitUntilEntered() async {
        if entered {
            return
        }
        await withCheckedContinuation { entryWaiters.append($0) }
    }

    func release() {
        let pending = releaseWaiters
        releaseWaiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
    }
}

private actor LifetimeSessionCapture {
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
        return await withCheckedContinuation { waiters.append($0) }
    }
}

private actor LifetimeChunkSource {
    private var emitted = false

    func next() -> Data? {
        guard !emitted else { return nil }
        emitted = true
        return Data("chunk".utf8)
    }
}

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SessionLifetimeRaceTests {
        @Test func sessionTimeReservesZeroAndSaturates() {
            #expect(SessionTime.unixMilliseconds(Date(timeIntervalSince1970: -1)) == 1)
            #expect(SessionTime.unixMilliseconds(Date(timeIntervalSince1970: 0)) == 1)
            #expect(SessionTime.unixMilliseconds(Date(timeIntervalSince1970: -.infinity)) == 1)
            #expect(SessionTime.unixMilliseconds(Date(timeIntervalSince1970: .nan)) == 1)
            #expect(SessionTime.unixMilliseconds(Date(timeIntervalSince1970: .infinity)) == .max)
            let past = SessionTime.unixMilliseconds(Date(timeIntervalSinceNow: -1))
            #expect(past > 1)
            #expect(SessionTime.remainingMilliseconds(until: past) == 0)
            let nilDeadline = Date?.none.map(SessionTime.unixMilliseconds) ?? 0
            #expect(nilDeadline == 0)
            #expect(SessionTime.remainingMilliseconds(until: 1) == 0)
            #expect(SessionFrameCodec.durationNanoseconds(.infinity) == .max)
            #expect(SessionFrameCodec.deadline(after: 0) == nil)
        }

        @Test func canceledDescriptorOwnerCannotShutdownReusedDescriptor() throws {
            var original: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &original) == 0)
            let reusedDescriptor = original[0]
            let owner = OwnedDescriptor()
            try owner.install(reusedDescriptor)
            Darwin.close(original[1])
            owner.close()

            var replacement: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &replacement) == 0)
            let replacementPeer = replacement[1]
            if replacement[0] != reusedDescriptor {
                try #require(dup2(replacement[0], reusedDescriptor) == reusedDescriptor)
                Darwin.close(replacement[0])
            }
            defer {
                Darwin.close(reusedDescriptor)
                Darwin.close(replacementPeer)
            }

            owner.cancel()
            var sent: UInt8 = 0xA5
            try #require(Darwin.write(reusedDescriptor, &sent, 1) == 1)
            var received: UInt8 = 0
            try #require(Darwin.read(replacementPeer, &received, 1) == 1)
            #expect(received == sent)
        }

        @Test func droppingClientDuringResponseSettlementClosesPeer() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("drop-settling-client.sock").path
                let capture = LifetimeSessionCapture()
                let server = SocketServer(path: path, wireBuild: "drop-settling-client") { request in
                    await capture.record(request.session)
                    return .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let settling = LifetimeGate()
                var client: SocketClient? = try await SocketClient(
                    path: path,
                    wireBuild: "drop-settling-client",
                    role: SessionPeerRole.unprotected
                )
                client?.requestSettlementHook = { await settling.wait() }
                weak let weakClient = client
                var call: SocketCall? = try await client?.open(operation: "terminal")
                #expect(call != nil)
                await settling.waitUntilEntered()
                let session = await capture.value()
                call = nil
                client = nil
                #expect(weakClient == nil)
                await session.waitUntilClosed()
                await settling.release()
            }
        }

        @Test func droppingClientDuringStreamDeliveryClosesPeer() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("drop-streaming-client.sock").path
                let capture = LifetimeSessionCapture()
                let chunks = LifetimeChunkSource()
                let server = SocketServer(path: path, wireBuild: "drop-streaming-client") { request in
                    await capture.record(request.session)
                    return .stream(SocketResponseStream(
                        nextChunk: { await chunks.next() },
                        terminal: { SocketTerminal() },
                        cancel: {}
                    ))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let delivering = LifetimeGate()
                var client: SocketClient? = try await SocketClient(
                    path: path,
                    wireBuild: "drop-streaming-client",
                    role: SessionPeerRole.unprotected
                )
                client?.receiveStreamOfferHook = { await delivering.wait() }
                weak let weakClient = client
                var call: SocketCall? = try await client?.open(operation: "stream")
                #expect(call != nil)
                await delivering.waitUntilEntered()
                let session = await capture.value()
                call = nil
                client = nil
                #expect(weakClient == nil)
                await session.waitUntilClosed()
                await delivering.release()
            }
        }
    }
}
