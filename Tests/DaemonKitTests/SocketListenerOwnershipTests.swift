@testable import DaemonKit
import Foundation
import Testing

private actor ListenerOwnershipGate {
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

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SocketListenerOwnershipTests {
        @Test func listenerOwnsPathUntilStopSettlementCompletes() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("owner.sock").path
                let handlerGate = ListenerOwnershipGate()
                let drainGate = ListenerOwnershipGate()
                let old = SocketServer(path: path, wireBuild: "old") { _ in
                    await handlerGate.wait()
                    return .terminal(SocketTerminal())
                }
                try await old.start()
                cleanup.add { await old.stop() }
                let client = try await SocketClient(path: path, wireBuild: "old",
                role: SessionPeerRole.unprotected)
                cleanup.add { client.abort() }
                let call = Task { try await client.call(operation: "hold") }
                await handlerGate.waitUntilEntered()

                old.stopDrainHook = { await drainGate.wait() }
                let stopping = Task { await old.stop() }
                await drainGate.waitUntilEntered()

                let contender = SocketServer(path: path, wireBuild: "new") { _ in
                    .terminal(SocketTerminal())
                }
                await #expect(throws: SocketServerError.self) {
                    try await contender.start()
                }
                #expect(FileManager.default.fileExists(atPath: path))

                await drainGate.release()
                await handlerGate.release()
                await stopping.value
                _ = try? await call.value
                old.stopDrainHook = nil
                #expect(!FileManager.default.fileExists(atPath: path))

                let replacement = SocketServer(path: path, wireBuild: "new") { _ in
                    .terminal(SocketTerminal())
                }
                try await replacement.start()
                cleanup.add { await replacement.stop() }
                #expect(FileManager.default.fileExists(atPath: path))
                await replacement.stop()
                #expect(!FileManager.default.fileExists(atPath: path))
            }
        }
    }
}
