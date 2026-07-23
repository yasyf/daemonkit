@testable import DaemonKit
import Darwin
import Foundation
import Testing

private actor AsyncGate {
    private var entered = false
    private var entryWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        entered = true
        let waiters = entryWaiters
        entryWaiters.removeAll()
        for waiter in waiters {
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
        let waiters = releaseWaiters
        releaseWaiters.removeAll()
        for waiter in waiters {
            waiter.resume()
        }
    }
}

private actor OneShotGate {
    private var used = false
    private var entered = false
    private var released = false
    private var entryWaiters: [CheckedContinuation<Void, Never>] = []
    private var releaseWaiters: [CheckedContinuation<Void, Never>] = []

    func wait() async {
        guard !used else { return }
        used = true
        entered = true
        let pending = entryWaiters
        entryWaiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
        if released {
            return
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
        released = true
        let pending = releaseWaiters
        releaseWaiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
    }
}

private actor FrameStartProbe {
    private var started: Set<UInt64> = []
    private var waiters: [UInt64: [CheckedContinuation<Void, Never>]] = [:]

    func record(_ id: UInt64) {
        started.insert(id)
        let pending = waiters.removeValue(forKey: id) ?? []
        for waiter in pending {
            waiter.resume()
        }
    }

    func wait(for id: UInt64) async {
        if started.contains(id) {
            return
        }
        await withCheckedContinuation { waiters[id, default: []].append($0) }
    }

    func contains(_ id: UInt64) -> Bool {
        started.contains(id)
    }
}

private actor CompletionProbe {
    private var completed = false
    private var waiters: [CheckedContinuation<Void, Never>] = []

    func finish() {
        completed = true
        let pending = waiters
        waiters.removeAll()
        for waiter in pending {
            waiter.resume()
        }
    }

    func value() -> Bool {
        completed
    }

    func waitUntilFinished() async {
        if completed {
            return
        }
        await withCheckedContinuation { waiters.append($0) }
    }
}

private actor InvocationProbe {
    private var count = 0
    private var waiters: [(Int, CheckedContinuation<Void, Never>)] = []

    func record() {
        count += 1
        let ready = waiters.filter { $0.0 <= count }
        waiters.removeAll { $0.0 <= count }
        for (_, waiter) in ready {
            waiter.resume()
        }
    }

    func wait(for target: Int) async {
        if count >= target {
            return
        }
        await withCheckedContinuation { waiters.append((target, $0)) }
    }
}

private actor BooleanProbe {
    private var result: Bool?
    private var waiters: [CheckedContinuation<Bool, Never>] = []

    func record(_ result: Bool) {
        self.result = result
        let pending = waiters
        waiters.removeAll()
        for waiter in pending {
            waiter.resume(returning: result)
        }
    }

    func value() async -> Bool {
        if let result {
            return result
        }
        return await withCheckedContinuation { waiters.append($0) }
    }
}

private actor SingleChunkSource {
    private var emitted = false

    func next() -> Data? {
        guard !emitted else { return nil }
        emitted = true
        return Data("chunk".utf8)
    }
}

private actor SocketSessionCapture {
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

extension SocketTransportTests {
    @Suite(.timeLimit(.minutes(1)))
    struct SessionAsyncRuntimeTests {
        @Test func invalidPublicTransportBoundsFailBeforeIO() async {
            await #expect(throws: SessionTransportError.self) {
                _ = try await SocketClient(
                    path: "/tmp/never-connect.sock",
                    wireBuild: "invalid",
                    configuration: .init(maximumFrameBytes: 0),
                    trust: .sameEffectiveUser
                )
            }
            await #expect(throws: SessionTransportError.self) {
                _ = try await SocketClient(
                    path: "/tmp/never-connect.sock",
                    wireBuild: "invalid",
                    configuration: .init(handshakeTimeout: .infinity),
                    trust: .sameEffectiveUser
                )
            }
            let server = SocketServer(
                path: "/tmp/never-bind.sock",
                wireBuild: "invalid",
                configuration: .init(maximumActiveRequests: 0, maximumSessions: 0, writeTimeout: .nan),
                trust: .sameEffectiveUser
            ) { _ in .terminal(SocketTerminal()) }
            await #expect(throws: SessionTransportError.self) { try await server.start() }
            await server.stop()
        }

        @Test func expiredNonNilDeadlineCancelsTheServerHandler() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("expired.sock").path
                let server = SocketServer(path: path, wireBuild: "expired", trust: .sameEffectiveUser) { _ in
                    do {
                        try await Task.sleep(for: .seconds(5))
                    } catch {}
                    return .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "expired", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let terminal = try await client.call(
                    operation: "expired",
                    deadline: Date(timeIntervalSince1970: -1)
                )
                #expect(terminal.error == "wire: request canceled")
            }
        }

        @Test(.enabled(if: cooperativeStrict))
        func strictExecutorRunsProductionClientAndServerTogether() async throws {
            #expect(ProcessInfo.processInfo.environment["LIBDISPATCH_COOPERATIVE_POOL_STRICT"] == "1")
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("strict.sock").path
                let server = SocketServer(path: path, wireBuild: "strict", trust: .sameEffectiveUser) { request in
                    .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "strict", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let payload = Data(#"{"strict":true}"#.utf8)
                let response = try await client.call(operation: "echo", payload: payload)
                #expect(response.payload == payload)
            }
        }

        @Test func canceledStartCannotPublishOrLeakListener() async throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("start.sock").path
            let gate = AsyncGate()
            let server = SocketServer(path: path, wireBuild: "start", trust: .sameEffectiveUser) { _ in
                .terminal(SocketTerminal())
            }
            server.startCommitHook = { await gate.wait() }
            let start = Task { try await server.start() }
            await gate.waitUntilEntered()
            start.cancel()
            await gate.release()
            await #expect(throws: CancellationError.self) { try await start.value }
            await server.stop()
            #expect(!FileManager.default.fileExists(atPath: path))
        }

        @Test func concurrentStopsAwaitTheSameSessionDrain() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("stop.sock").path
                let server = SocketServer(path: path, wireBuild: "stop", trust: .sameEffectiveUser) { _ in
                    .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let finishing = AsyncGate()
                let coAwaiting = AsyncGate()
                let secondDone = CompletionProbe()
                server.stopFinishHook = { await finishing.wait() }
                server.stopWaitHook = { await coAwaiting.wait() }
                let first = Task { await server.stop() }
                await finishing.waitUntilEntered()
                let second = Task {
                    await server.stop()
                    await secondDone.finish()
                }
                await coAwaiting.waitUntilEntered()
                #expect(await !(secondDone.value()))
                await finishing.release()
                await coAwaiting.release()
                await first.value
                await second.value
                server.stopFinishHook = nil
                server.stopWaitHook = nil
                #expect(!FileManager.default.fileExists(atPath: path))
            }
        }
    }

    @Suite(.timeLimit(.minutes(1)))
    struct SessionSettlementTests {
        @Test func cancellationAfterRequestCommitCancelsOnlyThatRequest() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("open-cancel.sock").path
                let handlerGate = AsyncGate()
                let cancellationObserved = CompletionProbe()
                let server = SocketServer(path: path, wireBuild: "open-cancel", trust: .sameEffectiveUser) { request in
                    if request.operation == "cancel-me" {
                        await handlerGate.wait()
                        if Task.isCancelled {
                            await cancellationObserved.finish()
                        }
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "open-cancel", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let gate = AsyncGate()
                client.openCommitHook = { await gate.wait() }
                let opening = Task { try await client.open(operation: "cancel-me") }
                await gate.waitUntilEntered()
                await handlerGate.waitUntilEntered()
                opening.cancel()
                await gate.release()
                await #expect(throws: CancellationError.self) { try await opening.value }
                client.openCommitHook = nil
                let payload = Data(#"{"healthy":true}"#.utf8)
                let terminal = try await client.call(operation: "echo", payload: payload)
                #expect(terminal.payload == payload)
                await handlerGate.release()
                await cancellationObserved.waitUntilFinished()
            }
        }

        @Test func canceledResponseWaiterReturnsCancellationAndKeepsSessionHealthy() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("response-cancel.sock").path
                let handlerGate = AsyncGate()
                let cancellationObserved = CompletionProbe()
                let server = SocketServer(path: path, wireBuild: "response-cancel", trust: .sameEffectiveUser) { request in
                    if request.operation == "wait" {
                        await handlerGate.wait()
                        if Task.isCancelled {
                            await cancellationObserved.finish()
                        }
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "response-cancel", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let call = try await client.open(operation: "wait")
                await handlerGate.waitUntilEntered()
                let response = Task { try await call.response() }
                response.cancel()
                await #expect(throws: CancellationError.self) { try await response.value }
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
                await handlerGate.release()
                await cancellationObserved.waitUntilFinished()
            }
        }

        @Test func terminalResponseSettlesRequestIOBeforeReturning() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("terminal-settlement.sock").path
                let server = SocketServer(path: path, wireBuild: "terminal-settlement", trust: .sameEffectiveUser) { request in
                    if request.operation == "terminal" {
                        return .terminal(SocketTerminal())
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(
                    path: path,
                    wireBuild: "terminal-settlement",
                    trust: .sameEffectiveUser
                )
                cleanup.add { await client.close() }
                let call = try await client.open(operation: "terminal", endInput: false)
                _ = try await call.response()
                await #expect(throws: SessionTransportError.self) {
                    try await call.sendChunk(Data("late".utf8))
                }
                await #expect(throws: SessionTransportError.self) {
                    try await call.closeSend()
                }
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
            }
        }

        @Test func terminalSettlementCancelsPrewriteRequestChunk() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("send-settlement.sock").path
                let handlerGate = AsyncGate()
                let streamChunkObserved = CompletionProbe()
                let server = SocketServer(path: path, wireBuild: "send-settlement", trust: .sameEffectiveUser) { request in
                    if request.operation == "upload" {
                        Task {
                            do {
                                for try await _ in request.chunks {
                                    await streamChunkObserved.finish()
                                    return
                                }
                            } catch {}
                        }
                        await handlerGate.wait()
                    }
                    if request.operation == "upload" {
                        return .terminal(SocketTerminal())
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "send-settlement", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let sendGate = AsyncGate()
                let drainWaiting = CompletionProbe()
                let responseDone = CompletionProbe()
                client.requestSendHook = { await sendGate.wait() }
                client.requestSendDrainWaitHook = { Task { await drainWaiting.finish() } }
                let call = try await client.open(operation: "upload", endInput: false)
                let sending = Task { try await call.closeSend() }
                await sendGate.waitUntilEntered()
                let response = Task {
                    let terminal = try await call.response()
                    await responseDone.finish()
                    return terminal
                }
                await handlerGate.waitUntilEntered()
                await handlerGate.release()
                await drainWaiting.waitUntilFinished()
                #expect(await !(responseDone.value()))
                await sendGate.release()
                await #expect(throws: CancellationError.self) { try await sending.value }
                _ = try await response.value
                client.requestSendHook = nil
                client.requestSendDrainWaitHook = nil
                await #expect(throws: SessionTransportError.self) {
                    try await call.sendChunk(Data("late".utf8))
                }
                await #expect(throws: SessionTransportError.self) {
                    try await call.closeSend()
                }
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
                #expect(await !(streamChunkObserved.value()))
            }
        }

        @Test func callerCancellationAfterStreamCommitKeepsSequenceContiguous() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("stream-commit-cancel.sock").path
                let server = SocketServer(
                    path: path,
                    wireBuild: "stream-commit-cancel",
                    trust: .sameEffectiveUser
                ) { request in
                    if request.operation == "upload" {
                        do {
                            var count = 0
                            for try await chunk in request.chunks {
                                count += 1
                                if chunk.end {
                                    break
                                }
                            }
                            return .terminal(SocketTerminal(payload: Data(String(count).utf8)))
                        } catch {
                            return .terminal(SocketTerminal(error: String(describing: error)))
                        }
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(
                    path: path,
                    wireBuild: "stream-commit-cancel",
                    trust: .sameEffectiveUser
                )
                cleanup.add { await client.close() }
                let committing = OneShotGate()
                client.requestSendHook = { await committing.wait() }
                let call = try await client.open(operation: "upload", endInput: false)
                let first = Task { try await call.sendChunk(Data("first".utf8)) }
                await committing.waitUntilEntered()
                first.cancel()
                await committing.release()
                try await first.value
                try await call.sendChunk(Data("second".utf8))
                try await call.closeSend()
                let terminal = try await call.response()
                #expect(terminal.payload == Data("3".utf8))
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
            }
        }

        @Test func closeAwaitsWinningRequestSettlement() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("close-settlement.sock").path
                let server = SocketServer(path: path, wireBuild: "close-settlement", trust: .sameEffectiveUser) { _ in
                    .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "close-settlement", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let settling = AsyncGate()
                let coAwaiting = AsyncGate()
                let closeDone = CompletionProbe()
                client.requestSettlementHook = { await settling.wait() }
                client.requestSettlementWaitHook = { await coAwaiting.wait() }
                let call = try await client.open(operation: "terminal")
                let response = Task { try await call.response() }
                await settling.waitUntilEntered()
                let closing = Task {
                    await client.close()
                    await closeDone.finish()
                }
                await coAwaiting.waitUntilEntered()
                #expect(await !(closeDone.value()))
                await settling.release()
                await coAwaiting.release()
                _ = try await response.value
                await closing.value
                client.requestSettlementHook = nil
                client.requestSettlementWaitHook = nil
            }
        }
    }

    @Suite(.timeLimit(.minutes(1)))
    struct SessionCancellationRaceTests {
        @Test func cancellationDuringResponseDeliveryKeepsSessionHealthy() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("stream-cancel.sock").path
                let chunks = SingleChunkSource()
                let server = SocketServer(path: path, wireBuild: "stream-cancel", trust: .sameEffectiveUser) { request in
                    if request.operation == "stream" {
                        return .stream(SocketResponseStream(
                            nextChunk: { await chunks.next() },
                            terminal: { SocketTerminal() },
                            cancel: {}
                        ))
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "stream-cancel", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let delivering = AsyncGate()
                client.receiveStreamOfferHook = { await delivering.wait() }
                let call = try await client.open(operation: "stream")
                await delivering.waitUntilEntered()
                await call.cancel()
                client.receiveStreamOfferHook = nil
                await delivering.release()
                _ = try await call.response()
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
            }
        }

        @Test func cancellationDiscardsInboundWhileOutboundSendSettles() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("duplex-cancel.sock").path
                let chunks = SingleChunkSource()
                let server = SocketServer(path: path, wireBuild: "duplex-cancel", trust: .sameEffectiveUser) { request in
                    if request.operation == "stream" {
                        return .stream(SocketResponseStream(
                            nextChunk: { await chunks.next() },
                            terminal: { SocketTerminal() },
                            cancel: {}
                        ))
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "duplex-cancel", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let sending = AsyncGate()
                let delivering = AsyncGate()
                let drainWaiting = CompletionProbe()
                client.requestSendHook = { await sending.wait() }
                client.requestSendDrainWaitHook = { Task { await drainWaiting.finish() } }
                client.receiveStreamOfferHook = { await delivering.wait() }
                let call = try await client.open(operation: "stream", endInput: false)
                let send = Task { try await call.sendChunk(Data("late".utf8)) }
                await sending.waitUntilEntered()
                await delivering.waitUntilEntered()
                let cancel = Task { await call.cancel() }
                await drainWaiting.waitUntilFinished()
                await delivering.release()
                await sending.release()
                await #expect(throws: CancellationError.self) { try await send.value }
                await cancel.value
                _ = try await call.response()
                client.requestSendHook = nil
                client.requestSendDrainWaitHook = nil
                client.receiveStreamOfferHook = nil
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
            }
        }

        @Test func terminalSettlementWinsAgainstEligibleCancellationTimeout() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("timeout-race.sock").path
                let handlerGate = AsyncGate()
                let server = SocketServer(path: path, wireBuild: "timeout-race", trust: .sameEffectiveUser) { request in
                    if request.operation == "wait" {
                        await handlerGate.wait()
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(
                    path: path,
                    wireBuild: "timeout-race",
                    configuration: .init(cancellationSettlementTimeout: 0.001),
                    trust: .sameEffectiveUser
                )
                cleanup.add { await client.close() }
                let timeoutGate = AsyncGate()
                let timeoutResult = BooleanProbe()
                client.cancellationTimeoutHook = { await timeoutGate.wait() }
                client.cancellationTimeoutResultHook = { await timeoutResult.record($0) }
                let call = try await client.open(operation: "wait")
                await handlerGate.waitUntilEntered()
                await call.cancel()
                await timeoutGate.waitUntilEntered()
                await handlerGate.release()
                _ = try await call.response()
                await timeoutGate.release()
                #expect(await timeoutResult.value() == false)
                client.cancellationTimeoutHook = nil
                client.cancellationTimeoutResultHook = nil
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
            }
        }
    }

    @Suite(.timeLimit(.minutes(1)))
    struct SessionCreditSettlementTests {
        @Test func canceledRequestOfferDoesNotBecomeWindowOverflow() async throws {
            let offering = AsyncGate()
            let state = ServerRequestState(capacity: 1) { await offering.wait() }
            let receive = Task {
                await state.receive(SessionFrame(kind: .stream, id: 1, sequence: 0, payload: Data("chunk".utf8)))
            }
            await offering.waitUntilEntered()
            await state.cancel()
            await offering.release()
            await receive.value
            #expect(await state.error() == nil)
            #expect(try await state.channel.next(onCancel: {}) == nil)
        }

        @Test func terminalSettlementReleasesAnAdmittedNoCreditSender() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("credit-settlement.sock").path
                let handlerGate = AsyncGate()
                let server = SocketServer(
                    path: path,
                    wireBuild: "credit-settlement",
                    configuration: .init(streamQueueDepth: 1),
                    trust: .sameEffectiveUser
                ) { request in
                    if request.operation == "upload" {
                        await handlerGate.wait()
                        return .terminal(SocketTerminal())
                    }
                    return .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, wireBuild: "credit-settlement", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let admissions = InvocationProbe()
                client.requestSendAdmissionHook = { await admissions.record() }
                let call = try await client.open(operation: "upload", endInput: false)
                try await call.sendChunk(Data("first".utf8))
                let noCreditSend = Task { try await call.sendChunk(Data("second".utf8)) }
                await admissions.wait(for: 2)
                await handlerGate.waitUntilEntered()
                await handlerGate.release()
                _ = try await call.response()
                await #expect(throws: CancellationError.self) { try await noCreditSend.value }
                let payload = Data(#"{"healthy":true}"#.utf8)
                #expect(try await client.call(operation: "echo", payload: payload).payload == payload)
            }
        }
    }

    @Suite(.timeLimit(.minutes(1)))
    struct SessionWriterRuntimeTests {
        @Test func queuedWriterCancellationEmitsNoFrame() async throws {
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
            let flags = fcntl(descriptors[1], F_GETFL)
            try #require(flags >= 0)
            try #require(fcntl(descriptors[1], F_SETFL, flags | O_NONBLOCK) == 0)
            let writeCodec = SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 2)
            let readCodec = SessionFrameCodec(descriptor: descriptors[1])
            let admissionProbe = FrameStartProbe()
            let startProbe = FrameStartProbe()
            let writer = SessionWriter(
                codec: writeCodec,
                maximumPendingWrites: 2,
                label: "writer-test",
                admissionHook: { frame in Task { await admissionProbe.record(frame.id) } },
                startHook: { frame in Task { await startProbe.record(frame.id) } }
            )
            let firstFrame = SessionFrame(
                kind: .stream,
                id: 1,
                sequence: 0,
                payload: Data(repeating: 0xA5, count: 512 * 1024)
            )
            let secondFrame = SessionFrame(kind: .stream, id: 2, sequence: 0, payload: Data("second".utf8))
            let first = Task { try await writer.write(firstFrame) }
            await startProbe.wait(for: 1)
            first.cancel()
            let second = Task { try await writer.write(secondFrame) }
            await admissionProbe.wait(for: 2)
            second.cancel()
            let readQueue = DispatchQueue(label: "com.yasyf.daemonkit.tests.writer-read")
            let received = Task { try await readQueue.performIO { try readCodec.read(timeout: 2) } }
            try await first.value
            await #expect(throws: CancellationError.self) { try await second.value }
            #expect(try await received.value.id == 1)
            #expect(throws: SessionTransportError.self) { _ = try readCodec.read(timeout: 0.02) }
            writer.abort()
            await writer.drain()
        }

        @Test func firstWriterFailureRejectsQueuedFrames() async throws {
            var descriptors: [Int32] = [-1, -1]
            try #require(socketpair(AF_UNIX, SOCK_STREAM, 0, &descriptors) == 0)
            defer { Darwin.close(descriptors[0]) }
            var sendBuffer: Int32 = 4096
            setsockopt(
                descriptors[0],
                SOL_SOCKET,
                SO_SNDBUF,
                &sendBuffer,
                socklen_t(MemoryLayout<Int32>.size)
            )
            let admissionProbe = FrameStartProbe()
            let startProbe = FrameStartProbe()
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 2),
                maximumPendingWrites: 1,
                label: "writer-failure-test",
                admissionHook: { frame in Task { await admissionProbe.record(frame.id) } },
                startHook: { frame in Task { await startProbe.record(frame.id) } }
            )
            let first = Task {
                try await writer.write(SessionFrame(
                    kind: .stream,
                    id: 1,
                    payload: Data(repeating: 0xA5, count: 512 * 1024)
                ))
            }
            await startProbe.wait(for: 1)
            let second = Task {
                try await writer.write(SessionFrame(kind: .stream, id: 2, payload: Data("second".utf8)))
            }
            await admissionProbe.wait(for: 2)
            Darwin.close(descriptors[1])
            await #expect(throws: SessionTransportError.self) { try await first.value }
            await #expect(throws: SessionTransportError.self) { try await second.value }
            #expect(await !(startProbe.contains(2)))
            await writer.drain()
        }

        @Test func settlementWriteBypassesSaturatedOrdinaryQueueWithoutStarvingIt() async throws {
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
            let admissions = FrameStartProbe()
            let starts = FrameStartProbe()
            let writer = SessionWriter(
                codec: SessionFrameCodec(descriptor: descriptors[0], writeTimeout: 2),
                maximumPendingWrites: 4,
                label: "writer-priority-test",
                admissionHook: { frame in Task { await admissions.record(frame.id) } },
                startHook: { frame in Task { await starts.record(frame.id) } }
            )
            let first = Task {
                try await writer.write(SessionFrame(
                    kind: .stream,
                    id: 1,
                    payload: Data(repeating: 0xA5, count: 512 * 1024)
                ))
            }
            await starts.wait(for: 1)
            let ordinary = Task {
                try await writer.write(SessionFrame(kind: .stream, id: 2, payload: Data("ordinary".utf8)))
            }
            await admissions.wait(for: 2)
            let settlement = Task {
                try await writer.writeSettlement(SessionFrame(kind: .cancel, flags: .end, id: 3))
            }
            await admissions.wait(for: 3)
            let readQueue = DispatchQueue(label: "com.yasyf.daemonkit.tests.writer-priority-read")
            let readDescriptor = descriptors[1]
            let received = Task {
                try await readQueue.performIO {
                    let codec = SessionFrameCodec(descriptor: readDescriptor)
                    return try (0 ..< 3).map { _ in try codec.read(timeout: 2).id }
                }
            }
            try await first.value
            try await settlement.value
            try await ordinary.value
            #expect(try await received.value == [1, 3, 2])
            writer.abort()
            await writer.drain()
        }
    }

    @Suite(.timeLimit(.minutes(1)))
    struct SessionBootstrapLifetimeTests {
        @Test func canceledHandshakeClosesTheAcceptedDescriptor() async throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("handshake.sock").path
            var address = try #require(makeAddress(path: path))
            let listener = socket(AF_UNIX, SOCK_STREAM, 0)
            try #require(listener >= 0)
            defer { Darwin.close(listener) }
            try #require(withUnsafePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Darwin.bind(listener, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
                }
            } == 0)
            try #require(listen(listener, 1) == 0)
            let peerQueue = DispatchQueue(label: "com.yasyf.daemonkit.tests.handshake-peer")
            let accepted = Task {
                try await peerQueue.performIO { () throws -> Int32 in
                    let peer = accept(listener, nil, nil)
                    guard peer >= 0 else {
                        throw SessionTransportError.systemCall(operation: "accept", errno: errno)
                    }
                    do {
                        let hello = try SessionFrameCodec(descriptor: peer).read()
                        guard hello.kind == .hello else {
                            throw SessionTransportError.handshake("expected client hello")
                        }
                        return peer
                    } catch {
                        Darwin.close(peer)
                        throw error
                    }
                }
            }
            let connecting = Task {
                try await SocketClient(
                    path: path,
                    wireBuild: "handshake",
                    configuration: .init(handshakeTimeout: 5),
                    trust: .sameEffectiveUser
                )
            }
            let peer = try await accepted.value
            defer { Darwin.close(peer) }
            connecting.cancel()
            await #expect(throws: CancellationError.self) { try await connecting.value }
            let observedEOF = Task {
                try await peerQueue.performIO {
                    var buffer = [UInt8](repeating: 0, count: 4096)
                    while true {
                        var descriptor = pollfd(fd: peer, events: Int16(POLLIN | POLLHUP), revents: 0)
                        guard poll(&descriptor, 1, 1000) > 0 else { return false }
                        let count = read(peer, &buffer, buffer.count)
                        if count == 0 {
                            return true
                        }
                        if count < 0, errno != EINTR {
                            return false
                        }
                    }
                }
            }
            #expect(try await observedEOF.value)
        }

        @Test func droppingCanceledClientClosesPeerWithoutWaitingForSettlementTimer() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("drop-client.sock").path
                let handlerGate = AsyncGate()
                let capture = SocketSessionCapture()
                let server = SocketServer(path: path, wireBuild: "drop-client", trust: .sameEffectiveUser) { request in
                    await capture.record(request.session)
                    await handlerGate.wait()
                    return .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                var client: SocketClient? = try await SocketClient(
                    path: path,
                    wireBuild: "drop-client",
                    configuration: .init(cancellationSettlementTimeout: 3600),
                    trust: .sameEffectiveUser
                )
                weak let weakClient = client
                var call: SocketCall? = try await client?.open(operation: "wait")
                await handlerGate.waitUntilEntered()
                let session = await capture.value()
                await call?.cancel()
                call = nil
                client = nil
                await session.waitUntilClosed()
                #expect(weakClient == nil)
                await handlerGate.release()
            }
        }
    }
}
