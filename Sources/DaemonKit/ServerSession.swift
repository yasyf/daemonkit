import Darwin
import Foundation

final class ServerSession: @unchecked Sendable {
    let descriptor: Int32
    private let serverWireBuild: String
    private let peer: SocketPeer
    private let configuration: SocketServer.Configuration
    private let handler: @Sendable (SocketRequest) async -> SocketResponse
    private let runtimeLifecycle: RuntimeLifecycleController?
    private let controlOperations: Set<String>
    private let sessionPolicy: SocketServer.SessionPolicy?
    private let shutdownDescriptor: @Sendable () -> Void
    private let codec: SessionFrameCodec
    private let readQueue: DispatchQueue
    private let writer: SessionWriter
    private let lifecyclePublisher: ServerLifecyclePublisher
    private let eventWindow = SocketCreditWindow()
    private let lifecycle = SocketSessionLifecycle()
    private let generation: Data
    private let lock = NSLock()
    private var active: [UInt64: ServerRequestState] = [:]
    private var seen: Set<UInt64> = []
    private var watermark: UInt64 = 0
    private var closed = false

    init(
        descriptor: Int32,
        shutdown: @escaping @Sendable () -> Void,
        wireBuild: String,
        peer: SocketPeer,
        configuration: SocketServer.Configuration,
        runtimeLifecycle: RuntimeLifecycleController?,
        controlOperations: Set<String>,
        sessionPolicy: SocketServer.SessionPolicy?,
        handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
    ) {
        self.descriptor = descriptor
        shutdownDescriptor = shutdown
        serverWireBuild = wireBuild
        self.peer = peer
        self.configuration = configuration
        self.runtimeLifecycle = runtimeLifecycle
        self.controlOperations = controlOperations
        self.sessionPolicy = sessionPolicy
        self.handler = handler
        var uuid = UUID().uuid
        generation = withUnsafeBytes(of: &uuid) { Data($0) }
        codec = SessionFrameCodec(
            descriptor: descriptor,
            maximumFrameBytes: configuration.maximumFrameBytes,
            writeTimeout: configuration.writeTimeout
        )
        let sessionWriter = SessionWriter(
            codec: codec,
            maximumPendingWrites: configuration.maximumPendingWrites,
            label: "com.yasyf.daemonkit.SocketServer.write.\(descriptor)"
        )
        writer = sessionWriter
        lifecyclePublisher = ServerLifecyclePublisher(
            wireBuild: wireBuild,
            submit: { payload in
                sessionWriter.enqueueLifecycle(SessionFrame(
                    kind: .lifecycle,
                    flags: .end,
                    payload: payload
                ))
            },
            closeSession: {
                sessionWriter.abort()
                shutdown()
            }
        )
        readQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.read.\(descriptor)")
    }

    deinit {
        lifecycle.close()
    }

    func run() async throws {
        do {
            let identity = try await handshake()
            while true {
                let frame = try await read()
                switch frame.kind {
                case .request:
                    try await receiveRequest(frame, identity: identity)
                case .cancel:
                    try await receiveCancel(frame)
                case .stream:
                    try await receiveStream(frame)
                case .window:
                    try await receiveWindow(frame)
                case .acknowledgment:
                    try await receiveAcknowledgement(frame)
                case .goAway:
                    await eventWindow.close()
                    await settleRequests()
                    try await writeSettlement(SessionFrame(kind: .goAway, flags: .end))
                    close()
                    return
                default:
                    throw SessionTransportError.invalidFrame("client frame kind \(frame.kind)")
                }
            }
        } catch {
            close()
            await eventWindow.close()
            await settleRequests()
            throw error
        }
    }

    func write(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock { closed }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try await writer.write(frame)
    }

    func writeSettlement(_ frame: SessionFrame) async throws {
        let isClosed = lock.withLock { closed }
        if isClosed {
            throw SessionTransportError.disconnected
        }
        try await writer.writeSettlement(frame)
    }

    func pushEvent(topic: String, payload: Data) async throws {
        guard await eventWindow.acquire() else { throw CancellationError() }
        try await write(SessionFrame(kind: .event, flags: .end, operation: topic, payload: payload))
    }

    func pushLifecycle(_ payload: Data) async throws {
        _ = try enqueueLifecycle(payload)
    }

    func enqueueLifecycle(_ payload: Data) throws -> LifecycleWriteReceipt? {
        guard !payload.isEmpty else { throw SessionTransportError.invalidFrame("empty lifecycle payload") }
        return try lifecyclePublisher.publish(payload)
    }

    func close() {
        lock.lock()
        guard !closed else {
            lock.unlock()
            return
        }
        closed = true
        lock.unlock()
        lifecycle.close()
        lifecyclePublisher.finish()
        writer.abort()
        Task { await eventWindow.close() }
        shutdownDescriptor()
    }

    func drainIO() async {
        writer.abort()
        shutdownDescriptor()
        await withCheckedContinuation { continuation in
            writer.afterDrained { [readQueue] in
                readQueue.async {
                    continuation.resume()
                }
            }
        }
    }

    private func handshake() async throws -> SessionHelloIdentity {
        let frame = try await read(timeout: configuration.handshakeTimeout)
        guard frame.kind == .hello, frame.flags == .end, frame.id == 0,
              frame.sequence == 0, frame.operation.isEmpty, frame.tenant.isEmpty
        else {
            throw SessionTransportError.handshake("invalid hello")
        }
        let identity = try SessionHandshakeCodec.decodeHello(frame.payload)
        guard identity.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(identity.protocolVersion)
        }
        guard !identity.wireBuild.isEmpty else {
            throw SessionTransportError.handshake("empty wireBuild")
        }
        guard !identity.role.isEmpty else {
            throw SessionTransportError.handshake("empty role")
        }
        if let sessionPolicy {
            guard identity.role == sessionPolicy.role else {
                try await rejectHandshake(
                    code: .permissionDenied,
                    reason: "wire: peer role does not match service role"
                )
                throw SocketHandshakeRejectionError(
                    code: .permissionDenied,
                    reason: "wire: peer role does not match service role"
                )
            }
        }
        guard identity.wireBuild == serverWireBuild else {
            let payload = try SessionHandshakeCodec.encodeRejection(
                wireBuild: serverWireBuild,
                code: .buildMismatch,
                reason: "wire: client build does not match server build"
            )
            try await write(SessionFrame(kind: .helloAck, flags: .end, payload: payload))
            throw SocketWireBuildMismatchError(server: serverWireBuild, client: identity.wireBuild)
        }
        let payload = try SessionHandshakeCodec.encodeSuccess(wireBuild: serverWireBuild, session: generation)
        try await write(SessionFrame(kind: .helloAck, flags: .end, payload: payload))
        return identity
    }

    private func rejectHandshake(code: SocketResponseCode, reason: String) async throws {
        let payload = try SessionHandshakeCodec.encodeRejection(
            wireBuild: serverWireBuild,
            code: code,
            reason: reason
        )
        try await write(SessionFrame(kind: .helloAck, flags: .end, payload: payload))
    }

    private func read(timeout: TimeInterval = 0) async throws -> SessionFrame {
        try await readQueue.performIO {
            try self.codec.read(timeout: timeout)
        }
    }

    private enum Admission {
        case accepted(ServerRequestState, RuntimeAdmissionPin?)
        case rejected(code: SocketResponseCode, reason: String)
    }

    private func receiveRequest(_ frame: SessionFrame, identity: SessionHelloIdentity) async throws {
        guard frame.id != 0, !frame.operation.isEmpty, frame.sequence == 0 else {
            throw SessionTransportError.invalidFrame("request")
        }
        let control = controlOperations.contains(frame.operation)
        guard try await validateRoute(frame, control: control) else { return }
        let admission = try admit(
            frame,
            clientWireBuild: identity.wireBuild,
            control: control
        )
        guard case let .accepted(state, runtimeAdmission) = admission else {
            guard case let .rejected(code, reason) = admission else { return }
            try await sendRejected(id: frame.id, code: code, reason: reason)
            return
        }
        if frame.flags.contains(.end) {
            await state.finishInitialInput()
        }

        let chunks = SocketChunkStream(channel: state.channel) {
            Task { await state.cancel() }
        } consumptionOperation: { [weak self] _ in
            guard let self else { throw SessionTransportError.disconnected }
            try await writeSettlement(SessionFrame(kind: .window, id: frame.id, sequence: 1))
        }
        try await write(SessionFrame(
            kind: .window,
            id: frame.id,
            sequence: UInt32(configuration.streamQueueDepth)
        ))
        let publicSession = SocketSession(implementation: self, lifecycle: lifecycle)
        let request = SocketRequest(
            id: frame.id,
            operation: frame.operation,
            tenant: frame.tenant,
            payload: frame.payload,
            chunks: chunks,
            peer: peer,
            peerWireBuild: identity.wireBuild,
            peerRole: identity.role,
            session: publicSession,
            runtimeAdmission: runtimeAdmission
        )
        let task = Task { [weak self] in
            guard let self else { return }
            var response = await handler(request)
            if let transportError = await state.error() {
                await cancelAndSettle(response)
                response = .terminal(SocketTerminal(error: String(describing: transportError)))
            } else if Task.isCancelled {
                await cancelAndSettle(response)
                response = .terminal(SocketTerminal(error: "wire: request canceled"))
            }
            do {
                try await send(response, id: frame.id, state: state)
            } catch {
                socketServerLog.debug("response failed: \(String(describing: error), privacy: .public)")
                if await state.hasSentTerminal() {
                    close()
                } else {
                    do {
                        try await sendTerminal(
                            SocketTerminal(error: "wire: invalid handler response"),
                            id: frame.id,
                            state: state
                        )
                        try await state.waitForTerminalAcknowledgement(
                            timeout: configuration.writeTimeout
                        )
                    } catch {
                        close()
                    }
                }
            }
            await state.finish()
            runtimeAdmission?.revoke()
            remove(frame.id)
        }
        await state.attach(task)
        if frame.deadlineUnixMilliseconds > 0 {
            let interval = SessionTime.remainingMilliseconds(until: frame.deadlineUnixMilliseconds)
            let timer = Task {
                try? await Task.sleep(for: .milliseconds(interval))
                if !Task.isCancelled {
                    task.cancel()
                }
            }
            await state.attachDeadline(timer)
        }
    }

    private func validateRoute(_ frame: SessionFrame, control: Bool) async throws -> Bool {
        let rejection: String? = if frame.operation.hasPrefix("daemon."), !control {
            "wire: Swift sessions cannot authorize protected daemonkit operations"
        } else if control, !frame.tenant.isEmpty {
            "wire: control operation tenant must be empty"
        } else if !control, let sessionPolicy, frame.operation != sessionPolicy.operation {
            "wire: operation does not match service operation"
        } else if !control, let sessionPolicy, frame.tenant != sessionPolicy.tenant {
            "wire: tenant does not match service tenant"
        } else {
            nil
        }
        guard let rejection else { return true }
        try await sendRejected(id: frame.id, code: .permissionDenied, reason: rejection)
        return false
    }
}

private extension ServerSession {
    private func admit(_ frame: SessionFrame, clientWireBuild: String, control: Bool) throws -> Admission {
        let runtimeAdmission: RuntimeAdmissionPin?
        if let runtimeLifecycle {
            let admission = control ? runtimeLifecycle.admitControl() : runtimeLifecycle.admitBusiness()
            switch admission {
            case let .admitted(pin):
                runtimeAdmission = pin
            case let .rejected(code, reason):
                return .rejected(code: code, reason: reason)
            }
        } else {
            runtimeAdmission = nil
        }
        var retainsAdmission = false
        defer {
            if !retainsAdmission {
                runtimeAdmission?.revoke()
            }
        }
        lock.lock()
        defer { lock.unlock() }
        guard frame.id > watermark, !seen.contains(frame.id) else {
            throw SessionTransportError.duplicateRequestID(frame.id)
        }
        guard frame.id - watermark <= UInt64(configuration.maximumActiveRequests),
              active.count < configuration.maximumActiveRequests
        else {
            return .rejected(code: .sessionCapacity, reason: "wire: queue at capacity")
        }
        seen.insert(frame.id)
        while seen.remove(watermark + 1) != nil {
            watermark += 1
        }
        if clientWireBuild != serverWireBuild {
            return .rejected(code: .buildMismatch, reason: "wire: client wireBuild does not match server wireBuild")
        }
        let state = ServerRequestState(capacity: configuration.streamQueueDepth)
        active[frame.id] = state
        retainsAdmission = true
        return .accepted(state, runtimeAdmission)
    }

    private func receiveCancel(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.flags == .end, frame.operation.isEmpty,
              frame.tenant.isEmpty, frame.payload.isEmpty
        else {
            throw SessionTransportError.invalidFrame("cancel")
        }
        await request(frame.id)?.cancel()
    }

    private func receiveStream(_ frame: SessionFrame) async throws {
        guard frame.id != 0, frame.operation.isEmpty, frame.tenant.isEmpty else {
            throw SessionTransportError.invalidFrame("stream")
        }
        await request(frame.id)?.receive(frame)
    }

    private func settleRequests() async {
        let requests = takeRequests()
        for request in requests {
            await request.settle()
        }
    }

    private func takeRequests() -> [ServerRequestState] {
        lock.lock()
        let requests = Array(active.values)
        active.removeAll()
        lock.unlock()
        return requests
    }

    private func send(_ response: SocketResponse, id: UInt64, state: ServerRequestState) async throws {
        switch response {
        case let .terminal(terminal):
            try await sendTerminal(terminal, id: id, state: state)
        case let .stream(stream):
            try await send(stream, id: id, state: state)
        }
        try await state.waitForTerminalAcknowledgement(timeout: configuration.writeTimeout)
    }

    private func send(_ stream: SocketResponseStream, id: UInt64, state: ServerRequestState) async throws {
        let settlement = SocketResponseSettlement(stream: stream)
        var sequence = SessionSequence()
        try await withTaskCancellationHandler {
            do {
                while true {
                    try Task.checkCancellation()
                    guard await state.responseWindow.acquire() else { throw CancellationError() }
                    guard let payload = try await stream.nextChunk() else { break }
                    let current = try sequence.take()
                    try await write(SessionFrame(kind: .stream, id: id, sequence: current, payload: payload))
                }
                let terminal = try await settlement.value().get()
                try Task.checkCancellation()
                try await sendTerminal(terminal, id: id, state: state)
            } catch is CancellationError {
                stream.cancel()
                _ = await settlement.value()
                try await sendTerminal(SocketTerminal(error: "wire: request canceled"), id: id, state: state)
            } catch {
                stream.cancel()
                _ = await settlement.value()
                try await sendTerminal(SocketTerminal(error: String(describing: error)), id: id, state: state)
            }
        } onCancel: {
            stream.cancel()
        }
    }

    private func cancelAndSettle(_ response: SocketResponse) async {
        guard case let .stream(stream) = response else { return }
        stream.cancel()
        _ = await SocketResponseSettlement(stream: stream).value()
    }

    private func sendTerminal(_ terminal: SocketTerminal, id: UInt64, state: ServerRequestState) async throws {
        let envelope = try responseEnvelope(terminal, acknowledge: true)
        try await state.writeTerminal { [self] in
            try await writeSettlement(SessionFrame(kind: .response, flags: .end, id: id, payload: envelope))
        }
        terminal.afterWrite?()
    }

    private func sendRejected(id: UInt64, code: SocketResponseCode, reason: String) async throws {
        try await writeSettlement(SessionFrame(
            kind: .response,
            flags: .end,
            id: id,
            payload: responseEnvelope(SocketTerminal(rejected: true, code: code, reason: reason))
        ))
    }

    private func remove(_ id: UInt64) {
        lock.lock()
        active.removeValue(forKey: id)
        lock.unlock()
    }

    private func responseEnvelope(_ response: SocketTerminal, acknowledge: Bool = false) throws -> Data {
        var members: [String] = []
        if acknowledge {
            members.append("\"ack\":true")
        }
        if response.rejected {
            members.append("\"rejected\":true")
        }
        if let code = response.code {
            try members.append("\"code\":\(jsonString(code.rawValue))")
        }
        if let reason = response.reason {
            try members.append("\"reason\":\(jsonString(reason))")
        }
        if let error = response.error {
            try members.append("\"err\":\(jsonString(error))")
        }
        if let payload = response.payload {
            guard (try? JSONSerialization.jsonObject(with: payload, options: [.fragmentsAllowed])) != nil else {
                throw SessionTransportError.invalidFrame("response payload is not JSON")
            }
            guard let payloadString = String(data: payload, encoding: .utf8) else {
                throw SessionTransportError.invalidFrame("response payload is not UTF-8 JSON")
            }
            members.append("\"payload\":\(payloadString)")
        }
        return Data("{\(members.joined(separator: ","))}".utf8)
    }

    private func jsonString(_ value: String) throws -> String {
        let data = try JSONSerialization.data(withJSONObject: [value])
        guard let array = String(data: data, encoding: .utf8) else {
            throw SessionTransportError.invalidFrame("JSON string encoding is not UTF-8")
        }
        return String(array.dropFirst().dropLast())
    }
}

private extension ServerSession {
    func request(_ id: UInt64) -> ServerRequestState? {
        lock.lock()
        let state = active[id]
        lock.unlock()
        return state
    }

    func receiveWindow(_ frame: SessionFrame) async throws {
        guard frame.flags.isEmpty, frame.sequence > 0, frame.operation.isEmpty,
              frame.tenant.isEmpty, frame.payload.isEmpty
        else { throw SessionTransportError.invalidFrame("response or event window") }
        if frame.id == 0 {
            await eventWindow.grant(frame.sequence)
            return
        }
        await request(frame.id)?.grantResponseCredits(frame.sequence)
    }

    func receiveAcknowledgement(_ frame: SessionFrame) async throws {
        guard frame.flags == .end, frame.id != 0, frame.sequence == 0,
              frame.operation.isEmpty, frame.tenant.isEmpty, frame.payload == generation,
              let state = request(frame.id), await state.acknowledgeTerminal()
        else { throw SessionTransportError.invalidFrame("acknowledgement") }
    }
}
