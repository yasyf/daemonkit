import Darwin
import Foundation

/// SessionServiceTrust is the exact local-peer trust policy of a session service.
public enum SessionServiceTrust: Sendable {
    case sameEffectiveUser
}

/// SessionServiceConfiguration bounds every session-service transport resource.
public struct SessionServiceConfiguration: Sendable {
    public let maximumFrameBytes: Int
    public let maximumRequestBytes: Int
    public let maximumActiveRequests: Int
    public let maximumSessions: Int
    public let streamQueueDepth: Int
    public let maximumPendingWrites: Int
    public let handshakeTimeout: TimeInterval
    public let writeTimeout: TimeInterval

    public init(
        maximumFrameBytes: Int,
        maximumRequestBytes: Int,
        maximumActiveRequests: Int,
        maximumSessions: Int,
        streamQueueDepth: Int,
        maximumPendingWrites: Int,
        handshakeTimeout: TimeInterval,
        writeTimeout: TimeInterval
    ) {
        self.maximumFrameBytes = maximumFrameBytes
        self.maximumRequestBytes = maximumRequestBytes
        self.maximumActiveRequests = maximumActiveRequests
        self.maximumSessions = maximumSessions
        self.streamQueueDepth = streamQueueDepth
        self.maximumPendingWrites = maximumPendingWrites
        self.handshakeTimeout = handshakeTimeout
        self.writeTimeout = writeTimeout
    }
}

/// SessionServiceCodec is the exact typed request and response codec of one service.
public struct SessionServiceCodec<Request: Sendable, Response: Sendable>: Sendable {
    let decode: @Sendable (Data) throws -> Request
    let encode: @Sendable (Response) throws -> Data

    public init(
        decodeRequest: @escaping @Sendable (Data) throws -> Request,
        encodeResponse: @escaping @Sendable (Response) throws -> Data
    ) {
        decode = decodeRequest
        encode = encodeResponse
    }
}

/// SessionServiceHandler defines one exact typed operation and tenant route.
public struct SessionServiceHandler<Request: Sendable, Response: Sendable>: Sendable {
    let operation: String
    let tenant: String
    let codec: SessionServiceCodec<Request, Response>
    let handle: @Sendable (Request) async -> Response

    public init(
        operation: String,
        tenant: String,
        codec: SessionServiceCodec<Request, Response>,
        handle: @escaping @Sendable (Request) async -> Response
    ) {
        self.operation = operation
        self.tenant = tenant
        self.codec = codec
        self.handle = handle
    }
}

/// SessionServiceRuntimeError reports an invalid or conflicting service lifetime.
public enum SessionServiceRuntimeError: Error, Equatable, Sendable {
    case emptyPath
    case emptyWireBuild
    case emptyRuntimeBuild
    case emptyRole
    case emptyOperation
    case reservedOperation
    case invalidLimit(String)
    case startAlreadyIssued
    case startInProgress
}

/// SessionServiceRuntimeResult is the retained terminal result of one service generation.
public enum SessionServiceRuntimeResult: @unchecked Sendable {
    case failed(any Error)
    case draining
}

/// StaticSessionServiceRuntime owns one exact typed local service generation.
public final class StaticSessionServiceRuntime<Request: Sendable, Response: Sendable>: @unchecked Sendable {
    private enum State {
        case idle
        case starting
        case running
        case shuttingDown(Task<Void, Error>)
        case stopped
    }

    public let identity: RuntimeIdentity

    private let runtime: DaemonRuntime
    private let slot: RuntimePublicationSlot<SessionServiceRoute<Request, Response>>
    private let route: SessionServiceRoute<Request, Response>
    private let lock = NSLock()
    private var state = State.idle

    public init(
        path: String,
        wireBuild: String,
        runtimeBuild: String,
        role: String,
        trust: SessionServiceTrust,
        configuration: SessionServiceConfiguration,
        handler: SessionServiceHandler<Request, Response>
    ) throws {
        try Self.validate(
            path: path,
            wireBuild: wireBuild,
            runtimeBuild: runtimeBuild,
            role: role,
            configuration: configuration,
            operation: handler.operation
        )
        let generation = try OwnerGeneration(
            UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
        )
        let identity = RuntimeIdentity(
            runtimeBuild: runtimeBuild,
            processGeneration: generation
        )
        let route = SessionServiceRoute(
            maximumRequestBytes: configuration.maximumRequestBytes,
            codec: handler.codec,
            handle: handler.handle
        )
        let dispatch = SessionServiceDispatch<Request, Response>()
        let effectiveUserID: uid_t
        switch trust {
        case .sameEffectiveUser:
            effectiveUserID = geteuid()
        }
        let runtime = try DaemonRuntime(
            path: path,
            wireBuild: wireBuild,
            identity: identity,
            configuration: SocketServer.Configuration(
                maximumFrameBytes: configuration.maximumFrameBytes,
                maximumActiveRequests: configuration.maximumActiveRequests,
                maximumSessions: configuration.maximumSessions,
                streamQueueDepth: configuration.streamQueueDepth,
                maximumPendingWrites: configuration.maximumPendingWrites,
                handshakeTimeout: configuration.handshakeTimeout,
                writeTimeout: configuration.writeTimeout
            ),
            sessionPolicy: SocketServer.SessionPolicy(
                effectiveUserID: effectiveUserID,
                role: role,
                operation: handler.operation,
                tenant: handler.tenant
            ),
            handler: RuntimeHandlerSpec { request in
                await dispatch.handle(request)
            }
        )
        let slot: RuntimePublicationSlot<SessionServiceRoute<Request, Response>> = runtime.newPublicationSlot()
        dispatch.install(slot)
        self.identity = identity
        self.runtime = runtime
        self.slot = slot
        self.route = route
    }

    /// Binds the listener and atomically publishes Ready exactly once.
    public func start(deadline: Date) async throws {
        let allowed = lock.withLock { () -> Bool in
            guard case .idle = state else { return false }
            state = .starting
            return true
        }
        guard allowed else { throw SessionServiceRuntimeError.startAlreadyIssued }
        do {
            let activation = try await runtime.begin(deadline: deadline)
            let publication = try slot.stage(activation, value: route)
            try activation.commitReady(publication)
            lock.withLock { state = .running }
        } catch {
            lock.withLock { state = .stopped }
            throw error
        }
    }

    /// Waits for the retained terminal result of this service generation.
    public func wait() async -> SessionServiceRuntimeResult {
        switch await runtime.wait() {
        case let .failed(error):
            return .failed(error)
        case .draining:
            return .draining
        }
    }

    /// Drains, settles, unlinks, and stops this service by the deadline.
    public func shutdown(deadline: Date) async throws {
        let acquired = lock.withLock { () -> (Task<Void, Error>?, Bool) in
            switch state {
            case .idle:
                state = .stopped
                return (nil, false)
            case .starting:
                return (nil, true)
            case .running:
                let task = Task { try await self.runtime.shutdown(deadline: deadline) }
                state = .shuttingDown(task)
                return (task, false)
            case let .shuttingDown(task):
                return (task, false)
            case .stopped:
                return (nil, false)
            }
        }
        guard !acquired.1 else { throw SessionServiceRuntimeError.startInProgress }
        guard let task = acquired.0 else { return }
        do {
            try await task.value
            lock.withLock { state = .stopped }
        } catch {
            lock.withLock {
                if case .shuttingDown = state {
                    state = .running
                }
            }
            throw error
        }
    }
}

private extension StaticSessionServiceRuntime {
    static func validate(
        path: String,
        wireBuild: String,
        runtimeBuild: String,
        role: String,
        configuration: SessionServiceConfiguration,
        operation: String
    ) throws {
        guard !path.isEmpty else { throw SessionServiceRuntimeError.emptyPath }
        guard !wireBuild.isEmpty else { throw SessionServiceRuntimeError.emptyWireBuild }
        guard !runtimeBuild.isEmpty else { throw SessionServiceRuntimeError.emptyRuntimeBuild }
        guard !role.isEmpty else { throw SessionServiceRuntimeError.emptyRole }
        guard !operation.isEmpty else { throw SessionServiceRuntimeError.emptyOperation }
        guard !operation.hasPrefix("daemon.") else { throw SessionServiceRuntimeError.reservedOperation }
        guard configuration.maximumFrameBytes > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("maximumFrameBytes")
        }
        guard configuration.maximumRequestBytes > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("maximumRequestBytes")
        }
        guard configuration.maximumActiveRequests > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("maximumActiveRequests")
        }
        guard configuration.maximumSessions > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("maximumSessions")
        }
        guard (1 ... Int(UInt32.max)).contains(configuration.streamQueueDepth) else {
            throw SessionServiceRuntimeError.invalidLimit("streamQueueDepth")
        }
        guard configuration.maximumPendingWrites > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("maximumPendingWrites")
        }
        guard configuration.handshakeTimeout.isFinite, configuration.handshakeTimeout > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("handshakeTimeout")
        }
        guard configuration.writeTimeout.isFinite, configuration.writeTimeout > 0 else {
            throw SessionServiceRuntimeError.invalidLimit("writeTimeout")
        }
    }
}

private struct SessionServiceRoute<Request: Sendable, Response: Sendable>: Sendable {
    let maximumRequestBytes: Int
    let codec: SessionServiceCodec<Request, Response>
    let handle: @Sendable (Request) async -> Response

    func call(_ request: SocketRequest) async -> SocketResponse {
        do {
            var payload = request.payload
            guard payload.count <= maximumRequestBytes else {
                return rejected(.requestTooLarge, "wire: request exceeds service limit")
            }
            for try await chunk in request.chunks {
                try Task.checkCancellation()
                guard payload.count <= maximumRequestBytes - chunk.payload.count else {
                    return rejected(.requestTooLarge, "wire: request exceeds service limit")
                }
                payload.append(chunk.payload)
            }
            try Task.checkCancellation()
            let decoded: Request
            do {
                decoded = try codec.decode(payload)
            } catch {
                return rejected(.invalidRequest, "wire: request decode failed")
            }
            let response = await handle(decoded)
            try Task.checkCancellation()
            do {
                return .terminal(SocketTerminal(payload: try codec.encode(response)))
            } catch {
                return .terminal(SocketTerminal(error: "daemonkit: response encode failed"))
            }
        } catch is CancellationError {
            return .terminal(SocketTerminal(error: "wire: request canceled"))
        } catch {
            return .terminal(SocketTerminal(error: "wire: request stream failed"))
        }
    }

    private func rejected(_ code: SocketResponseCode, _ reason: String) -> SocketResponse {
        .terminal(SocketTerminal(rejected: true, code: code, reason: reason))
    }
}

private final class SessionServiceDispatch<Request: Sendable, Response: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var slot: RuntimePublicationSlot<SessionServiceRoute<Request, Response>>?

    func install(_ slot: RuntimePublicationSlot<SessionServiceRoute<Request, Response>>) {
        lock.withLock { self.slot = slot }
    }

    func handle(_ request: SocketRequest) async -> SocketResponse {
        guard let route = lock.withLock({ slot?.loadPinned(from: request) }) else {
            return .terminal(SocketTerminal(
                rejected: true,
                code: .runtimeStarting,
                reason: "wire: runtime publication unavailable"
            ))
        }
        return await route.call(request)
    }
}
