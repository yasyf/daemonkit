import Foundation

/// RuntimeShutdownError reports a bounded shutdown that could not settle safely.
enum RuntimeShutdownError: Error, Equatable, Sendable {
    case deadlineExceeded
}

/// RuntimeHandlerSpec defines the one Ready-only product handler of a daemon runtime.
struct RuntimeHandlerSpec: Sendable {
    let operation: @Sendable (SocketRequest) async -> SocketResponse

    init(_ operation: @escaping @Sendable (SocketRequest) async -> SocketResponse) {
        self.operation = operation
    }
}

/// DaemonRuntime composes authenticated serving, lifecycle, admission, and publication.
final class DaemonRuntime: @unchecked Sendable {
    let controller: RuntimeLifecycleController
    private let server: SocketServer
    private let lock = NSLock()
    private var began = false
    private var shutDown = false

    let identity: RuntimeIdentity

    init(
        path: String,
        wireBuild: String,
        identity: RuntimeIdentity,
        configuration: SocketServer.Configuration = .init(),
        handler: RuntimeHandlerSpec
    ) throws {
        let controller = try RuntimeLifecycleController(
            wireBuild: wireBuild,
            runtimeIdentity: identity
        )
        self.identity = identity
        self.controller = controller
        server = SocketServer(
            path: path,
            wireBuild: wireBuild,
            configuration: configuration,
            runtimeLifecycle: controller,
            handler: handler.operation
        )
    }

    /// Starts authenticated serving in Starting within one acquisition deadline.
    func begin(deadline: Date) async throws -> RuntimeActivation {
        guard deadline > Date() else { throw RuntimeShutdownError.deadlineExceeded }
        let allowed = lock.withLock {
            guard !began, !shutDown else { return false }
            began = true
            return true
        }
        guard allowed else { throw RuntimeActivationError.activationAlreadyIssued }
        var serverStarted = false
        do {
            try await server.start()
            serverStarted = true
            guard deadline > Date() else { throw RuntimeShutdownError.deadlineExceeded }
            return try controller.beginActivation()
        } catch {
            if serverStarted {
                do {
                    try await server.stopRuntime(deadline: deadline)
                    controller.finishShutdown()
                } catch {
                    lock.withLock { shutDown = true }
                    throw RuntimeShutdownError.deadlineExceeded
                }
            } else {
                controller.finishShutdown()
            }
            throw error
        }
    }

    /// Creates a typed slot bound to this runtime's lifecycle controller.
    func newPublicationSlot<Value: Sendable>(of _: Value.Type = Value.self) -> RuntimePublicationSlot<Value> {
        RuntimePublicationSlot(controller: controller)
    }

    /// Closes business admission, invalidates preparation, and announces Draining.
    func closeIntake() async throws {
        try await controller.closeIntake()
    }

    /// Waits for the retained terminal lifecycle result.
    func wait() async -> RuntimeWaitResult {
        await controller.wait()
    }

    /// Returns the runtime-owned O(1) health and activity snapshot.
    func statusSnapshot() -> RuntimeStatusSnapshot {
        controller.statusSnapshot()
    }

    /// Stops serving by the deadline and clears publication only after complete settlement.
    func shutdown(deadline: Date) async throws {
        let shouldStop = lock.withLock {
            guard !shutDown else { return false }
            shutDown = true
            return true
        }
        guard shouldStop else { return }
        try await controller.closeIntake(deadline: deadline)
        try await server.stopRuntime(deadline: deadline)
        controller.finishShutdown()
    }
}
