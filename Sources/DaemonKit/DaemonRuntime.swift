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
    private var failedStartCleanup: Task<Void, Never>?

    let identity: RuntimeIdentity

    var serverStartCommitHook: (@Sendable () async -> Void)? {
        get { server.startCommitHook }
        set { server.startCommitHook = newValue }
    }

    init(
        path: String,
        wireBuild: String,
        identity: RuntimeIdentity,
        configuration: SocketServer.Configuration = .init(),
        sessionPolicy: SocketServer.SessionPolicy? = nil,
        handler: RuntimeHandlerSpec
    ) throws {
        let controller = try RuntimeLifecycleController(
            wireBuild: wireBuild,
            runtimeIdentity: identity
        )
        self.identity = identity
        self.controller = controller
        let productHandler = handler.operation
        server = SocketServer(
            path: path,
            wireBuild: wireBuild,
            configuration: configuration,
            runtimeLifecycle: controller,
            controlOperations: [runtimeReadinessSubscribeOperation, runtimeReceiptOperation],
            sessionPolicy: sessionPolicy,
            handler: { request in
                if let response = controller.handleControl(request) {
                    return response
                }
                return await productHandler(request)
            }
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
                await settleFailedStart()
            } else {
                controller.finishShutdown()
                lock.withLock { shutDown = true }
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

    /// Retains and joins complete cleanup after any partially-started generation.
    func settleFailedStart() async {
        let task = lock.withLock { () -> Task<Void, Never> in
            if let failedStartCleanup {
                return failedStartCleanup
            }
            shutDown = true
            let task = Task { [controller, server] in
                try? await controller.failStarting()
                await server.stop()
                controller.finishShutdown()
            }
            failedStartCleanup = task
            return task
        }
        await task.value
    }

    /// Stops serving by the deadline and clears publication only after complete settlement.
    func shutdown(deadline: Date) async throws {
        if let failedStartCleanup = lock.withLock({ failedStartCleanup }) {
            await failedStartCleanup.value
            return
        }
        let shouldStop = lock.withLock {
            guard !shutDown else { return false }
            shutDown = true
            return true
        }
        guard shouldStop else { return }
        do {
            try await controller.closeIntake(deadline: deadline)
            try await server.stopRuntime(deadline: deadline)
            controller.finishShutdown()
        } catch {
            lock.withLock { shutDown = false }
            throw error
        }
    }
}
