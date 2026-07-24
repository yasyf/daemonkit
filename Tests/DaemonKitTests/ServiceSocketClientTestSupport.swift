@testable import DaemonKit
import Foundation

func serviceTestClient(
    path: String,
    wireBuild: String,
    role: String,
    noProgressTimeout: TimeInterval,
    configuration: SocketClient.Configuration = .init(),
    onProgress: (@Sendable (ReadinessProgress) -> Void)? = nil
) throws -> ServiceSocketClient {
    try ServiceSocketClient(
        path: path,
        wireBuild: wireBuild,
        role: role,
        readinessOperation: readinessSubscribeOperation,
        noProgressTimeout: noProgressTimeout,
        configuration: configuration,
        onProgress: onProgress
    )
}

func serviceTestServer(
    path: String,
    wireBuild: String,
    configuration: SocketServer.Configuration = .init(),
    runtimeLifecycle: RuntimeLifecycleController? = nil,
    handler: @escaping @Sendable (SocketRequest) async -> SocketResponse
) -> SocketServer {
    guard let runtimeLifecycle else {
        return SocketServer(path: path, wireBuild: wireBuild, configuration: configuration) { request in
            guard request.operation == readinessSubscribeOperation else {
                return await handler(request)
            }
            return .terminal(SocketTerminal(error: "wire: runtime lifecycle is not configured"))
        }
    }
    return SocketServer(
        path: path,
        wireBuild: wireBuild,
        configuration: configuration,
        runtimeLifecycle: runtimeLifecycle,
        controlOperations: [readinessSubscribeOperation]
    ) { request in
        guard request.operation == readinessSubscribeOperation else {
            return await handler(request)
        }
        do {
            try RuntimeReadinessCodec.decodeSubscribeAck(request.payload)
            guard let session = request.session.implementation else {
                throw SessionTransportError.disconnected
            }
            guard runtimeLifecycle.register(session) else {
                return .terminal(SocketTerminal(
                    rejected: true,
                    code: .readinessSubscriptionExists,
                    reason: "wire: readiness subscription already registered"
                ))
            }
            Task {
                await request.session.waitUntilClosed()
                runtimeLifecycle.unregister(session)
            }
            return try .terminal(SocketTerminal(
                payload: RuntimeReadinessCodec.encodeSubscribe()
            ) {
                runtimeLifecycle.activate(session)
            })
        } catch {
            return .terminal(SocketTerminal(error: String(describing: error)))
        }
    }
}

func genericServiceCall(
    operation: String,
    tenant: String = "",
    payload: Data = Data(),
    replay: ServiceSocketReplayPolicy = .provenNonDispatch,
    deadline: Date
) -> ServiceSocketCall {
    ServiceSocketCall(
        operation: operation,
        tenant: tenant,
        payload: payload,
        replay: replay,
        runtimeTarget: .anyAuthenticatedSuccessor,
        deadline: deadline
    )
}
