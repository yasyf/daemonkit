import Foundation

public func acquireReadyRuntime(
    configuration: RuntimeClientConfiguration,
    expectedRuntimeBuild: String,
    deadline: Date
) async throws -> RuntimeProcessReceipt {
    let client = try ServiceSocketClient(
        path: configuration.path,
        wireBuild: configuration.wireBuild,
        role: configuration.role,
        noProgressTimeout: configuration.noProgressTimeout,
        configuration: configuration.socket,
        onProgress: configuration.onProgress
    )
    do {
        let receipt = try await client.acquireReadyRuntime(
            expectedRuntimeBuild: expectedRuntimeBuild,
            deadline: deadline
        )
        await client.close()
        return receipt
    } catch {
        await client.close()
        throw error
    }
}
