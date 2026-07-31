import Foundation

/// Connects the configured socket and returns once a ready session is
/// established, mirroring internal/wire.Client.WaitReady over reconnects.
public func acquireReadyRuntime(
    configuration: RuntimeClientConfiguration,
    deadline: Date
) async throws {
    let client = try ServiceSocketClient(
        path: configuration.path,
        schema: configuration.schema,
        lane: configuration.lane,
        configuration: configuration.socket,
        onProgress: configuration.onProgress
    )
    do {
        try await client.waitReady(deadline: deadline)
        await client.close()
    } catch {
        await client.close()
        throw error
    }
}
