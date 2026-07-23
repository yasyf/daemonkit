import Darwin
import Foundation

/// Errors raised by a generation-aware service socket client.
public enum ServiceSocketClientError: Error, Equatable, Sendable {
    case closed
    case deadlineExceeded
    case peerWireBuild(got: String, want: String)
}

/// A persistent unary client that crosses expected service startup and takeover.
public actor ServiceSocketClient {
    private struct Generation: Sendable {
        let id: UInt64
        let client: Task<SocketClient, Error>
    }

    private let path: String
    private let wireBuild: String
    private let configuration: SocketClient.Configuration
    private let trust: PeerTrust
    private var generation: Generation?
    private var nextGeneration: UInt64 = 1
    private var closed = false

    var startedGenerations: UInt64 {
        nextGeneration - 1
    }

    /// Creates a lazy exact-build service client.
    public init(
        path: String,
        wireBuild: String,
        configuration: SocketClient.Configuration = .init(),
        trust: PeerTrust
    ) {
        self.path = path
        self.wireBuild = wireBuild
        self.configuration = configuration
        self.trust = trust
    }

    /// Sends one unary call, waiting through typed non-dispatch lifecycle states.
    public func call(
        operation: String,
        tenant: String = "",
        payload: Data = Data(),
        deadline: Date? = nil
    ) async throws -> SocketTerminal {
        while true {
            try checkBound(deadline)
            let current: (Generation, SocketClient)
            do {
                current = try await session(deadline: deadline)
            } catch {
                guard Self.provesNoListener(error) else { throw error }
                try await waitForRetry(deadline: deadline)
                continue
            }

            let terminal: SocketTerminal
            do {
                terminal = try await current.1.call(
                    operation: operation,
                    tenant: tenant,
                    payload: payload,
                    deadline: deadline
                )
            } catch {
                await retire(current.0)
                throw error
            }
            guard terminal.rejected else { return terminal }
            switch terminal.code {
            case .runtimeStarting:
                try await waitForRetry(deadline: deadline)
            case .serverDraining:
                await retire(current.0)
                try await waitForRetry(deadline: deadline)
            default:
                return terminal
            }
        }
    }

    /// Closes the service lifetime and its current session generation.
    public func close() async {
        guard !closed else { return }
        closed = true
        guard let current = generation else { return }
        generation = nil
        if let client = try? await current.client.value {
            await client.close()
        }
    }

    private func session(deadline: Date?) async throws -> (Generation, SocketClient) {
        guard !closed else { throw ServiceSocketClientError.closed }
        let current: Generation
        if let generation {
            current = generation
        } else {
            var attemptConfiguration = configuration
            if let deadline {
                let remaining = deadline.timeIntervalSinceNow
                guard remaining > 0 else { throw ServiceSocketClientError.deadlineExceeded }
                attemptConfiguration.handshakeTimeout = min(attemptConfiguration.handshakeTimeout, remaining)
            }
            let id = nextGeneration
            nextGeneration += 1
            let path = path
            let wireBuild = wireBuild
            let trust = trust
            let task = Task {
                try await SocketClient(
                    path: path,
                    wireBuild: wireBuild,
                    configuration: attemptConfiguration,
                    trust: trust
                )
            }
            current = Generation(id: id, client: task)
            generation = current
        }

        let client: SocketClient
        do {
            client = try await current.client.value
        } catch {
            if generation?.id == current.id {
                generation = nil
            }
            throw error
        }
        guard !closed else {
            await client.close()
            throw ServiceSocketClientError.closed
        }
        guard client.peerWireBuild == wireBuild else {
            if generation?.id == current.id {
                generation = nil
            }
            await client.close()
            throw ServiceSocketClientError.peerWireBuild(got: client.peerWireBuild, want: wireBuild)
        }
        return (current, client)
    }

    private func retire(_ current: Generation) async {
        guard generation?.id == current.id else { return }
        generation = nil
        if let client = try? await current.client.value {
            await client.close()
        }
    }

    private func waitForRetry(deadline: Date?) async throws {
        try checkBound(deadline)
        var nanoseconds: UInt64 = 25_000_000
        if let deadline {
            let remaining = deadline.timeIntervalSinceNow
            guard remaining > 0 else { throw ServiceSocketClientError.deadlineExceeded }
            nanoseconds = min(nanoseconds, UInt64(remaining * 1_000_000_000))
        }
        try await Task.sleep(nanoseconds: nanoseconds)
        try checkBound(deadline)
    }

    private func checkBound(_ deadline: Date?) throws {
        try Task.checkCancellation()
        if let deadline, deadline <= Date() {
            throw ServiceSocketClientError.deadlineExceeded
        }
        if closed {
            throw ServiceSocketClientError.closed
        }
    }

    private static func provesNoListener(_ error: Error) -> Bool {
        guard case let SessionTransportError.systemCall(operation, code) = error else { return false }
        return operation == "connect" && (code == ENOENT || code == ECONNREFUSED)
    }
}
