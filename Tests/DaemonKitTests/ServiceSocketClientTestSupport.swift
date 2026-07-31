@testable import DaemonKit
import Darwin
import Foundation

enum GoWireServerError: Error, CustomStringConvertible {
    case binaryMissing
    case startupFailed
    case startupTimeout

    var description: String {
        switch self {
        case .binaryMissing:
            "DAEMONKIT_WIRE_TEST_SERVER is unset; run scripts/swift-test.sh"
        case .startupFailed:
            "wire test server exited before printing READY"
        case .startupTimeout:
            "wire test server did not print READY in time"
        }
    }
}

/// Launches the prebuilt Go wire test server (internal/wire/testserver) over a
/// fresh short unix socket, exposing its path once it prints READY. Teardown
/// closes stdin so the server exits even if the runner dies.
final class GoWireServer: @unchecked Sendable {
    static let schema = "suite.v1"

    let socketPath: String
    private let process: Process
    private let stdin: Pipe
    private let directory: URL

    static var binaryPath: String {
        get throws {
            let environment = ProcessInfo.processInfo.environment
            guard let path = environment["DAEMONKIT_WIRE_TEST_SERVER"], !path.isEmpty else {
                throw GoWireServerError.binaryMissing
            }
            return path
        }
    }

    init(
        phases: String = "ready",
        schema: String = GoWireServer.schema,
        accept: [String] = [],
        controlTeam: String? = nil
    ) throws {
        directory = try shortSocketDir()
        socketPath = directory.appendingPathComponent("s").path
        let process = Process()
        process.executableURL = try URL(fileURLWithPath: Self.binaryPath)
        var arguments = ["-socket", socketPath, "-schema", schema, "-phases", phases]
        for accepted in accept {
            arguments += ["-accept", accepted]
        }
        if let controlTeam {
            arguments += ["-control-team", controlTeam]
        }
        process.arguments = arguments
        let output = Pipe()
        process.standardOutput = output
        process.standardError = FileHandle.standardError
        let input = Pipe()
        process.standardInput = input
        stdin = input
        self.process = process
        try process.run()
        try Self.waitForReady(output.fileHandleForReading)
    }

    func shutdown() {
        try? stdin.fileHandleForWriting.close()
        process.waitUntilExit()
        try? FileManager.default.removeItem(at: directory)
    }

    private static func waitForReady(_ handle: FileHandle) throws {
        var buffer = Data()
        while true {
            let chunk = handle.availableData
            if chunk.isEmpty {
                throw GoWireServerError.startupFailed
            }
            buffer.append(chunk)
            if let text = String(data: buffer, encoding: .utf8), text.contains("READY ") {
                return
            }
            if buffer.count > 4096 {
                throw GoWireServerError.startupTimeout
            }
        }
    }
}

func businessServiceClient(
    _ server: GoWireServer,
    onProgress: (@Sendable (PhaseSnapshot) -> Void)? = nil
) throws -> ServiceSocketClient {
    try ServiceSocketClient(
        path: server.socketPath,
        schema: GoWireServer.schema,
        lane: .business,
        onProgress: onProgress
    )
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
        deadline: deadline
    )
}
