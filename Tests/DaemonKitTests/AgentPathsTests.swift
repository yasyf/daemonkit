@testable import DaemonKit
import Foundation
import Testing

@Suite(.timeLimit(.minutes(1)))
struct AgentPathsTests {
    @Test func agentPathsRootStateUnderTheHiddenDir() throws {
        let home = RealHome.directory()
        let label = "com.example.daemon"
        let root = home
            .appendingPathComponent(".daemonkit", isDirectory: true)
            .appendingPathComponent("a", isDirectory: true)
            .appendingPathComponent(label, isDirectory: true)

        #expect(AgentPaths.root.path == home.appendingPathComponent(".daemonkit").path)
        #expect(AgentPaths.directory(label: label).path == root.path)
        #expect(try AgentPaths.socket(label: label).path == root.appendingPathComponent("daemon.sock").path)
    }

    /// The Swift derivation must agree with Go `paths.Agent` byte for byte: a
    /// consumer dialing a socket the Go daemon bound has no second chance to
    /// notice they disagree.
    @Test func agentSocketMatchesTheGoLayout() throws {
        let socket = try AgentPaths.socket(label: "com.example.daemon")
        #expect(socket.path.hasSuffix("/.daemonkit/a/com.example.daemon/daemon.sock"))
    }

    @Test func agentSocketRefusesAPathOverSunPath() {
        let label = String(repeating: "a", count: 200)
        #expect(throws: AgentPathError.self) {
            try AgentPaths.socket(label: label)
        }
    }
}
