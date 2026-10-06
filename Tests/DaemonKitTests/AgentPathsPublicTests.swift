import DaemonKit
import Foundation
import Testing

struct AgentPathsPublicTests {
    @Test func anotherUsersHomeRootsTheLayoutThroughThePublicInitializer() throws {
        let home = URL(fileURLWithPath: "/Users/console", isDirectory: true)
        let paths = AgentPaths(home: home, label: "com.example.daemon")
        #expect(paths.stateDirectory.path == "/Users/console/.daemonkit/a/com.example.daemon")
        #expect(try paths.socket().path == "/Users/console/.daemonkit/a/com.example.daemon/daemon.sock")
    }
}
