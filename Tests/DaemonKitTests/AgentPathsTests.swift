@testable import DaemonKit
import Foundation
import Testing

struct AgentPathsTests {
    private static func golden() throws -> [String: String] {
        let root = URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
        let object = try JSONSerialization.jsonObject(
            with: Data(contentsOf: root.appendingPathComponent("paths/testdata/agent-layout.json"))
        ) as? [String: String]
        return try #require(object)
    }

    @Test func layoutMatchesTheSharedGoGolden() throws {
        let golden = try Self.golden()
        let home = try URL(fileURLWithPath: #require(golden["home"]), isDirectory: true)
        let paths = try AgentPaths(home: home, label: #require(golden["label"]))
        #expect(paths.stateDirectory.path == golden["stateDir"])
        #expect(try paths.socket().path == golden["socket"])
    }

    @Test func realHomeRootsTheDefaultLayout() {
        let paths = AgentPaths(label: "com.example.daemon")
        #expect(paths.stateDirectory == AgentPaths(home: RealHome.directory(), label: "com.example.daemon").stateDirectory)
    }

    @Test(arguments: [(73, false), (74, true)])
    func socketRefusesALabelPastSunPath(labelLength: Int, refused: Bool) throws {
        let paths = AgentPaths(home: URL(fileURLWithPath: "/tmp", isDirectory: true),
                               label: String(repeating: "a", count: labelLength))
        if refused {
            #expect(throws: SocketPathError.self) { try paths.socket() }
        } else {
            #expect(try paths.socket().path.utf8.count == 103)
        }
    }
}
