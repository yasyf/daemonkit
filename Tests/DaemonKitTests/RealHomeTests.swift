@testable import DaemonKit
import Darwin
import Foundation
import Testing

@Suite(.timeLimit(.minutes(1)))
struct RealHomeTests {
    @Test func realHomeMatchesPasswdEntry() throws {
        let entry = try #require(getpwuid(getuid()))
        let dir = try #require(entry.pointee.pw_dir)
        let expected = String(cString: dir)
        #expect(!expected.isEmpty)
        #expect(RealHome.directory().path == expected)
    }

    @Test func realHomeIsAnExistingDirectory() {
        let home = RealHome.directory()
        var isDirectory: ObjCBool = false
        #expect(FileManager.default.fileExists(atPath: home.path, isDirectory: &isDirectory))
        #expect(isDirectory.boolValue)
    }
}
