@testable import DaemonKit
import Foundation
import Testing

struct TestSupportTests {
    @Test func shortSocketDirectoriesAreAtomicallyUniqueUnderConcurrency() async throws {
        let count = 256
        let directories = try await withThrowingTaskGroup(of: URL.self) { group in
            for _ in 0 ..< count {
                group.addTask { try shortSocketDir() }
            }
            var directories: [URL] = []
            for try await directory in group {
                directories.append(directory)
            }
            return directories
        }
        defer {
            for directory in directories {
                try? FileManager.default.removeItem(at: directory)
            }
        }

        #expect(Set(directories.map(\.path)).count == count)
        #expect(directories.allSatisfy { FileManager.default.fileExists(atPath: $0.path) })
    }
}
