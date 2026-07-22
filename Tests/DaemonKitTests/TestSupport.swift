import Darwin
import Foundation

final class AsyncCleanup: @unchecked Sendable {
    private let lock = NSLock()
    private var operations: [@Sendable () async -> Void] = []

    func add(_ operation: @escaping @Sendable () async -> Void) {
        lock.withLock { operations.append(operation) }
    }

    func settle() async {
        let operations = lock.withLock {
            let operations = self.operations.reversed()
            self.operations.removeAll()
            return Array(operations)
        }
        for operation in operations {
            await operation()
        }
    }
}

func withAsyncCleanup<Result>(
    _ operation: (AsyncCleanup) async throws -> Result
) async rethrows -> Result {
    let cleanup = AsyncCleanup()
    do {
        let result = try await operation(cleanup)
        await cleanup.settle()
        return result
    } catch {
        await cleanup.settle()
        throw error
    }
}

func shortSocketDir() throws -> URL {
    let directory = URL(fileURLWithPath: "/tmp/dk-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory
}

func makeAddress(path: String) -> sockaddr_un? {
    var address = sockaddr_un()
    address.sun_family = sa_family_t(AF_UNIX)
    let bytes = Array(path.utf8)
    guard bytes.count < MemoryLayout.size(ofValue: address.sun_path) else { return nil }
    withUnsafeMutableBytes(of: &address.sun_path) { destination in
        bytes.withUnsafeBytes { destination.copyMemory(from: $0) }
    }
    return address
}
