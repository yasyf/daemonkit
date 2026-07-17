@testable import DaemonKit
import Darwin
import Foundation
import Testing

/// A short scratch directory under `/tmp` — long `NSTemporaryDirectory()` paths
/// blow the `sockaddr_un.sun_path` limit.
private func shortSocketDir() throws -> URL {
    let dir = URL(fileURLWithPath: "/tmp/dk-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
    try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
    return dir
}

private func makeAddress(path: String) -> sockaddr_un? {
    var addr = sockaddr_un()
    addr.sun_family = sa_family_t(AF_UNIX)
    let bytes = Array(path.utf8)
    let capacity = MemoryLayout.size(ofValue: addr.sun_path)
    guard bytes.count < capacity else { return nil }
    withUnsafeMutableBytes(of: &addr.sun_path) { dst in
        bytes.withUnsafeBytes { dst.copyMemory(from: $0) }
    }
    return addr
}

private func withAddress<R>(_ addr: inout sockaddr_un, _ body: (UnsafePointer<sockaddr>, socklen_t) -> R) -> R {
    withUnsafePointer(to: &addr) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { rebound in
            body(rebound, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
}

/// Connects, writes `payload + "\n"`, reads one reply line (newline stripped).
/// Returns `nil` when the connection cannot be made.
private func sendLine(to path: String, _ payload: Data, timeout: TimeInterval = 3) -> Data? {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else { return nil }
    defer { close(descriptor) }
    guard var addr = makeAddress(path: path) else { return nil }
    guard withAddress(&addr, { connect(descriptor, $0, $1) }) == 0 else { return nil }

    var timeoutValue = timeval(tv_sec: Int(timeout), tv_usec: 0)
    precondition(
        setsockopt(
            descriptor,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeoutValue,
            socklen_t(MemoryLayout<timeval>.size)
        ) == 0,
        "setsockopt(SO_RCVTIMEO) failed: errno \(errno)"
    )
    var noSignal: Int32 = 1
    setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size))

    var line = payload
    line.append(0x0A)
    _ = line.withUnsafeBytes { write(descriptor, $0.baseAddress, $0.count) }

    var reply = Data()
    var buffer = [UInt8](repeating: 0, count: 4096)
    while true {
        let bytesRead = buffer.withUnsafeMutableBytes { read(descriptor, $0.baseAddress, $0.count) }
        if bytesRead <= 0 {
            break
        }
        if let newline = buffer[0 ..< bytesRead].firstIndex(of: 0x0A) {
            reply.append(contentsOf: buffer[0 ..< newline])
            break
        }
        reply.append(contentsOf: buffer[0 ..< bytesRead])
    }
    return reply
}

/// Binds a raw socket at `path` and closes it *without* unlinking — leaves a
/// dead socket file behind, exactly what a crashed daemon leaves.
private func leaveStaleSocket(at path: String) {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0, var addr = makeAddress(path: path) else { return }
    _ = withAddress(&addr) { Darwin.bind(descriptor, $0, $1) }
    close(descriptor)
}

@Suite(.serialized, .timeLimit(.minutes(1)))
struct SocketServerTests {
    @Test func roundTripsOneLine() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path) { request in
            var upper = Data("echo:".utf8)
            upper.append(request)
            return upper
        }
        try server.start()
        defer { server.stop() }

        let reply = sendLine(to: path, Data("hello".utf8))
        #expect(reply == Data("echo:hello".utf8))
    }

    @Test func chmodsSocketTo0600BeforeAccepting() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path) { $0 }
        try server.start()
        defer { server.stop() }

        var status = stat()
        #expect(stat(path, &status) == 0)
        #expect((status.st_mode & 0o777) == 0o600)
    }

    @Test func refusesToBindOverALivePeer() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let live = SocketServer(path: path) { $0 }
        try live.start()
        defer { live.stop() }

        let intruder = SocketServer(path: path) { $0 }
        do {
            try intruder.start()
            Issue.record("expected addressInUse")
        } catch let error as SocketServerError {
            guard case .addressInUse = error else {
                Issue.record("expected .addressInUse, got \(error)")
                return
            }
        }
    }

    @Test func reclaimsADeadSocket() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        leaveStaleSocket(at: path)
        #expect(FileManager.default.fileExists(atPath: path))

        let server = SocketServer(path: path) { request in
            var out = Data("re:".utf8)
            out.append(request)
            return out
        }
        try server.start()
        defer { server.stop() }

        #expect(sendLine(to: path, Data("ping".utf8)) == Data("re:ping".utf8))
    }

    @Test func dropsLinesOverTheByteCap() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path, configuration: .init(maxLineBytes: 16)) { $0 }
        try server.start()
        defer { server.stop() }

        let oversized = Data(String(repeating: "x", count: 128).utf8)
        let reply = sendLine(to: path, oversized)
        #expect(reply?.isEmpty == true)

        let small = Data("small".utf8)
        #expect(sendLine(to: path, small) == small)
    }

    @Test func cleanShutdownUnlinksAndStopsAccepting() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path) { $0 }
        try server.start()
        #expect(sendLine(to: path, Data("up".utf8)) == Data("up".utf8))

        server.stop()
        #expect(!FileManager.default.fileExists(atPath: path))
        #expect(sendLine(to: path, Data("down".utf8)) == nil)
    }

    @Test func rejectsAnOverlongPath() {
        let path = "/tmp/" + String(repeating: "a", count: 200) + ".sock"
        let server = SocketServer(path: path) { $0 }
        do {
            try server.start()
            Issue.record("expected pathTooLong")
        } catch let error as SocketServerError {
            guard case .pathTooLong = error else {
                Issue.record("expected .pathTooLong, got \(error)")
                return
            }
        } catch {
            Issue.record("expected SocketServerError, got \(error)")
        }
    }
}
