import Foundation

private let sunPathBytes = 104

/// AgentPaths is one daemon's private state layout, at `~/.daemonkit/a/<label>`.
/// It is the Swift half of the Go `paths.Agent` derivation, pinned to it by the
/// shared `paths/testdata/agent-layout.json` golden.
public struct AgentPaths: Sendable {
    public let label: String
    let home: URL

    public init(label: String) {
        self.init(home: RealHome.directory(), label: label)
    }

    init(home: URL, label: String) {
        self.home = home
        self.label = label
    }

    public var stateDirectory: URL {
        home.appendingPathComponent(".daemonkit", isDirectory: true)
            .appendingPathComponent("a", isDirectory: true)
            .appendingPathComponent(label, isDirectory: true)
    }

    /// The daemon's control-plane socket, refused when it cannot fit darwin's
    /// `sun_path` with its terminating NUL rather than surviving to a truncated
    /// bind.
    public func socket() throws -> URL {
        let path = stateDirectory.appendingPathComponent("daemon.sock")
        guard path.path.utf8.count < sunPathBytes else {
            throw SocketPathError(path: path.path)
        }
        return path
    }
}

/// SocketPathError reports a socket path too long for darwin's 104-byte
/// `sun_path`.
public struct SocketPathError: Error, CustomStringConvertible, Equatable, Sendable {
    public let path: String

    public var description: String {
        "socket path is \(path.utf8.count) bytes; sun_path fits \(sunPathBytes - 1): \(path)"
    }
}
