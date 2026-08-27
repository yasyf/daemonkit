import Foundation

/// AgentPaths resolves one daemon's private state paths from its launchd label.
/// It is the Swift half of Go `paths.Agent`: a mixed-language consumer reads the
/// layout here instead of transcribing it.
public enum AgentPaths {
    /// The daemonkit-owned root (`~/.daemonkit`).
    public static var root: URL {
        RealHome.directory().appendingPathComponent(daemonkitDir, isDirectory: true)
    }

    /// One daemon's private state directory (`~/.daemonkit/a/<label>`).
    public static func directory(label: String) -> URL {
        root
            .appendingPathComponent(agentsDir, isDirectory: true)
            .appendingPathComponent(label, isDirectory: true)
    }

    /// The daemon's control-plane socket (`~/.daemonkit/a/<label>/daemon.sock`).
    /// A path that cannot fit darwin's `sun_path` with its terminating NUL
    /// throws rather than surviving to a truncated connect.
    public static func socket(label: String) throws -> URL {
        let url = directory(label: label).appendingPathComponent(socketLeaf)
        let bytes = url.path.utf8.count
        guard bytes < sunPathBytes else {
            throw AgentPathError.socketPathTooLong(path: url.path, bytes: bytes)
        }
        return url
    }

    private static let daemonkitDir = ".daemonkit"
    private static let agentsDir = "a"
    private static let socketLeaf = "daemon.sock"
    private static let sunPathBytes = 104
}

/// AgentPathError reports a derived path darwin cannot carry.
public enum AgentPathError: Error, Equatable, Sendable {
    /// The socket path is `bytes` long; `sun_path` fits `sunPathBytes - 1`.
    case socketPathTooLong(path: String, bytes: Int)
}
