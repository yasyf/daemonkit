import Darwin
import Foundation

/// Resolves the invoking user's real home via `getpwuid(getuid())`: inside an
/// app extension, `NSHomeDirectory()` returns the sandbox container path.
public enum RealHome {
    /// The invoking user's real home directory.
    public static func directory() -> URL {
        if let entry = getpwuid(getuid()), let dir = entry.pointee.pw_dir {
            let path = String(cString: dir)
            if !path.isEmpty {
                return URL(fileURLWithPath: path, isDirectory: true)
            }
        }
        return FileManager.default.homeDirectoryForCurrentUser
    }
}
