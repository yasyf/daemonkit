import Darwin
import Foundation

/// Resolves the invoking user's real home directory.
///
/// Inside an app extension (appex), `NSHomeDirectory()` and
/// `FileManager.homeDirectoryForCurrentUser` return the sandbox **container**
/// path, not the user's home. Reading the passwd database via
/// `getpwuid(getuid())` yields the true home regardless of container
/// redirection — that is the reason this exists.
public enum RealHome {
    /// The invoking user's real home directory. Falls back to
    /// `FileManager.homeDirectoryForCurrentUser` only when the passwd lookup
    /// yields no usable path.
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
