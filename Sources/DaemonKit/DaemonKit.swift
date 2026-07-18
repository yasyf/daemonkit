/// DaemonKit is the Swift half of daemonkit: socket serving, codesign peer
/// trust, launchd registration, and snapshot watching for signed helper apps.
public enum DaemonKit {
    /// `os.Logger` subsystem shared by every DaemonKit diagnostic category.
    public static let loggingSubsystem = "com.yasyf.daemonkit"
}
