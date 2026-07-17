/// DaemonKit is the Swift half of daemonkit: socket serving, codesign peer
/// trust, launchd registration, and snapshot watching for signed helper apps.
public enum DaemonKit {
    /// Version of the daemonkit-native lifecycle envelope (`"v"` in every
    /// frame). Golden-pinned against the Go side's `wire/lifeproto`.
    public static let lifeProtocolVersion = 1

    /// `os.Logger` subsystem shared by every DaemonKit diagnostic category.
    public static let loggingSubsystem = "com.yasyf.daemonkit"
}
