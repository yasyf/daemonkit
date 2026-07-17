// swift-tools-version: 6.2
import PackageDescription

/// DaemonKit is the Swift half of daemonkit — a library only, statically
/// linked by consumers (no executable target, so no appex signing surface).
let package = Package(
    name: "daemonkit",
    platforms: [.macOS(.v15)],
    products: [
        .library(name: "DaemonKit", targets: ["DaemonKit"]),
    ],
    targets: [
        .target(name: "DaemonKit"),
        .testTarget(name: "DaemonKitTests", dependencies: ["DaemonKit"]),
    ]
)
