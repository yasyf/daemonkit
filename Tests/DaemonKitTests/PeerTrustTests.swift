@testable import DaemonKit
import Darwin
import Foundation
import Security
import Testing

/// A short scratch directory under `/tmp` — long `NSTemporaryDirectory()` paths
/// blow the `sockaddr_un.sun_path` limit.
private func shortSocketDir() throws -> URL {
    let dir = URL(fileURLWithPath: "/tmp/dk-tr-\(getpid())-\(UInt32.random(in: 0 ..< 0xFFFF))")
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

/// Establishes a connected AF_UNIX stream pair via listen/connect/accept and
/// returns the server-accepted fd plus the client fd. Both endpoints belong to
/// this test process, so the peer's euid is this process's euid and its audit
/// token resolves to the test binary — exactly the shape a same-user peer takes.
private func acceptedConnection(at path: String) -> (server: Int32, client: Int32) {
    guard var addr = makeAddress(path: path) else { preconditionFailure("socket path too long: \(path)") }
    let listener = socket(AF_UNIX, SOCK_STREAM, 0)
    precondition(listener >= 0, "socket(listener): errno \(errno)")
    precondition(withAddress(&addr) { Darwin.bind(listener, $0, $1) } == 0, "bind: errno \(errno)")
    precondition(listen(listener, 1) == 0, "listen: errno \(errno)")

    let client = socket(AF_UNIX, SOCK_STREAM, 0)
    precondition(client >= 0, "socket(client): errno \(errno)")
    precondition(withAddress(&addr) { connect(client, $0, $1) } == 0, "connect: errno \(errno)")

    let server = accept(listener, nil, nil)
    precondition(server >= 0, "accept: errno \(errno)")
    close(listener)
    return (server, client)
}

/// The current process's own designated requirement string, resolved through the
/// SecCode APIs. For an ad-hoc-signed test binary this is a `cdhash` clause; for a
/// Developer-ID build it is the identity requirement. Either way it is a
/// requirement the test process satisfies, so it exercises the accept path.
private func selfDesignatedRequirement() throws -> String {
    var code: SecCode?
    try #require(SecCodeCopySelf([], &code) == errSecSuccess)
    let selfCode = try #require(code)
    var staticCode: SecStaticCode?
    try #require(SecCodeCopyStaticCode(selfCode, [], &staticCode) == errSecSuccess)
    let selfStatic = try #require(staticCode)
    var requirement: SecRequirement?
    try #require(SecCodeCopyDesignatedRequirement(selfStatic, [], &requirement) == errSecSuccess)
    let designated = try #require(requirement)
    var string: CFString?
    try #require(SecRequirementCopyString(designated, [], &string) == errSecSuccess)
    return try #require(string) as String
}

/// A Developer-ID requirement the ad-hoc/self-built test binary cannot satisfy —
/// used to drive the fail-closed rejection path.
private let unsatisfiableRequirement =
    "identifier \"com.yasyf.daemonkit.absent\" and anchor apple generic " +
    "and certificate leaf[subject.OU] = \"SXKCTF23Q2\""

/// Connects, writes `payload + "\n"`, reads one reply line (newline stripped).
/// Returns `nil` when the connection cannot be made.
private func sendLine(to path: String, _ payload: Data, timeout: TimeInterval = 3) -> Data? {
    let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
    guard descriptor >= 0 else { return nil }
    defer { close(descriptor) }
    guard var addr = makeAddress(path: path) else { return nil }
    guard withAddress(&addr, { connect(descriptor, $0, $1) }) == 0 else { return nil }

    var timeoutValue = timeval(tv_sec: Int(timeout), tv_usec: 0)
    setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeoutValue, socklen_t(MemoryLayout<timeval>.size))
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

private func entitlementsXML(_ entitlements: [String]) -> String {
    let entries = entitlements.map { "<key>\($0)</key><true/>" }.joined()
    return """
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0"><dict>\(entries)</dict></plist>
    """
}

/// Re-signs `binary` ad hoc with the given codesign options and entitlements.
private func codesign(_ binary: URL, options: String?, entitlements: [String]?, scratch: URL) throws {
    var arguments = ["-f", "-s", "-"]
    if let options {
        arguments += ["--options", options]
    }
    if let entitlements {
        let plist = scratch.appendingPathComponent("entitlements.plist")
        try entitlementsXML(entitlements).write(to: plist, atomically: true, encoding: .utf8)
        arguments += ["--entitlements", plist.path]
    }
    arguments.append(binary.path)
    let sign = Process()
    sign.executableURL = URL(fileURLWithPath: "/usr/bin/codesign")
    sign.arguments = arguments
    sign.standardError = FileHandle.nullDevice
    try sign.run()
    sign.waitUntilExit()
    try #require(sign.terminationStatus == 0, "codesign \(arguments.joined(separator: " ")) failed")
}

/// A live re-signed `nc` peer: the accepted server fd, the copy's own
/// designated requirement, and the process to reap.
private struct SignedPeer {
    let server: Int32
    let requirement: String
    let process: Process

    func close() {
        Darwin.close(server)
        process.terminate()
        process.waitUntilExit()
    }
}

/// Spawns a copy of `nc` re-signed ad hoc with the given options/entitlements
/// as a live unix-socket peer. Ad-hoc signing needs no identity, so these
/// peers build anywhere; Developer-ID anchors are the Go trust E2E's domain.
private func spawnSignedPeer(
    in dir: URL, options: String?, entitlements: [String]? = nil
) throws -> SignedPeer {
    let binary = dir.appendingPathComponent("peer")
    try FileManager.default.copyItem(at: URL(fileURLWithPath: "/usr/bin/nc"), to: binary)
    try codesign(binary, options: options, entitlements: entitlements, scratch: dir)

    var staticCodeRef: SecStaticCode?
    try #require(SecStaticCodeCreateWithPath(binary as CFURL, [], &staticCodeRef) == errSecSuccess)
    let staticCode = try #require(staticCodeRef)
    var designatedRef: SecRequirement?
    try #require(SecCodeCopyDesignatedRequirement(staticCode, [], &designatedRef) == errSecSuccess)
    let designated = try #require(designatedRef)
    var requirementRef: CFString?
    try #require(SecRequirementCopyString(designated, [], &requirementRef) == errSecSuccess)
    let requirement = try #require(requirementRef) as String

    let path = dir.appendingPathComponent("s.sock").path
    var addr = try #require(makeAddress(path: path))
    let listener = socket(AF_UNIX, SOCK_STREAM, 0)
    try #require(listener >= 0)
    defer { close(listener) }
    try #require(withAddress(&addr) { Darwin.bind(listener, $0, $1) } == 0)
    try #require(listen(listener, 1) == 0)

    let peer = Process()
    peer.executableURL = binary
    peer.arguments = ["-U", path]
    peer.standardInput = Pipe()
    peer.standardOutput = FileHandle.nullDevice
    peer.standardError = FileHandle.nullDevice
    try peer.run()

    var poller = pollfd(fd: listener, events: Int16(POLLIN), revents: 0)
    try #require(poll(&poller, 1, 5000) == 1, "signed peer never connected")
    let server = accept(listener, nil, nil)
    try #require(server >= 0)
    return SignedPeer(server: server, requirement: requirement, process: peer)
}

/// The peer's dynamic code-signing status, read through the same system APIs
/// the gate uses — ground truth for asserting live status bits.
private func peerDynamicStatus(descriptor: Int32) throws -> UInt32 {
    var token = audit_token_t()
    var length = socklen_t(MemoryLayout<audit_token_t>.size)
    try #require(getsockopt(descriptor, SOL_LOCAL, LOCAL_PEERTOKEN, &token, &length) == 0)
    let tokenData = withUnsafeBytes(of: &token) { Data($0) }
    var codeRef: SecCode?
    let attributes = [kSecGuestAttributeAudit: tokenData] as CFDictionary
    try #require(SecCodeCopyGuestWithAttributes(nil, attributes, [], &codeRef) == errSecSuccess)
    let code = try #require(codeRef)
    var infoRef: CFDictionary?
    let staticCode = unsafeBitCast(code, to: SecStaticCode.self)
    let flags = SecCSFlags(rawValue: kSecCSDynamicInformation)
    try #require(SecCodeCopySigningInformation(staticCode, flags, &infoRef) == errSecSuccess)
    let info = try #require(infoRef) as NSDictionary
    return try #require((info[kSecCodeInfoStatus] as? NSNumber)?.uint32Value)
}

@Suite(.serialized, .timeLimit(.minutes(1)))
struct PeerTrustTests {
    @Test func selfConnectionPassesTheEUIDFloor() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let (server, client) = acceptedConnection(at: dir.appendingPathComponent("s.sock").path)
        defer { close(server); close(client) }

        try PeerTrust().check(descriptor: server)
    }

    @Test func matchingRequirementPassesWithAllowUnhardened() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let (server, client) = acceptedConnection(at: dir.appendingPathComponent("s.sock").path)
        defer { close(server); close(client) }

        // The test host is not hardened-runtime signed, so the requirement path
        // needs the gate relaxed; hardenedSignedPeerPasses covers the strict path.
        let requirement = try selfDesignatedRequirement()
        try PeerTrust(requirement: requirement, allowUnhardened: true).check(descriptor: server)
    }

    @Test func nonMatchingRequirementRejects() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let (server, client) = acceptedConnection(at: dir.appendingPathComponent("s.sock").path)
        defer { close(server); close(client) }

        do {
            try PeerTrust(requirement: unsatisfiableRequirement).check(descriptor: server)
            Issue.record("expected the Developer-ID requirement to reject the test binary")
        } catch let error as PeerTrust.TrustError {
            guard case .untrustedPeer = error else {
                Issue.record("expected .untrustedPeer, got \(error)")
                return
            }
        }
    }

    @Test func unverifiableRequirementRejects() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let (server, client) = acceptedConnection(at: dir.appendingPathComponent("s.sock").path)
        defer { close(server); close(client) }

        do {
            try PeerTrust(requirement: "this is not a valid requirement (((").check(descriptor: server)
            Issue.record("expected an uncompilable requirement to reject")
        } catch let error as PeerTrust.TrustError {
            guard case .requirementInvalid = error else {
                Issue.record("expected .requirementInvalid, got \(error)")
                return
            }
        }
    }

    @Test func floorGuardedServerRoundTripsSameUserPeer() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path) { request in
            var out = Data("ok:".utf8)
            out.append(request)
            return out
        }
        try server.start()
        defer { server.stop() }

        #expect(sendLine(to: path, Data("hi".utf8)) == Data("ok:hi".utf8))
    }

    @Test func serverClosesConnectionForARejectedPeer() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let path = dir.appendingPathComponent("s.sock").path
        let server = SocketServer(path: path, trust: PeerTrust(requirement: unsatisfiableRequirement)) { request in
            Issue.record("handler must not run for a rejected peer")
            return request
        }
        try server.start()
        defer { server.stop() }

        #expect(sendLine(to: path, Data("hello".utf8))?.isEmpty == true)
    }

    @Test func allowUnhardenedStillEnforcesTheRequirement() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let (server, client) = acceptedConnection(at: dir.appendingPathComponent("s.sock").path)
        defer { close(server); close(client) }

        do {
            try PeerTrust(requirement: unsatisfiableRequirement, allowUnhardened: true).check(descriptor: server)
            Issue.record("expected allowUnhardened to leave the requirement enforced")
        } catch let error as PeerTrust.TrustError {
            guard case .untrustedPeer = error else {
                Issue.record("expected .untrustedPeer, got \(error)")
                return
            }
        }
    }

    @Test func hardenedSignedPeerPasses() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let peer = try spawnSignedPeer(in: dir, options: "runtime")
        defer { peer.close() }

        try PeerTrust(requirement: peer.requirement).check(descriptor: peer.server)
    }

    @Test func unhardenedSignedPeerRejected() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let peer = try spawnSignedPeer(in: dir, options: nil)
        defer { peer.close() }

        do {
            try PeerTrust(requirement: peer.requirement).check(descriptor: peer.server)
            Issue.record("expected the unhardened peer to be rejected")
        } catch let error as PeerTrust.TrustError {
            guard case .hardenedRuntimeMissing = error else {
                Issue.record("expected .hardenedRuntimeMissing, got \(error)")
                return
            }
        }
        try PeerTrust(requirement: peer.requirement, allowUnhardened: true).check(descriptor: peer.server)
    }

    @Test func jitEntitledPeerRejectedEvenUnderForcedLibraryValidation() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let peer = try spawnSignedPeer(
            in: dir, options: "runtime,library", entitlements: [allowJIT]
        )
        defer { peer.close() }

        let status = try peerDynamicStatus(descriptor: peer.server)
        #expect(
            status & (csRequireLV | csForcedLV) != 0,
            "the peer must actually run under enforced library validation"
        )

        do {
            try PeerTrust(requirement: peer.requirement).check(descriptor: peer.server)
            Issue.record("expected the allow-jit peer to be rejected")
        } catch let error as PeerTrust.TrustError {
            #expect(error == .injectionEntitled(allowJIT))
        }
        try PeerTrust(requirement: peer.requirement, allowUnhardened: true).check(descriptor: peer.server)
    }

    @Test func disableLibraryValidationPeerRejected() throws {
        let dir = try shortSocketDir()
        defer { try? FileManager.default.removeItem(at: dir) }
        let peer = try spawnSignedPeer(
            in: dir, options: "runtime", entitlements: [disableLV]
        )
        defer { peer.close() }

        do {
            try PeerTrust(requirement: peer.requirement).check(descriptor: peer.server)
            Issue.record("expected the disable-library-validation peer to be rejected")
        } catch let error as PeerTrust.TrustError {
            #expect(error == .injectionEntitled(disableLV))
        }
        try PeerTrust(requirement: peer.requirement, allowUnhardened: true).check(descriptor: peer.server)
    }
}
