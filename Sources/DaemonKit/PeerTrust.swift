import Darwin
import Foundation
import os
import Security

private let log = Logger(subsystem: DaemonKit.loggingSubsystem, category: "PeerTrust")

/// Verifies the code-signing identity of a connected unix-socket peer.
///
/// Every check enforces a **same-effective-UID floor**: the accepted peer's euid
/// (`getpeereid` on the connection fd) must equal `geteuid()`. The floor is
/// unconditional — no configuration disables or replaces it. When a designated
/// requirement string is configured, the check additionally resolves the peer's
/// code identity from the socket's audit token (`LOCAL_PEERTOKEN`, then
/// `SecCodeCopyGuestWithAttributes` keyed on `kSecGuestAttributeAudit`) and
/// validates it against the compiled `SecRequirement` with `SecCodeCheckValidity`.
/// The requirement augments the floor; it never replaces it. A configured
/// requirement that cannot be verified — audit token unavailable, no code object,
/// or a requirement that will not compile — fails closed with a throw, never a
/// silent downgrade to UID-only.
///
/// `LOCAL_PEERTOKEN` is a query-time binding, not connect-frozen: it is
/// known-unsound against same-UID fork/exec identity substitution, so a surface
/// needing a real per-message identity guarantee uses XPC. That hardening is a
/// later phase.
public struct PeerTrust: Sendable {
    /// Every failure path is fail-closed; the caller maps any throw to a refusal.
    public enum TrustError: Error, Sendable {
        /// `getpeereid` on the connection fd failed.
        case peerCredentialsUnavailable(errno: Int32)
        /// The peer's euid does not equal this process's euid.
        case untrustedUID(peer: uid_t, expected: uid_t)
        /// `getsockopt(LOCAL_PEERTOKEN)` on the connection fd failed.
        case auditTokenUnavailable(errno: Int32)
        /// `SecCodeCopyGuestWithAttributes` could not resolve the peer's code.
        case codeUnresolvable(OSStatus)
        /// The requirement string did not compile into a `SecRequirement`.
        case requirementInvalid(OSStatus)
        /// The peer's code did not satisfy the designated requirement.
        case untrustedPeer(OSStatus)
    }

    /// The designated requirement the peer must satisfy, or `nil` for UID-only
    /// trust (the same-effective-UID floor alone).
    public let requirement: String?

    /// - Parameter requirement: A designated requirement string; `nil` enforces
    ///   the same-effective-UID floor alone.
    public init(requirement: String? = nil) {
        self.requirement = requirement
    }

    /// Throws unless the peer on `descriptor` passes the same-effective-UID floor
    /// and, when a requirement is configured, the designated requirement.
    public func check(descriptor: Int32) throws {
        try enforceUIDFloor(descriptor)
        guard let requirement else { return }
        try enforceRequirement(descriptor, requirement)
    }

    private func enforceUIDFloor(_ descriptor: Int32) throws {
        var euid = uid_t()
        var egid = gid_t()
        guard getpeereid(descriptor, &euid, &egid) == 0 else {
            throw TrustError.peerCredentialsUnavailable(errno: errno)
        }
        let ours = geteuid()
        guard euid == ours else {
            throw TrustError.untrustedUID(peer: euid, expected: ours)
        }
    }

    private func enforceRequirement(_ descriptor: Int32, _ requirement: String) throws {
        var token = try peerAuditToken(descriptor)
        let tokenData = withUnsafeBytes(of: &token) { Data($0) }

        var code: SecCode?
        let guestStatus = SecCodeCopyGuestWithAttributes(
            nil,
            [kSecGuestAttributeAudit: tokenData] as CFDictionary,
            [],
            &code
        )
        guard guestStatus == errSecSuccess, let code else {
            throw TrustError.codeUnresolvable(guestStatus)
        }

        var secRequirement: SecRequirement?
        let requirementStatus = SecRequirementCreateWithString(requirement as CFString, [], &secRequirement)
        guard requirementStatus == errSecSuccess, let secRequirement else {
            throw TrustError.requirementInvalid(requirementStatus)
        }

        let validity = SecCodeCheckValidity(code, [], secRequirement)
        guard validity == errSecSuccess else {
            log.error("peer rejected: OSStatus \(validity, privacy: .public)")
            throw TrustError.untrustedPeer(validity)
        }
    }

    private func peerAuditToken(_ descriptor: Int32) throws -> audit_token_t {
        var token = audit_token_t()
        var length = socklen_t(MemoryLayout<audit_token_t>.size)
        guard getsockopt(descriptor, SOL_LOCAL, LOCAL_PEERTOKEN, &token, &length) == 0 else {
            throw TrustError.auditTokenUnavailable(errno: errno)
        }
        return token
    }
}
