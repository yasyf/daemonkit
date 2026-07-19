import Darwin
import Foundation
import os
import Security

private let log = Logger(subsystem: DaemonKit.loggingSubsystem, category: "PeerTrust")

// Dynamic code-signing status bits, verified against xnu's cs_blobs.h.
private let csGetTaskAllow: UInt32 = 0x0000_0004 // CS_GET_TASK_ALLOW: get-task-allow entitlement
private let csForcedLV: UInt32 = 0x0000_0010 // CS_FORCED_LV: library validation forced by system policy
private let csRequireLV: UInt32 = 0x0000_2000 // CS_REQUIRE_LV: library validation required
private let csRuntime: UInt32 = 0x0001_0000 // CS_RUNTIME: Hardened Runtime (codesign --options runtime)
private let csDebugged: UInt32 = 0x1000_0000 // CS_DEBUGGED: ran with invalid pages under a debugger

/// entDisableLV turns off library validation unless CS_REQUIRE_LV/CS_FORCED_LV
/// enforce it dynamically anyway.
private let entDisableLV = "com.apple.security.cs.disable-library-validation"

/// injectionEntitlements re-open code injection or debugger attachment on a
/// Hardened Runtime binary; a peer signed with any of them is untrusted.
private let injectionEntitlements = [
    entDisableLV,
    "com.apple.security.cs.allow-dyld-environment-variables",
    "com.apple.security.cs.allow-unsigned-executable-memory",
    "com.apple.security.cs.allow-jit",
    "com.apple.security.cs.disable-executable-page-protection",
    "com.apple.security.get-task-allow",
]

/// Verifies the code-signing identity of a connected unix-socket peer: an
/// unconditional same-effective-UID floor plus an optional designated
/// requirement resolved from the socket's audit token. The requirement
/// augments the floor, never replaces it; every failure path fails closed.
///
/// `LOCAL_PEERTOKEN` binds at query time — known-unsound against same-UID
/// fork/exec identity substitution; a real per-message guarantee needs XPC.
public struct PeerTrust: Sendable {
    /// Every failure path is fail-closed; the caller maps any throw to a refusal.
    public enum TrustError: Error, Equatable, Sendable {
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
        /// `SecCodeCopySigningInformation` on the peer's code failed.
        case signingInfoUnavailable(OSStatus)
        /// The peer's signing information carries no dynamic code-signing status.
        case signingStatusUnavailable
        /// The peer was signed without the Hardened Runtime.
        case hardenedRuntimeMissing(status: UInt32)
        /// The peer permits debugger attachment (`CS_GET_TASK_ALLOW`).
        case debuggable(status: UInt32)
        /// The peer ran with invalid pages under a debugger (`CS_DEBUGGED`).
        case debugged(status: UInt32)
        /// The peer's entitlements are not in dictionary form, so they cannot
        /// be proven free of injection entitlements.
        case entitlementsUndecodable
        /// The peer is signed with an injection-enabling entitlement.
        case injectionEntitled(String)
    }

    /// The designated requirement the peer must satisfy, or `nil` for UID-only
    /// trust (the same-effective-UID floor alone).
    public let requirement: String?

    /// Skips the Hardened Runtime and injection-entitlement gate. The requirement
    /// and the UID floor still apply; enabling it is a security decision.
    public let allowUnhardened: Bool

    /// Creates a policy enforcing the UID floor plus the optional designated requirement.
    public init(requirement: String? = nil, allowUnhardened: Bool = false) {
        self.requirement = requirement
        self.allowUnhardened = allowUnhardened
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

        if allowUnhardened {
            return
        }
        try enforceHardenedRuntime(code)
    }

    private func enforceHardenedRuntime(_ code: SecCode) throws {
        // The audit-token guest is what SecCodeCopySigningInformation needs for
        // live status flags; Swift imports SecCode/SecStaticCode as unrelated: bit cast.
        let staticCode = unsafeBitCast(code, to: SecStaticCode.self)
        var info: CFDictionary?
        let flags = SecCSFlags(rawValue: kSecCSDynamicInformation | kSecCSRequirementInformation)
        let status = SecCodeCopySigningInformation(staticCode, flags, &info)
        guard status == errSecSuccess, let info else {
            throw TrustError.signingInfoUnavailable(status)
        }
        try Self.enforceHardenedRuntime(info: info as NSDictionary)
    }

    static func enforceHardenedRuntime(info: NSDictionary) throws {
        guard let signingStatus = (info[kSecCodeInfoStatus] as? NSNumber)?.uint32Value else {
            throw TrustError.signingStatusUnavailable
        }
        guard signingStatus & csRuntime != 0 else {
            throw TrustError.hardenedRuntimeMissing(status: signingStatus)
        }
        guard signingStatus & csGetTaskAllow == 0 else {
            throw TrustError.debuggable(status: signingStatus)
        }
        guard signingStatus & csDebugged == 0 else {
            throw TrustError.debugged(status: signingStatus)
        }
        let lvProven = signingStatus & (csRequireLV | csForcedLV) != 0
        let entitlementsValue = info[kSecCodeInfoEntitlementsDict]
        guard let entitlements = entitlementsValue as? NSDictionary else {
            // Only both keys ABSENT is clean; a present-but-undecodable value
            // under either key cannot be proven free of injection entitlements.
            guard entitlementsValue == nil, info[kSecCodeInfoEntitlements] == nil else {
                throw TrustError.entitlementsUndecodable
            }
            return
        }
        for entitlement in injectionEntitlements {
            // Inert when the kernel enforces library validation regardless.
            if lvProven, entitlement == entDisableLV {
                continue
            }
            guard let value = entitlements[entitlement] else { continue }
            // Only a genuine CFBoolean false is clean: CFEqual alone also
            // matches CFNumber(0) through NSNumber bridging.
            let ref = value as CFTypeRef
            guard CFGetTypeID(ref) == CFBooleanGetTypeID(), CFEqual(ref, kCFBooleanFalse) else {
                throw TrustError.injectionEntitled(entitlement)
            }
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
