import Darwin
import Foundation
import os
import Security

private let log = Logger(subsystem: DaemonKit.loggingSubsystem, category: "PeerTrust")

private let csGetTaskAllow: UInt32 = 0x0000_0004
private let csForcedLV: UInt32 = 0x0000_0010
private let csRequireLV: UInt32 = 0x0000_2000
private let csRuntime: UInt32 = 0x0001_0000
private let csDebugged: UInt32 = 0x1000_0000

private let entDisableLV = "com.apple.security.cs.disable-library-validation"
private let applicationGroupsEntitlement = "com.apple.security.application-groups"

private let injectionEntitlements = [
    entDisableLV,
    "com.apple.security.cs.allow-dyld-environment-variables",
    "com.apple.security.cs.allow-unsigned-executable-memory",
    "com.apple.security.cs.allow-jit",
    "com.apple.security.cs.disable-executable-page-protection",
    "com.apple.security.get-task-allow",
]

/// PeerTrust verifies one explicitly configured signed consumer at unix-socket
/// admission. The same-effective-UID check is an unconditional floor.
///
/// `LOCAL_PEERTOKEN` is a query-time process reference, not an immutable record
/// of the process that originally connected. The signed policy sharply limits
/// substitution, but descriptor delegation or substitution by another process
/// satisfying the same policy before admission remains a platform limitation.
public struct PeerTrust: Sendable {
    /// A closed required-entitlement predicate.
    public enum EntitlementRequirement: Equatable, Sendable {
        case boolean(Bool)
        case string(String)
        case stringArrayContains(String)
    }

    /// Configuration errors are rejected before a server can accept peers.
    public enum RequirementError: Error, Equatable, Sendable {
        case emptyTeamIdentifier
        case emptySigningIdentifier
        case emptyRequiredAppGroup
        case unsafeIdentityField(String)
        case emptyEntitlementKey
        case duplicateAppGroupRequirement
        case emptyEntitlementValue(String)
    }

    /// Requirement pins the consumer's Developer ID identity and any mandatory
    /// capabilities owned by that consumer.
    public struct Requirement: Equatable, Sendable {
        public let teamIdentifier: String
        public let signingIdentifier: String
        public let requiredEntitlements: [String: EntitlementRequirement]

        public init(
            teamIdentifier: String,
            signingIdentifier: String,
            requiredAppGroup: String? = nil,
            requiredEntitlements: [String: EntitlementRequirement] = [:]
        ) throws {
            guard !teamIdentifier.isEmpty else { throw RequirementError.emptyTeamIdentifier }
            guard !signingIdentifier.isEmpty else { throw RequirementError.emptySigningIdentifier }
            for value in [teamIdentifier, signingIdentifier] where value.contains("\"") || value.contains("\\") {
                throw RequirementError.unsafeIdentityField(value)
            }
            if let requiredAppGroup {
                guard !requiredAppGroup.isEmpty else { throw RequirementError.emptyRequiredAppGroup }
                guard requiredEntitlements[applicationGroupsEntitlement] == nil else {
                    throw RequirementError.duplicateAppGroupRequirement
                }
            }
            for (key, requirement) in requiredEntitlements {
                guard !key.isEmpty else { throw RequirementError.emptyEntitlementKey }
                switch requirement {
                case .boolean:
                    break
                case let .string(value), let .stringArrayContains(value):
                    guard !value.isEmpty else { throw RequirementError.emptyEntitlementValue(key) }
                }
            }
            self.teamIdentifier = teamIdentifier
            self.signingIdentifier = signingIdentifier
            var entitlements = requiredEntitlements
            if let requiredAppGroup {
                entitlements[applicationGroupsEntitlement] = .stringArrayContains(requiredAppGroup)
            }
            self.requiredEntitlements = entitlements
        }

        var designatedRequirement: String {
            "identifier \"\(signingIdentifier)\" and anchor apple generic " +
                "and certificate leaf[subject.OU] = \"\(teamIdentifier)\" " +
                "and certificate 1[field.1.2.840.113635.100.6.2.6] exists " +
                "and certificate leaf[field.1.2.840.113635.100.6.1.13] exists"
        }
    }

    public enum TrustError: Error, Equatable, Sendable {
        case peerCredentialsUnavailable(errno: Int32)
        case untrustedUID(peer: uid_t, expected: uid_t)
        case auditTokenUnavailable(errno: Int32)
        case codeUnresolvable(OSStatus)
        case requirementInvalid(OSStatus)
        case untrustedPeer(OSStatus)
        case signingInfoUnavailable(OSStatus)
        case signingStatusUnavailable
        case hardenedRuntimeMissing(status: UInt32)
        case debuggable(status: UInt32)
        case debugged(status: UInt32)
        case entitlementsUndecodable
        case injectionEntitled(String)
        case requiredEntitlementMissing(String)
        case requiredEntitlementMismatch(String)
    }

    private enum Mode: Sendable {
        case signed(Requirement)
        case testingUIDOnly
    }

    private let mode: Mode

    public init(requirement: Requirement) {
        mode = .signed(requirement)
    }

    static var testingUIDOnly: PeerTrust {
        PeerTrust(mode: .testingUIDOnly)
    }

    private init(mode: Mode) {
        self.mode = mode
    }

    /// Throws unless the peer passes the UID floor and configured signed policy.
    public func check(descriptor: Int32) throws {
        try enforceUIDFloor(descriptor)
        switch mode {
        case let .signed(requirement):
            try enforceRequirement(descriptor, requirement)
        case .testingUIDOnly:
            return
        }
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

    private func enforceRequirement(_ descriptor: Int32, _ requirement: Requirement) throws {
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
        let requirementStatus = SecRequirementCreateWithString(
            requirement.designatedRequirement as CFString,
            [],
            &secRequirement
        )
        guard requirementStatus == errSecSuccess, let secRequirement else {
            throw TrustError.requirementInvalid(requirementStatus)
        }

        let validity = SecCodeCheckValidity(code, [], secRequirement)
        guard validity == errSecSuccess else {
            log.error(
                "signed peer rejected: team=\(requirement.teamIdentifier, privacy: .public) identifier=\(requirement.signingIdentifier, privacy: .public) status=\(validity, privacy: .public)"
            )
            throw TrustError.untrustedPeer(validity)
        }

        try enforceSignedPeer(code, requiredEntitlements: requirement.requiredEntitlements)
    }

    private func enforceSignedPeer(
        _ code: SecCode,
        requiredEntitlements: [String: EntitlementRequirement]
    ) throws {
        let staticCode = unsafeBitCast(code, to: SecStaticCode.self)
        var info: CFDictionary?
        let flags = SecCSFlags(rawValue: kSecCSDynamicInformation | kSecCSRequirementInformation)
        let status = SecCodeCopySigningInformation(staticCode, flags, &info)
        guard status == errSecSuccess, let info else {
            throw TrustError.signingInfoUnavailable(status)
        }
        try Self.enforceSignedPeer(
            info: info as NSDictionary,
            requiredEntitlements: requiredEntitlements
        )
    }

    static func enforceSignedPeer(
        info: NSDictionary,
        requiredEntitlements: [String: EntitlementRequirement]
    ) throws {
        try enforceHardenedRuntime(info: info)
        let entitlements = try decodedEntitlements(info: info)
        for key in requiredEntitlements.keys.sorted() {
            guard let actual = entitlements?[key] else {
                throw TrustError.requiredEntitlementMissing(key)
            }
            guard matches(actual, requirement: requiredEntitlements[key]!) else {
                throw TrustError.requiredEntitlementMismatch(key)
            }
        }
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
        guard let entitlements = try decodedEntitlements(info: info) else { return }
        for entitlement in injectionEntitlements {
            if lvProven, entitlement == entDisableLV { continue }
            guard let value = entitlements[entitlement] else { continue }
            let ref = value as CFTypeRef
            guard CFGetTypeID(ref) == CFBooleanGetTypeID(), CFEqual(ref, kCFBooleanFalse) else {
                throw TrustError.injectionEntitled(entitlement)
            }
        }
    }

    private static func decodedEntitlements(info: NSDictionary) throws -> NSDictionary? {
        let value = info[kSecCodeInfoEntitlementsDict]
        if let entitlements = value as? NSDictionary {
            return entitlements
        }
        guard value == nil, info[kSecCodeInfoEntitlements] == nil else {
            throw TrustError.entitlementsUndecodable
        }
        return nil
    }

    private static func matches(_ actual: Any, requirement: EntitlementRequirement) -> Bool {
        switch requirement {
        case let .boolean(expected):
            let ref = actual as CFTypeRef
            let wanted = expected ? kCFBooleanTrue : kCFBooleanFalse
            return CFGetTypeID(ref) == CFBooleanGetTypeID() && CFEqual(ref, wanted)
        case let .string(expected):
            return (actual as? String) == expected
        case let .stringArrayContains(expected):
            return (actual as? [String])?.contains(expected) == true
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
