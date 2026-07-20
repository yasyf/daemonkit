import Foundation
import Security

/// AppGroupContainer resolves paths inside a signed process's App Group container.
public struct AppGroupContainer: Sendable {
    public enum ContainerError: Error, Equatable, Sendable {
        case invalidIdentifier(String)
        case signingInfoUnavailable(OSStatus)
        case entitlementsUnavailable
        case entitlementMissing(String)
        case unavailable(String)
        case invalidLeaf(String)
    }

    /// SocketLeaf is one validated non-directory socket filename.
    public struct SocketLeaf: Hashable, Sendable {
        public let rawValue: String

        public init(_ rawValue: String) throws {
            guard !rawValue.isEmpty,
                  rawValue != ".",
                  rawValue != "..",
                  !rawValue.contains("/"),
                  !rawValue.utf8.contains(0)
            else {
                throw ContainerError.invalidLeaf(rawValue)
            }
            self.rawValue = rawValue
        }
    }

    public let identifier: String

    public init(identifier: String) throws {
        guard Self.isValidIdentifier(identifier) else {
            throw ContainerError.invalidIdentifier(identifier)
        }
        self.identifier = identifier
    }

    /// directory returns the entitlement-resolved App Group container.
    public func directory() throws -> URL {
        try Self.resolve(
            identifier: identifier,
            entitledGroups: Self.entitledGroups(),
            containerURL: FileManager.default.containerURL
        )
    }

    /// socketPath returns a socket path beneath the resolved container.
    public func socketPath(leaf: SocketLeaf) throws -> String {
        try directory().appendingPathComponent(leaf.rawValue, isDirectory: false).path
    }

    static func resolve(
        identifier: String,
        entitledGroups: Set<String>,
        containerURL: (String) -> URL?
    ) throws -> URL {
        guard entitledGroups.contains(identifier) else {
            throw ContainerError.entitlementMissing(identifier)
        }
        guard let url = containerURL(identifier) else {
            throw ContainerError.unavailable(identifier)
        }
        return url
    }

    private static func entitledGroups() throws -> Set<String> {
        var code: SecCode?
        let selfStatus = SecCodeCopySelf([], &code)
        guard selfStatus == errSecSuccess, let code else {
            throw ContainerError.signingInfoUnavailable(selfStatus)
        }
        let staticCode = unsafeBitCast(code, to: SecStaticCode.self)
        var signingInfo: CFDictionary?
        let infoStatus = SecCodeCopySigningInformation(
            staticCode,
            SecCSFlags(rawValue: kSecCSSigningInformation),
            &signingInfo
        )
        guard infoStatus == errSecSuccess, let signingInfo else {
            throw ContainerError.signingInfoUnavailable(infoStatus)
        }
        let info = signingInfo as NSDictionary
        guard let entitlements = info[kSecCodeInfoEntitlementsDict] as? NSDictionary else {
            throw ContainerError.entitlementsUnavailable
        }
        guard let groups = entitlements["com.apple.security.application-groups"] as? [String] else {
            return []
        }
        return Set(groups)
    }

    private static func isValidIdentifier(_ identifier: String) -> Bool {
        let segments = identifier.split(separator: ".", omittingEmptySubsequences: false)
        guard segments.count >= 2,
              segments.dropFirst().allSatisfy(isValidSegment)
        else {
            return false
        }
        if segments[0] == "group" {
            return true
        }
        return segments[0].utf8.count == 10 && segments[0].unicodeScalars.allSatisfy {
            (48 ... 57).contains($0.value) || (65 ... 90).contains($0.value)
        }
    }

    private static func isValidSegment(_ segment: Substring) -> Bool {
        guard let first = segment.unicodeScalars.first,
              let last = segment.unicodeScalars.last,
              isASCIIAlphanumeric(first),
              isASCIIAlphanumeric(last)
        else {
            return false
        }
        return segment.unicodeScalars.allSatisfy {
            isASCIIAlphanumeric($0) || $0.value == 45
        }
    }

    private static func isASCIIAlphanumeric(_ scalar: Unicode.Scalar) -> Bool {
        (48 ... 57).contains(scalar.value) ||
            (65 ... 90).contains(scalar.value) ||
            (97 ... 122).contains(scalar.value)
    }
}
