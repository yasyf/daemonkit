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

    public let identifier: String

    public init(identifier: String) throws {
        guard identifier.hasPrefix("group."),
              identifier == identifier.trimmingCharacters(in: .whitespacesAndNewlines),
              !identifier.contains("/"),
              !identifier.utf8.contains(0)
        else {
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
    public func socketPath(leaf: String) throws -> String {
        guard !leaf.isEmpty,
              leaf != ".",
              leaf != "..",
              !leaf.contains("/"),
              !leaf.utf8.contains(0)
        else {
            throw ContainerError.invalidLeaf(leaf)
        }
        return try directory().appendingPathComponent(leaf, isDirectory: false).path
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
}
