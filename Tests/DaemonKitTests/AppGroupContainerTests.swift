@testable import DaemonKit
import Foundation
import Testing

@Suite(.timeLimit(.minutes(1)))
struct AppGroupContainerTests {
    @Test(arguments: [
        "group.example",
        "group.com.example.product-name",
        "SXKCTF23Q2.ccp",
        "SXKCTF23Q2.com.example.ccp",
    ])
    func acceptsCanonicalAppGroupIdentifier(_ identifier: String) throws {
        let container = try AppGroupContainer(identifier: identifier)
        #expect(container.identifier == identifier)
    }

    @Test(arguments: [
        "",
        "group",
        " group.example",
        "group.example ",
        "group.ex ample",
        "group.example/nested",
        "group.example\0nested",
        "group..example",
        "group.example.",
        "group.-example",
        "group.example-",
        "group.ex_ample",
        "group.exämple",
        "SXKCTF23Q2",
        "SXKCTF23Q.ccp",
        "SXKCTF23Q23.ccp",
        "sxkctf23q2.ccp",
        "SXKCTF23Q2..ccp",
    ])
    func rejectsInvalidIdentifier(_ identifier: String) {
        #expect(throws: AppGroupContainer.ContainerError.invalidIdentifier(identifier)) {
            try AppGroupContainer(identifier: identifier)
        }
    }

    @Test(arguments: ["", ".", "..", "nested/socket", "/absolute"])
    func rejectsNonLeafSocketPath(_ leaf: String) throws {
        #expect(throws: AppGroupContainer.ContainerError.invalidLeaf(leaf)) {
            try AppGroupContainer.SocketLeaf(leaf)
        }
    }

    @Test func socketLeafIsValidBeforeProtectedContainerResolution() throws {
        let leaf = try AppGroupContainer.SocketLeaf("catalog.sock")
        #expect(leaf.rawValue == "catalog.sock")
    }

    @Test func entitlementMismatchFailsBeforeResolution() throws {
        var resolved = false
        #expect(throws: AppGroupContainer.ContainerError.entitlementMissing("group.example.expected")) {
            try AppGroupContainer.resolve(
                identifier: "group.example.expected",
                entitledGroups: ["group.example.other"],
                containerURL: { _ in
                    resolved = true
                    return URL(fileURLWithPath: "/should-not-resolve")
                }
            )
        }
        #expect(!resolved)
    }

    @Test(arguments: ["group.example.expected", "SXKCTF23Q2.ccp"])
    func entitledResolutionReturnsSystemURL(identifier: String) throws {
        let expected = URL(fileURLWithPath: "/group-container")
        let actual = try AppGroupContainer.resolve(
            identifier: identifier,
            entitledGroups: [identifier],
            containerURL: { _ in expected }
        )
        #expect(actual == expected)
    }
}
