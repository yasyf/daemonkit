@testable import DaemonKit
import Foundation
import Testing

@Suite(.timeLimit(.minutes(1)))
struct AppGroupContainerTests {
    @Test(arguments: ["", "group", " group.example", "group.example ", "group.example/nested"])
    func rejectsInvalidIdentifier(_ identifier: String) {
        #expect(throws: AppGroupContainer.ContainerError.invalidIdentifier(identifier)) {
            try AppGroupContainer(identifier: identifier)
        }
    }

    @Test(arguments: ["", ".", "..", "nested/socket", "/absolute"])
    func rejectsNonLeafSocketPath(_ leaf: String) throws {
        let container = try AppGroupContainer(identifier: "group.example.unavailable")
        #expect(throws: AppGroupContainer.ContainerError.invalidLeaf(leaf)) {
            try container.socketPath(leaf: leaf)
        }
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

    @Test func entitledResolutionReturnsSystemURL() throws {
        let expected = URL(fileURLWithPath: "/group-container")
        let actual = try AppGroupContainer.resolve(
            identifier: "group.example.expected",
            entitledGroups: ["group.example.expected"],
            containerURL: { _ in expected }
        )
        #expect(actual == expected)
    }
}
