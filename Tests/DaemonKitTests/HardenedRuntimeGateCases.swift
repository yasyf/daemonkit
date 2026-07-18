@testable import DaemonKit
import Foundation
import Security
import Testing

// Mirrors of xnu cs_blobs.h status bits, independent of the implementation's.
let csValid: UInt32 = 0x0000_0001
let csGetTaskAllow: UInt32 = 0x0000_0004
let csForcedLV: UInt32 = 0x0000_0010
let csRequireLV: UInt32 = 0x0000_2000
let csRuntime: UInt32 = 0x0001_0000
let csDebugged: UInt32 = 0x1000_0000

let allowJIT = "com.apple.security.cs.allow-jit"
let disableLV = "com.apple.security.cs.disable-library-validation"

private struct GateCase: Sendable, CustomTestStringConvertible {
    let name: String
    let status: UInt32?
    let entitlements: [String: Bool]?
    let rawEntitlementsOnly: Bool
    let expected: PeerTrust.TrustError?

    init(
        _ name: String,
        status: UInt32?,
        entitlements: [String: Bool]? = nil,
        rawEntitlementsOnly: Bool = false,
        expected: PeerTrust.TrustError?
    ) {
        self.name = name
        self.status = status
        self.entitlements = entitlements
        self.rawEntitlementsOnly = rawEntitlementsOnly
        self.expected = expected
    }

    var testDescription: String {
        name
    }
}

private let gateCases: [GateCase] = [
    GateCase("missing status", status: nil, expected: .signingStatusUnavailable),
    GateCase("unhardened", status: csValid, expected: .hardenedRuntimeMissing(status: csValid)),
    GateCase(
        "get-task-allow flag", status: csValid | csRuntime | csGetTaskAllow,
        expected: .debuggable(status: csValid | csRuntime | csGetTaskAllow)
    ),
    GateCase(
        "debugged", status: csValid | csRuntime | csDebugged,
        expected: .debugged(status: csValid | csRuntime | csDebugged)
    ),
    GateCase("hardened, no entitlements", status: csValid | csRuntime, expected: nil),
    GateCase(
        "hardened, benign entitlements", status: csValid | csRuntime,
        entitlements: ["com.apple.security.app-sandbox": true], expected: nil
    ),
    GateCase(
        "allow-jit", status: csValid | csRuntime,
        entitlements: [allowJIT: true], expected: .injectionEntitled(allowJIT)
    ),
    GateCase(
        "allow-jit under forced library validation",
        status: csValid | csRuntime | csRequireLV | csForcedLV,
        entitlements: [allowJIT: true], expected: .injectionEntitled(allowJIT)
    ),
    GateCase(
        "disable-library-validation unenforced", status: csValid | csRuntime,
        entitlements: [disableLV: true], expected: .injectionEntitled(disableLV)
    ),
    GateCase(
        "disable-library-validation inert under CS_REQUIRE_LV",
        status: csValid | csRuntime | csRequireLV,
        entitlements: [disableLV: true], expected: nil
    ),
    GateCase(
        "disable-library-validation inert under CS_FORCED_LV",
        status: csValid | csRuntime | csForcedLV,
        entitlements: [disableLV: true], expected: nil
    ),
    GateCase(
        "false-valued entitlement is clean", status: csValid | csRuntime,
        entitlements: [allowJIT: false], expected: nil
    ),
    GateCase(
        "entitlements not in dictionary form", status: csValid | csRuntime,
        rawEntitlementsOnly: true, expected: .entitlementsUndecodable
    ),
    GateCase(
        "dyld environment variables", status: csValid | csRuntime,
        entitlements: ["com.apple.security.cs.allow-dyld-environment-variables": true],
        expected: .injectionEntitled("com.apple.security.cs.allow-dyld-environment-variables")
    ),
    GateCase(
        "unsigned executable memory", status: csValid | csRuntime,
        entitlements: ["com.apple.security.cs.allow-unsigned-executable-memory": true],
        expected: .injectionEntitled("com.apple.security.cs.allow-unsigned-executable-memory")
    ),
    GateCase(
        "disabled executable page protection", status: csValid | csRuntime,
        entitlements: ["com.apple.security.cs.disable-executable-page-protection": true],
        expected: .injectionEntitled("com.apple.security.cs.disable-executable-page-protection")
    ),
    GateCase(
        "get-task-allow entitlement", status: csValid | csRuntime,
        entitlements: ["com.apple.security.get-task-allow": true],
        expected: .injectionEntitled("com.apple.security.get-task-allow")
    ),
]

struct HardenedRuntimeGateTests {
    @Test(arguments: gateCases) private func hardenedRuntimeGate(testCase: GateCase) {
        var info: [String: Any] = [:]
        if let status = testCase.status {
            info[kSecCodeInfoStatus as String] = status
        }
        if let entitlements = testCase.entitlements {
            info[kSecCodeInfoEntitlementsDict as String] = entitlements
        }
        if testCase.rawEntitlementsOnly {
            info[kSecCodeInfoEntitlements as String] = Data([0xFA, 0xDE])
        }

        do {
            try PeerTrust.enforceHardenedRuntime(info: info as NSDictionary)
            #expect(testCase.expected == nil, "passed, expected \(String(describing: testCase.expected))")
        } catch let error as PeerTrust.TrustError {
            #expect(error == testCase.expected)
        } catch {
            Issue.record("unexpected error \(error)")
        }
    }

    @Test func numericEntitlementValueRejected() throws {
        let info: [String: Any] = [
            kSecCodeInfoStatus as String: csValid | csRuntime,
            kSecCodeInfoEntitlementsDict as String: [allowJIT: 0],
        ]
        do {
            try PeerTrust.enforceHardenedRuntime(info: info as NSDictionary)
            Issue.record("expected a CFNumber(0) entitlement value to be rejected")
        } catch let error as PeerTrust.TrustError {
            #expect(error == .injectionEntitled(allowJIT))
        }
    }

    @Test func nonDictionaryEntitlementsValueRejected() throws {
        let info: [String: Any] = [
            kSecCodeInfoStatus as String: csValid | csRuntime,
            kSecCodeInfoEntitlementsDict as String: Data([0x01]),
        ]
        do {
            try PeerTrust.enforceHardenedRuntime(info: info as NSDictionary)
            Issue.record("expected a non-dictionary entitlements value to be rejected")
        } catch let error as PeerTrust.TrustError {
            #expect(error == .entitlementsUndecodable)
        }
    }
}
