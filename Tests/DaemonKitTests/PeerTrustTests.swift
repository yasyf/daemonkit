@testable import DaemonKit
import Darwin
import Foundation
import Security
import Testing

private let fixtureTeam = "SXKCTF23Q2"
private let fixtureGroup = "group.com.yasyf.daemonkit.fixture"
private let applicationGroupsKey = "com.apple.security.application-groups"
private let runtimeAndLibraryValidation: UInt32 = 0x0001_0000 | 0x0000_2000
private let trustE2EEnabled = ProcessInfo.processInfo.environment["DAEMONKIT_TRUST_E2E"] == "1"

private func withAddress<Result>(
    _ address: inout sockaddr_un,
    _ body: (UnsafePointer<sockaddr>, socklen_t) -> Result
) -> Result {
    withUnsafePointer(to: &address) { pointer in
        pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { rebound in
            body(rebound, socklen_t(MemoryLayout<sockaddr_un>.size))
        }
    }
}

private func acceptedConnection(at path: String) -> (server: Int32, client: Int32) {
    guard var address = makeAddress(path: path) else { preconditionFailure("socket path too long") }
    let listener = socket(AF_UNIX, SOCK_STREAM, 0)
    precondition(listener >= 0)
    precondition(withAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
    precondition(listen(listener, 1) == 0)
    let client = socket(AF_UNIX, SOCK_STREAM, 0)
    precondition(client >= 0)
    precondition(withAddress(&address) { connect(client, $0, $1) } == 0)
    let server = accept(listener, nil, nil)
    precondition(server >= 0)
    close(listener)
    return (server, client)
}

private func fixtureRequirement(
    identifier: String = "com.yasyf.daemonkit.fixture-a",
    appGroup: String = fixtureGroup
) throws -> PeerTrust.Requirement {
    try PeerTrust.Requirement(
        teamIdentifier: fixtureTeam,
        signingIdentifier: identifier,
        requiredAppGroup: appGroup
    )
}

private struct FixturePeer {
    let server: Int32
    let process: Process

    func close() {
        Darwin.close(server)
        process.terminate()
        process.waitUntilExit()
    }
}

private final class SocketReadProbe: @unchecked Sendable {
    private let lock = NSLock()
    private var count: Int?

    func record(_ value: Int) {
        lock.lock()
        count = value
        lock.unlock()
    }

    func value() -> Int? {
        lock.lock()
        defer { lock.unlock() }
        return count
    }
}

private func spawnFixture(_ name: String, in directory: URL) throws -> FixturePeer {
    let executable = URL(fileURLWithPath: FileManager.default.currentDirectoryPath)
        .appendingPathComponent(".trust-fixtures")
        .appendingPathComponent(name)
    try #require(FileManager.default.fileExists(atPath: executable.path), "run scripts/trust-fixtures.sh .trust-fixtures")
    let path = directory.appendingPathComponent("fixture.sock").path
    var address = try #require(makeAddress(path: path))
    let listener = socket(AF_UNIX, SOCK_STREAM, 0)
    try #require(listener >= 0)
    defer { close(listener) }
    try #require(withAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
    try #require(listen(listener, 1) == 0)

    let process = Process()
    process.executableURL = executable
    process.arguments = [path]
    try process.run()
    var poller = pollfd(fd: listener, events: Int16(POLLIN), revents: 0)
    try #require(poll(&poller, 1, 5000) == 1, "signed fixture never connected")
    let server = accept(listener, nil, nil)
    try #require(server >= 0)
    return FixturePeer(server: server, process: process)
}

extension SocketTransportTests {
    struct PeerTrustTests {
        @Test func requirementBuildsCanonicalDeveloperIDPolicy() throws {
            let requirement = try PeerTrust.Requirement(
                teamIdentifier: fixtureTeam,
                signingIdentifier: "com.yasyf.consumer",
                requiredAppGroup: "group.com.yasyf.consumer",
                requiredEntitlements: [
                    "com.yasyf.role": .string("broker"),
                ]
            )
            #expect(requirement.designatedRequirement ==
                "identifier \"com.yasyf.consumer\" and anchor apple generic " +
                "and certificate leaf[subject.OU] = \"SXKCTF23Q2\" " +
                "and certificate 1[field.1.2.840.113635.100.6.2.6] exists " +
                "and certificate leaf[field.1.2.840.113635.100.6.1.13] exists")
            #expect(requirement.requiredEntitlements[applicationGroupsKey] == .stringArrayContains("group.com.yasyf.consumer"))
            #expect(requirement.requiredEntitlements["com.yasyf.role"] == .string("broker"))
        }

        @Test func requirementRejectsIncompleteOrAmbiguousIdentity() {
            #expect(throws: PeerTrust.RequirementError.emptyTeamIdentifier) {
                _ = try PeerTrust.Requirement(teamIdentifier: "", signingIdentifier: "com.yasyf.x")
            }
            #expect(throws: PeerTrust.RequirementError.emptySigningIdentifier) {
                _ = try PeerTrust.Requirement(teamIdentifier: fixtureTeam, signingIdentifier: "")
            }
            #expect(throws: PeerTrust.RequirementError.emptyRequiredAppGroup) {
                _ = try PeerTrust.Requirement(
                    teamIdentifier: fixtureTeam,
                    signingIdentifier: "com.yasyf.x",
                    requiredAppGroup: ""
                )
            }
            #expect(throws: PeerTrust.RequirementError.unsafeIdentityField("bad\"team")) {
                _ = try PeerTrust.Requirement(teamIdentifier: "bad\"team", signingIdentifier: "com.yasyf.x")
            }
            #expect(throws: PeerTrust.RequirementError.duplicateAppGroupRequirement) {
                _ = try PeerTrust.Requirement(
                    teamIdentifier: fixtureTeam,
                    signingIdentifier: "com.yasyf.x",
                    requiredAppGroup: "group.x",
                    requiredEntitlements: [applicationGroupsKey: .stringArrayContains("group.other")]
                )
            }
        }

        @Test func signedPolicyWithoutAppGroupIsValid() throws {
            let requirement = try PeerTrust.Requirement(
                teamIdentifier: fixtureTeam,
                signingIdentifier: "com.yasyf.consumer"
            )
            #expect(requirement.requiredEntitlements.isEmpty)
        }

        @Test func sameUIDFloorStillAdmitsTheConnectedTestProcess() throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let connection = acceptedConnection(at: directory.appendingPathComponent("s.sock").path)
            defer { close(connection.server); close(connection.client) }
            try PeerTrust.sameEffectiveUser.check(descriptor: connection.server)
        }

        @Test func typedDeveloperIDRequirementRejectsTheAdHocTestProcess() throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let connection = acceptedConnection(at: directory.appendingPathComponent("s.sock").path)
            defer { close(connection.server); close(connection.client) }
            do {
                try PeerTrust(requirement: fixtureRequirement()).check(descriptor: connection.server)
                Issue.record("expected the ad-hoc test process to fail the Developer ID policy")
            } catch let error as PeerTrust.TrustError {
                guard case .untrustedPeer = error else {
                    Issue.record("expected .untrustedPeer, got \(error)")
                    return
                }
            }
        }

        @Test func kernelPeerPIDMustMatchAuditTokenPID() {
            #expect(throws: PeerTrust.TrustError.auditTokenPIDMismatch(peer: 41, audit: 42)) {
                try PeerTrust.enforcePeerIdentity(peerPID: 41, auditPID: 42)
            }
        }

        @Test func clientRejectsServerBeforeWritingProtocolBytes() async throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let path = directory.appendingPathComponent("s.sock").path
            var address = try #require(makeAddress(path: path))
            let listener = socket(AF_UNIX, SOCK_STREAM, 0)
            try #require(listener >= 0)
            defer { close(listener) }
            try #require(withAddress(&address) { Darwin.bind(listener, $0, $1) } == 0)
            try #require(listen(listener, 1) == 0)

            let settlement = Task.detached {
                let peer = accept(listener, nil, nil)
                guard peer >= 0 else {
                    return Int(-1)
                }
                defer { close(peer) }
                var poller = pollfd(fd: peer, events: Int16(POLLIN | POLLHUP), revents: 0)
                guard poll(&poller, 1, 5000) == 1 else {
                    return Int(-2)
                }
                var byte: UInt8 = 0
                return read(peer, &byte, 1)
            }

            await #expect(throws: PeerTrust.TrustError.self) {
                _ = try await SocketClient(
                    path: path,
                    build: "server-test",
                    trust: PeerTrust(requirement: fixtureRequirement())
                )
            }
            #expect(await settlement.value == 0)
        }

        @Test func requiredEntitlementsMatchClosedTypedPredicates() throws {
            let requirement = try PeerTrust.Requirement(
                teamIdentifier: fixtureTeam,
                signingIdentifier: "com.yasyf.consumer",
                requiredAppGroup: "group.com.yasyf.consumer",
                requiredEntitlements: [
                    "com.yasyf.enabled": .boolean(true),
                    "com.yasyf.role": .string("broker"),
                    "com.yasyf.scopes": .stringArrayContains("mutate"),
                ]
            )
            let info: NSDictionary = [
                kSecCodeInfoStatus: NSNumber(value: runtimeAndLibraryValidation),
                kSecCodeInfoEntitlementsDict: [
                    applicationGroupsKey: ["group.com.yasyf.consumer"],
                    "com.yasyf.enabled": true,
                    "com.yasyf.role": "broker",
                    "com.yasyf.scopes": ["read", "mutate"],
                ] as NSDictionary,
            ]
            try PeerTrust.enforceSignedPeer(info: info, requiredEntitlements: requirement.requiredEntitlements)
        }

        @Test func missingOrWrongAppGroupFailsClosed() throws {
            let requirement = try fixtureRequirement()
            let missing: NSDictionary = [
                kSecCodeInfoStatus: NSNumber(value: runtimeAndLibraryValidation),
                kSecCodeInfoEntitlementsDict: [:] as NSDictionary,
            ]
            #expect(throws: PeerTrust.TrustError.requiredEntitlementMissing(applicationGroupsKey)) {
                try PeerTrust.enforceSignedPeer(info: missing, requiredEntitlements: requirement.requiredEntitlements)
            }
            let wrong: NSDictionary = [
                kSecCodeInfoStatus: NSNumber(value: runtimeAndLibraryValidation),
                kSecCodeInfoEntitlementsDict: [applicationGroupsKey: ["group.other"]] as NSDictionary,
            ]
            #expect(throws: PeerTrust.TrustError.requiredEntitlementMismatch(applicationGroupsKey)) {
                try PeerTrust.enforceSignedPeer(info: wrong, requiredEntitlements: requirement.requiredEntitlements)
            }
        }

        @Test func socketServerRequiresAnExplicitTrustPolicy() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let server = SocketServer(path: path, build: "server-test", trust: .sameEffectiveUser) { request in
                    .terminal(SocketTerminal(payload: request.payload))
                }
                try await server.start()
                cleanup.add { await server.stop() }
                let client = try await SocketClient(path: path, build: "server-test", trust: .sameEffectiveUser)
                cleanup.add { await client.close() }
                let result = try await client.call(operation: "echo", payload: Data(#""hi""#.utf8))
                #expect(result.payload == Data(#""hi""#.utf8))
            }
        }

        @Test func serverClosesConnectionForARejectedSignedPeer() async throws {
            try await withAsyncCleanup { cleanup in
                let directory = try shortSocketDir()
                cleanup.add { try? FileManager.default.removeItem(at: directory) }
                let path = directory.appendingPathComponent("s.sock").path
                let server = try SocketServer(
                    path: path,
                    build: "server-test",
                    trust: PeerTrust(requirement: fixtureRequirement())
                ) { _ in
                    Issue.record("handler must not run for a rejected peer")
                    return .terminal(SocketTerminal())
                }
                try await server.start()
                cleanup.add { await server.stop() }
                await #expect(throws: (any Error).self) {
                    _ = try await SocketClient(path: path, build: "server-test", trust: .sameEffectiveUser)
                }
            }
        }

        @Test(.enabled(if: trustE2EEnabled, "set DAEMONKIT_TRUST_E2E=1 and build signed fixtures")) func signedFixtureWithRequiredAppGroupPasses() throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let peer = try spawnFixture("fixture-devid-a", in: directory)
            defer { peer.close() }
            try PeerTrust(requirement: fixtureRequirement()).check(descriptor: peer.server)
        }

        @Test(.enabled(if: trustE2EEnabled, "set DAEMONKIT_TRUST_E2E=1 and build signed fixtures")) func signedFixtureWithWrongAppGroupFails() throws {
            let directory = try shortSocketDir()
            defer { try? FileManager.default.removeItem(at: directory) }
            let peer = try spawnFixture("fixture-devid-wronggroup", in: directory)
            defer { peer.close() }
            #expect(throws: PeerTrust.TrustError.requiredEntitlementMismatch(applicationGroupsKey)) {
                try PeerTrust(
                    requirement: fixtureRequirement(identifier: "com.yasyf.daemonkit.fixture-wronggroup")
                ).check(descriptor: peer.server)
            }
        }
    }
}
