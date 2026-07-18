@testable import DaemonKit
import Foundation
import Testing

// `op` and `ok` mirror the fixture's frozen wire keys, below the SwiftLint
// identifier_name minimum.
// swiftlint:disable identifier_name
private struct GoldenFields: Codable {
    let version: String?
    let pid: Int?
    let state: String?
    let draining: Bool?
    let busy: Bool?
    let features: [String]?
    let ok: Bool?
}

private struct GoldenCase: Codable {
    let name: String
    let op: String
    let kind: String
    let fields: GoldenFields
    let bytes: String
}

// swiftlint:enable identifier_name

private struct GoldenFile: Codable {
    let version: Int
    let cases: [GoldenCase]
}

private struct MissingField: Error { let name: String }

private func unwrap<T>(_ value: T?, _ name: String) throws -> T {
    guard let value else { throw MissingField(name: name) }
    return value
}

/// Loads the ONE shared cross-language golden fixture, resolved relative to this
/// source file's location — the Go suite reads the same `wire/lifeproto/testdata`
/// file, no copy.
private func loadGolden() throws -> GoldenFile {
    let repoRoot = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent() // DaemonKitTests
        .deletingLastPathComponent() // Tests
        .deletingLastPathComponent() // repo root
    let url = repoRoot.appendingPathComponent("wire/lifeproto/testdata/golden.json")
    return try JSONDecoder().decode(GoldenFile.self, from: Data(contentsOf: url))
}

/// The two encodings under test: the message built from `fields` through the
/// generated initializers, and the message obtained by decoding `bytes` and
/// re-encoding it. Both must equal the case's frozen bytes.
private func encodings(for goldenCase: GoldenCase) throws -> (built: Data, roundTripped: Data) {
    let fields = goldenCase.fields
    let raw = Data(goldenCase.bytes.utf8)
    switch "\(goldenCase.op)/\(goldenCase.kind)" {
    case "health/request":
        return try (HealthRequest().encoded(), HealthRequest.decode(from: raw).encoded())
    case "health/response":
        let message = try HealthResponse(
            version: unwrap(fields.version, "version"),
            pid: unwrap(fields.pid, "pid"),
            state: unwrap(fields.state, "state"),
            draining: unwrap(fields.draining, "draining"),
            busy: unwrap(fields.busy, "busy"),
            features: unwrap(fields.features, "features")
        )
        return try (message.encoded(), HealthResponse.decode(from: raw).encoded())
    case "shutdown/request":
        return try (ShutdownRequest().encoded(), ShutdownRequest.decode(from: raw).encoded())
    case "shutdown/response":
        let message = try ShutdownResponse(ok: unwrap(fields.ok, "ok"))
        return try (message.encoded(), ShutdownResponse.decode(from: raw).encoded())
    case "hello/request":
        return try (HelloRequest().encoded(), HelloRequest.decode(from: raw).encoded())
    case "hello/response":
        let message = try HelloResponse(features: unwrap(fields.features, "features"))
        return try (message.encoded(), HelloResponse.decode(from: raw).encoded())
    case "handoff/request":
        return try (HandoffRequest().encoded(), HandoffRequest.decode(from: raw).encoded())
    case "handoff/response":
        let message = try HandoffResponse(ok: unwrap(fields.ok, "ok"))
        return try (message.encoded(), HandoffResponse.decode(from: raw).encoded())
    default:
        throw MissingField(name: "\(goldenCase.op)/\(goldenCase.kind)")
    }
}

@Suite(.timeLimit(.minutes(1)))
struct LifecycleWireTests {
    @Test func crossGoldenBothDirections() throws {
        let golden = try loadGolden()
        #expect(golden.version == DaemonKit.lifeProtocolVersion)
        for goldenCase in golden.cases {
            let want = goldenCase.bytes
            let (built, roundTripped) = try encodings(for: goldenCase)
            let builtString = try #require(String(data: built, encoding: .utf8))
            let roundTripString = try #require(String(data: roundTripped, encoding: .utf8))
            #expect(builtString == want, "encode \(goldenCase.name)")
            #expect(roundTripString == want, "decode/re-encode \(goldenCase.name)")
            #expect(!builtString.contains("\n"), "embedded newline in \(goldenCase.name)")
        }
    }

    @Test func protocolVersionIsPinned() {
        #expect(DaemonKit.lifeProtocolVersion == 1)
    }

    @Test func envelopeRejectsForeignVersion() {
        let foreign = Data(#"{"v":2,"op":"health"}"#.utf8)
        #expect(throws: LifecycleWireError.self) {
            _ = try Envelope.decode(from: foreign)
        }
    }

    @Test func envelopePeeksOpAndVersion() throws {
        let envelope = try Envelope.decode(from: Data(#"{"v":1,"op":"handoff"}"#.utf8))
        #expect(envelope.v == 1)
        #expect(envelope.op == LifecycleOp.handoff.rawValue)
    }

    @Test(arguments: [
        (LifecycleOp.health, "health"),
        (LifecycleOp.shutdown, "shutdown"),
        (LifecycleOp.hello, "hello"),
        (LifecycleOp.handoff, "handoff"),
    ])
    func lifecycleOpRawValues(lifecycleOp: LifecycleOp, raw: String) {
        #expect(lifecycleOp.rawValue == raw)
    }
}
