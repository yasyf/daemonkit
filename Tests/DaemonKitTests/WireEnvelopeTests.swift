@testable import DaemonKit
import Foundation
import Testing

private struct HealthPayload: Codable, Sendable, Equatable {
    let status: String
    let pid: Int
}

@Suite(.timeLimit(.minutes(1)))
struct WireEnvelopeTests {
    @Test func wireEnvelopeEncodesGoldenBytes() throws {
        let envelope = WireEnvelope(op: .health, payload: HealthPayload(status: "ok", pid: 4321))
        let data = try envelope.encoded()
        let json = try #require(String(data: data, encoding: .utf8))
        #expect(json == #"{"op":"health","payload":{"pid":4321,"status":"ok"},"v":1}"#)
        #expect(!json.contains("\n"))
    }

    @Test func wireEnvelopePinsProtocolVersion() {
        let envelope = WireEnvelope(op: .hello, payload: HealthPayload(status: "ok", pid: 1))
        #expect(envelope.v == DaemonKit.lifeProtocolVersion)
        #expect(envelope.v == 1)
    }

    @Test func wireEnvelopeRoundTrips() throws {
        let original = WireEnvelope(op: .handoff, payload: HealthPayload(status: "draining", pid: 7))
        let decoded = try WireEnvelope<HealthPayload>.decode(from: original.encoded())
        #expect(decoded.v == 1)
        #expect(decoded.op == "handoff")
        #expect(decoded.payload == original.payload)
    }

    @Test(arguments: [
        (LifecycleOp.health, "health"),
        (LifecycleOp.shutdown, "shutdown"),
        (LifecycleOp.hello, "hello"),
        (LifecycleOp.handoff, "handoff"),
    ])
    func lifecycleOpRawValues(lifecycleOp: LifecycleOp, raw: String) {
        #expect(lifecycleOp.rawValue == raw)
        #expect(WireEnvelope(op: lifecycleOp, payload: HealthPayload(status: "x", pid: 0)).op == raw)
    }
}
