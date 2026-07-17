import Foundation

/// A canonical lifecycle operation carried in a ``WireEnvelope``'s `op` field.
///
/// The `op` on the wire is a free-form `String` so consumers may add their own
/// operations; this enum names the four operations every daemonkit peer speaks.
public enum LifecycleOp: String, Sendable, CaseIterable {
    case health
    case shutdown
    case hello
    case handoff
}

/// The daemonkit-native lifecycle envelope: a versioned wrapper around a
/// consumer-defined `Payload`.
///
/// ## Framing
///
/// Envelopes are exchanged as **one JSON object per line**: each frame is a
/// single UTF-8 JSON object terminated by a `\n` (`0x0A`). ``encoded()`` returns
/// the object bytes *without* the trailing newline — the transport (see
/// ``SocketServer``) appends the framing `\n`. A JSON object never contains a
/// literal newline (string values escape it as `\n`), so the line framing is
/// unambiguous.
///
/// ## Key order
///
/// The encoder is pinned deterministic (`.sortedKeys` + `.withoutEscapingSlashes`)
/// and the keys are fixed by explicit ``CodingKeys``, so the encoded bytes are
/// stable across runs and platforms — golden-pinned in the test suite.
public struct WireEnvelope<Payload: Codable & Sendable>: Codable, Sendable {
    // `v` and `op` are frozen wire field names (see the golden-bytes test), so
    // the SwiftLint identifier_name minimum does not apply here.
    // swiftlint:disable identifier_name

    /// Protocol version; always ``DaemonKit/lifeProtocolVersion`` on send.
    public let v: Int
    /// The lifecycle operation, e.g. `"health"` (see ``LifecycleOp``).
    public let op: String
    /// The consumer-defined body of the frame.
    public let payload: Payload

    /// Wire keys, pinned so a property rename can never shift the on-wire names.
    public enum CodingKeys: String, CodingKey {
        case v
        case op
        case payload
    }

    /// Wraps `payload` under a free-form operation string, pinning `v` to the
    /// current protocol version.
    public init(op: String, payload: Payload) {
        v = DaemonKit.lifeProtocolVersion
        self.op = op
        self.payload = payload
    }

    /// Wraps `payload` under a canonical ``LifecycleOp``.
    public init(op: LifecycleOp, payload: Payload) {
        self.init(op: op.rawValue, payload: payload)
    }

    // swiftlint:enable identifier_name

    /// The single-line JSON encoding of this envelope, without the framing `\n`.
    public func encoded() throws -> Data {
        try Self.makeEncoder().encode(self)
    }

    /// Decodes an envelope from a single JSON frame (newline already stripped).
    public static func decode(from data: Data) throws -> WireEnvelope<Payload> {
        try JSONDecoder().decode(WireEnvelope<Payload>.self, from: data)
    }

    private static func makeEncoder() -> JSONEncoder {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
        return encoder
    }
}
