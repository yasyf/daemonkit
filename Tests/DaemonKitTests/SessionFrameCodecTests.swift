@testable import DaemonKit
import Darwin
import Foundation
import Testing

@Suite(.serialized)
struct SessionFrameCodecTests {
    private static func repositoryRoot() -> URL {
        URL(fileURLWithPath: #filePath)
            .deletingLastPathComponent()
            .deletingLastPathComponent()
            .deletingLastPathComponent()
    }

    private static func goldenHex(_ fixture: String) throws -> String {
        let object = try JSONSerialization.jsonObject(
            with: Data(contentsOf: repositoryRoot().appendingPathComponent(fixture))
        ) as? [String: String]
        return try #require(object?["hex"])
    }

    private static func goldenPacket() throws -> Data {
        let body = try SessionFrameCodec.encode(SessionFrame(
            kind: .request,
            flags: .end,
            id: 42,
            sequence: 0,
            deadlineUnixMilliseconds: 1_700_000_000_123,
            operation: "mutate",
            tenant: "acct-18",
            payload: Data(#"{"value":1}"#.utf8)
        ))
        var encoded = Data()
        var length = UInt32(body.count).bigEndian
        withUnsafeBytes(of: &length) { encoded.append(contentsOf: $0) }
        encoded.append(body)
        return encoded
    }

    @Test func frameV2MatchesSharedGoGolden() throws {
        let encoded = try Self.goldenPacket()
        let hex = try Self.goldenHex("internal/wire/testdata/frame-v2.json")
        #expect(encoded.map { String(format: "%02x", $0) }.joined() == hex)
    }

    @Test func frameV1ShapeSurvivesTheVersionBump() throws {
        var encoded = try Self.goldenPacket()
        encoded[8] = 0
        encoded[9] = 1
        let hex = try Self.goldenHex("wire/testdata/frame-v1.json")
        #expect(encoded.map { String(format: "%02x", $0) }.joined() == hex)
    }

    @Test func encodedBodyOpensWithTheFrozenCutPrefix() throws {
        let frozen = try String(
            contentsOf: Self.repositoryRoot()
                .appendingPathComponent("ci/mixedera/testdata/frozen/frame-prefix-cut.hex"),
            encoding: .utf8
        ).trimmingCharacters(in: .whitespacesAndNewlines)
        let body = try Self.goldenPacket().dropFirst(4)
        #expect(body.prefix(6).map { String(format: "%02x", $0) }.joined() == frozen)
    }

    @Test func deadlineSleepNanosecondsSaturatesInsteadOfTrapping() {
        #expect(deadlineSleepNanoseconds(until: .distantFuture) == .max)
        #expect(deadlineSleepNanoseconds(until: Date(timeIntervalSinceNow: -1)) == 0)
        #expect(deadlineSleepNanoseconds(until: Date(timeIntervalSinceNow: 5)) > 0)
    }

    @Test func drainPreamblePeekReportsDrainingServer() throws {
        var fds = [Int32](repeating: 0, count: 2)
        #expect(socketpair(AF_UNIX, SOCK_STREAM, 0, &fds) == 0)
        defer { close(fds[0]); close(fds[1]) }
        _ = SessionFrameCodec.drainPreamble.withUnsafeBytes {
            write(fds[1], $0.baseAddress, $0.count)
        }
        let codec = SessionFrameCodec(descriptor: fds[0])
        let draining = try codec.peekPreamble()
        #expect(draining)
    }

    @Test func drainPreamblePeekStashesNonPreambleFrameBytes() throws {
        var fds = [Int32](repeating: 0, count: 2)
        #expect(socketpair(AF_UNIX, SOCK_STREAM, 0, &fds) == 0)
        defer { close(fds[0]); close(fds[1]) }
        let body = try SessionFrameCodec.encode(SessionFrame(
            kind: .response,
            flags: .end,
            id: 7,
            payload: Data(#"{"ack":false}"#.utf8)
        ))
        var packet = Data()
        var length = UInt32(body.count).bigEndian
        withUnsafeBytes(of: &length) { packet.append(contentsOf: $0) }
        packet.append(body)
        _ = packet.withUnsafeBytes { write(fds[1], $0.baseAddress, $0.count) }
        let codec = SessionFrameCodec(descriptor: fds[0])
        let draining = try codec.peekPreamble()
        #expect(!draining)
        let frame = try codec.read()
        #expect(frame.kind == .response)
        #expect(frame.id == 7)
    }
}
