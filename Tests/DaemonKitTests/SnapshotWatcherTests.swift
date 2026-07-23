@testable import DaemonKit
import Darwin
import Foundation
import Testing

private let snapshotFingerprint = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
private let snapshotSchema = try! SnapshotSchema(
    identity: "test.snapshot.v1",
    fingerprint: snapshotFingerprint
)

private func snapshotCodec<S: Decodable & Sendable>(_: S.Type) -> SnapshotCodec<S> {
    SnapshotCodec(schema: snapshotSchema) { data, decoder in
        try decoder.decode(S.self, from: data)
    }
}

private struct Snap: Decodable, Sendable, Equatable {
    let schemaVersion: Int
    let value: Int
    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case value
    }
}

private struct Dated: Decodable, Sendable, Equatable {
    let schemaVersion: Int
    let updatedAt: Date
    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case updatedAt = "updated_at"
    }
}

private final class StateBox<S: Sendable>: @unchecked Sendable {
    private let lock = NSLock()
    private var states: [SnapshotState<S>] = []
    func append(_ state: SnapshotState<S>) {
        lock.lock(); states.append(state); lock.unlock()
    }

    var count: Int {
        lock.lock(); defer { lock.unlock() }; return states.count
    }

    var all: [SnapshotState<S>] {
        lock.lock(); defer { lock.unlock() }; return states
    }
}

private struct WaitTimeout: Error {}

private func waitUntil(_ seconds: Double = 3, _ condition: @Sendable () -> Bool) async throws {
    let clock = ContinuousClock()
    let deadline = clock.now + .seconds(seconds)
    while clock.now < deadline {
        if condition() {
            return
        }
        try await clock.sleep(for: .milliseconds(20))
    }
    guard condition() else { throw WaitTimeout() }
}

@Suite(.timeLimit(.minutes(1)))
struct SnapshotWatcherTests {
    @Test func decodesLoadedSnapshot() {
        let data = Data(#"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value":42}"#.utf8)
        let state = SnapshotWatcher<Snap>.decodedState(
            from: data,
            codec: snapshotCodec(Snap.self),
            decoder: SnapshotWatcher<Snap>.makeDecoder()
        )
        guard case let .loaded(snap) = state else {
            Issue.record("expected .loaded, got \(state)")
            return
        }
        #expect(snap == Snap(schemaVersion: 1, value: 42))
    }

    @Test func schemaSkewSurfacesExpectedAndFound() {
        let data = Data(#"{"identity":"test.snapshot.v1","schema_version":2,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value":42}"#.utf8)
        let state = SnapshotWatcher<Snap>.decodedState(
            from: data,
            codec: snapshotCodec(Snap.self),
            decoder: SnapshotWatcher<Snap>.makeDecoder()
        )
        guard case let .schemaSkew(expected, foundIdentity, foundVersion, foundFingerprint) = state else {
            Issue.record("expected .schemaSkew, got \(state)")
            return
        }
        #expect(expected == snapshotSchema)
        #expect(foundIdentity == snapshotSchema.identity)
        #expect(foundVersion == 2)
        #expect(foundFingerprint == snapshotSchema.fingerprint)
    }

    @Test(arguments: [
        #"not json at all"#,
        #"{"schema_version":1}"#,
        #"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"short","value":42}"#,
    ])
    func malformedSnapshotSurfacesMalformed(json: String) {
        let state = SnapshotWatcher<Snap>.decodedState(
            from: Data(json.utf8),
            codec: snapshotCodec(Snap.self),
            decoder: SnapshotWatcher<Snap>.makeDecoder()
        )
        guard case .malformed = state else {
            Issue.record("expected .malformed, got \(state)")
            return
        }
    }

    @Test func missingFileSurfacesMissing() {
        let url = FileManager.default.temporaryDirectory.appendingPathComponent("dk-missing-\(UUID()).json")
        let watcher = SnapshotWatcher<Snap>(
            fileURL: url,
            codec: snapshotCodec(Snap.self),
            callbackQueue: .global(),
            onChange: { _ in }
        )
        guard case .missing = watcher.computeState() else {
            Issue.record("expected .missing")
            return
        }
    }

    @Test func decodesBothFractionalAndPlainISO8601() throws {
        let decoder = SnapshotWatcher<Dated>.makeDecoder()

        let fractionalState = SnapshotWatcher<Dated>.decodedState(
            from: Data(#"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","updated_at":"2026-07-17T12:00:00.500Z"}"#.utf8),
            codec: snapshotCodec(Dated.self),
            decoder: decoder
        )
        let fractionalFormatter = ISO8601DateFormatter()
        fractionalFormatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let expectedFractional = try #require(fractionalFormatter.date(from: "2026-07-17T12:00:00.500Z"))
        guard case let .loaded(fractional) = fractionalState else {
            Issue.record("expected .loaded, got \(fractionalState)")
            return
        }
        #expect(fractional.updatedAt == expectedFractional)

        let plainState = SnapshotWatcher<Dated>.decodedState(
            from: Data(#"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","updated_at":"2026-07-17T12:00:00Z"}"#.utf8),
            codec: snapshotCodec(Dated.self),
            decoder: decoder
        )
        let plainFormatter = ISO8601DateFormatter()
        plainFormatter.formatOptions = [.withInternetDateTime]
        let expectedPlain = try #require(plainFormatter.date(from: "2026-07-17T12:00:00Z"))
        guard case let .loaded(plain) = plainState else {
            Issue.record("expected .loaded, got \(plainState)")
            return
        }
        #expect(plain.updatedAt == expectedPlain)
    }

    @Test func watchesDirectoryAcrossAtomicRename() async throws {
        let dir = FileManager.default.temporaryDirectory.appendingPathComponent("dkw-\(UUID())")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let file = dir.appendingPathComponent("snap.json")
        try Data(#"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value":1}"#.utf8).write(to: file)

        let box = StateBox<Snap>()
        let watcher = SnapshotWatcher<Snap>(
            fileURL: file,
            codec: snapshotCodec(Snap.self),
            debounce: 0.05,
            callbackQueue: .global(),
            onChange: { box.append($0) }
        )
        try watcher.start()
        defer { watcher.stop() }

        try await waitUntil { box.count >= 1 }
        guard case let .loaded(first) = try #require(box.all.first) else {
            Issue.record("expected initial .loaded, got \(box.all)")
            return
        }
        #expect(first.value == 1)

        let tmp = dir.appendingPathComponent(".tmp-\(UUID())")
        try Data(#"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value":2}"#.utf8).write(to: tmp)
        #expect(rename(tmp.path, file.path) == 0)

        try await waitUntil { box.count >= 2 }
        guard case let .loaded(latest) = try #require(box.all.last) else {
            Issue.record("expected updated .loaded, got \(box.all)")
            return
        }
        #expect(latest.value == 2)
    }

    @Test func droppingAStartedWatcherReleasesItsDirectoryFD() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("dk-snap-\(getpid())-\(UUID().uuidString.prefix(8))")
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: dir) }
        let file = dir.appendingPathComponent("snapshot.json")
        try Data(#"{"identity":"test.snapshot.v1","schema_version":1,"fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","value":1}"#.utf8).write(to: file)

        func probeFD() -> Int32 {
            let probe = open(dir.path, O_EVTONLY)
            if probe >= 0 {
                close(probe)
            }
            return probe
        }

        let baseline = probeFD()
        for _ in 0 ..< 64 {
            let watcher = SnapshotWatcher<Snap>(
                fileURL: file,
                codec: snapshotCodec(Snap.self),
                callbackQueue: .global(),
                onChange: { _ in }
            )
            try watcher.start()
            _ = watcher
        }
        let after = probeFD()
        #expect(after >= 0)
        #expect(after - baseline < 32, "directory fd climbed from \(baseline) to \(after) — watcher fds leaked")
    }
}
