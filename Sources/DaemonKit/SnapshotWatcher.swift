import Darwin
import Foundation

/// A Sendable carrier for a decode/read failure, so ``SnapshotState`` can cross
/// queues (a bare `any Error` is not `Sendable`).
public struct SnapshotDecodeError: Error, Sendable, CustomStringConvertible {
    public let description: String

    public init(_ error: any Error) {
        description = String(describing: error)
    }
}

/// The distinct states a ``SnapshotWatcher`` surfaces to its consumer. A bad
/// file never crashes and never silently yields stale data — every failure mode
/// is its own case the consumer switches on.
public enum SnapshotState<S: Sendable>: Sendable {
    /// The file decoded cleanly at the expected schema version.
    case loaded(S)
    /// The file does not exist.
    case missing
    /// The file exists but could not be read or decoded.
    case malformed(SnapshotDecodeError)
    /// The file's `schema_version` does not match the expected version.
    case versionSkew(expected: Int, found: Int)
}

/// Errors thrown while starting a ``SnapshotWatcher``.
public enum SnapshotWatcherError: Error, Sendable {
    /// The parent directory could not be opened for event monitoring.
    case cannotOpenDirectory(path: String, errno: Int32)
}

/// Watches a JSON snapshot file for changes and delivers a typed
/// ``SnapshotState`` on each change.
///
/// The watch is placed on the **parent directory**, not the file: consumers
/// publish snapshots with an atomic rename-into-place, which replaces the inode
/// and so silently kills any file-level (`vnode`) watch. Watching the directory
/// survives the swap.
///
/// Reloads are debounced (default ~200 ms) to collapse the burst of events a
/// single write emits. Decoding tries ISO8601 **with** fractional seconds first,
/// then plain ISO8601. Callbacks are delivered on the caller-provided queue.
public final class SnapshotWatcher<S: Decodable & Sendable>: @unchecked Sendable {
    private let fileURL: URL
    private let directoryURL: URL
    private let expectedSchemaVersion: Int
    private let debounce: TimeInterval
    private let callbackQueue: DispatchQueue
    private let onChange: @Sendable (SnapshotState<S>) -> Void
    private let internalQueue = DispatchQueue(label: "com.yasyf.daemonkit.SnapshotWatcher")
    private let decoder: JSONDecoder
    private let lock = NSLock()
    private var source: DispatchSourceFileSystemObject?
    private var pending: DispatchWorkItem?

    /// - Parameters:
    ///   - fileURL: The snapshot file to watch.
    ///   - expectedSchemaVersion: The `schema_version` the consumer accepts.
    ///   - debounce: Delay collapsing an event burst into one reload.
    ///   - callbackQueue: Queue the `onChange` callback runs on.
    ///   - onChange: Called with each new ``SnapshotState`` (including the
    ///     initial state on ``start()``).
    public init(
        fileURL: URL,
        expectedSchemaVersion: Int,
        debounce: TimeInterval = 0.2,
        callbackQueue: DispatchQueue,
        onChange: @escaping @Sendable (SnapshotState<S>) -> Void
    ) {
        self.fileURL = fileURL
        directoryURL = fileURL.deletingLastPathComponent()
        self.expectedSchemaVersion = expectedSchemaVersion
        self.debounce = debounce
        self.callbackQueue = callbackQueue
        self.onChange = onChange
        decoder = Self.makeDecoder()
    }

    /// Begins watching and delivers the current state once immediately.
    public func start() throws {
        let dirFD = open(directoryURL.path, O_EVTONLY)
        guard dirFD >= 0 else {
            throw SnapshotWatcherError.cannotOpenDirectory(path: directoryURL.path, errno: errno)
        }
        let src = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: dirFD,
            eventMask: [.write, .delete, .rename, .link, .revoke],
            queue: internalQueue
        )
        src.setEventHandler { [weak self] in self?.scheduleReload() }
        src.setCancelHandler { close(dirFD) }
        lock.lock()
        source = src
        lock.unlock()
        src.resume()
        internalQueue.async { [weak self] in self?.deliverCurrentState() }
    }

    /// Stops watching. Safe to call more than once.
    public func stop() {
        lock.lock()
        let src = source
        source = nil
        lock.unlock()
        src?.cancel()
    }

    deinit { stop() }

    private func scheduleReload() {
        pending?.cancel()
        let item = DispatchWorkItem { [weak self] in self?.deliverCurrentState() }
        pending = item
        internalQueue.asyncAfter(deadline: .now() + debounce, execute: item)
    }

    private func deliverCurrentState() {
        let state = computeState()
        callbackQueue.async { [onChange] in onChange(state) }
    }

    func computeState() -> SnapshotState<S> {
        guard FileManager.default.fileExists(atPath: fileURL.path) else { return .missing }
        let data: Data
        do {
            data = try Data(contentsOf: fileURL)
        } catch {
            return .malformed(SnapshotDecodeError(error))
        }
        return Self.decodedState(from: data, expectedSchemaVersion: expectedSchemaVersion, decoder: decoder)
    }

    static func decodedState(
        from data: Data,
        expectedSchemaVersion: Int,
        decoder: JSONDecoder
    ) -> SnapshotState<S> {
        let probe: SnapshotSchemaProbe
        do {
            probe = try decoder.decode(SnapshotSchemaProbe.self, from: data)
        } catch {
            return .malformed(SnapshotDecodeError(error))
        }
        guard probe.schemaVersion == expectedSchemaVersion else {
            return .versionSkew(expected: expectedSchemaVersion, found: probe.schemaVersion)
        }
        do {
            return try .loaded(decoder.decode(S.self, from: data))
        } catch {
            return .malformed(SnapshotDecodeError(error))
        }
    }

    static func makeDecoder() -> JSONDecoder {
        let decoder = JSONDecoder()
        let parser = ISO8601Parser()
        decoder.dateDecodingStrategy = .custom { decoder in
            let container = try decoder.singleValueContainer()
            let raw = try container.decode(String.self)
            guard let date = parser.date(from: raw) else {
                throw DecodingError.dataCorrupted(
                    .init(codingPath: container.codingPath, debugDescription: "unrecognized ISO8601 date: \(raw)")
                )
            }
            return date
        }
        return decoder
    }
}

private struct SnapshotSchemaProbe: Decodable {
    let schemaVersion: Int
    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
    }
}

/// Parses ISO8601 dates, preferring the fractional-seconds form and falling back
/// to the plain form. The `ISO8601DateFormatter` instances are reference types
/// (not `Sendable`); this wrapper is used only from the serial decode path, so
/// the unchecked conformance is sound.
private struct ISO8601Parser: @unchecked Sendable {
    private let fractional: ISO8601DateFormatter
    private let plain: ISO8601DateFormatter

    init() {
        fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        plain = ISO8601DateFormatter()
        plain.formatOptions = [.withInternetDateTime]
    }

    func date(from raw: String) -> Date? {
        fractional.date(from: raw) ?? plain.date(from: raw)
    }
}
