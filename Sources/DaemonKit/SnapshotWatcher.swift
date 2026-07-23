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

/// The distinct states a ``SnapshotWatcher`` surfaces to its consumer.
public enum SnapshotState<S: Sendable>: Sendable {
    /// The file decoded cleanly at the expected schema version.
    case loaded(S)
    /// The file does not exist.
    case missing
    /// The file exists but could not be read or decoded.
    case malformed(SnapshotDecodeError)
    /// The file's exact schema identity does not match the caller's codec.
    case schemaSkew(
        expected: SnapshotSchema,
        foundIdentity: String,
        foundVersion: Int,
        foundFingerprint: String
    )
}

/// SnapshotSchema is one caller-owned exact v1 snapshot identity.
public struct SnapshotSchema: Equatable, Sendable {
    public let identity: String
    public let version = 1
    public let fingerprint: String

    public init(identity: String, fingerprint: String) throws {
        guard !identity.isEmpty else { throw SnapshotSchemaError.invalidIdentity }
        let hex = CharacterSet(charactersIn: "0123456789abcdef")
        guard fingerprint.count == 64,
              fingerprint.unicodeScalars.allSatisfy(hex.contains)
        else { throw SnapshotSchemaError.invalidFingerprint }
        self.identity = identity
        self.fingerprint = fingerprint
    }
}

/// SnapshotSchemaError rejects an incomplete caller-owned schema.
public enum SnapshotSchemaError: Error, Sendable {
    case invalidIdentity
    case invalidFingerprint
}

/// SnapshotCodec supplies both exact schema identity and caller-owned decoding.
public struct SnapshotCodec<S: Sendable>: Sendable {
    public let schema: SnapshotSchema
    let decode: @Sendable (Data, JSONDecoder) throws -> S

    public init(
        schema: SnapshotSchema,
        decode: @escaping @Sendable (Data, JSONDecoder) throws -> S
    ) {
        self.schema = schema
        self.decode = decode
    }
}

/// Errors thrown while starting a ``SnapshotWatcher``.
public enum SnapshotWatcherError: Error, Sendable {
    /// The parent directory could not be opened for event monitoring.
    case cannotOpenDirectory(path: String, errno: Int32)
}

/// Watches a JSON snapshot file and delivers a typed ``SnapshotState`` on
/// each change. The watch is on the **parent directory**: consumers publish
/// via atomic rename-into-place, which replaces the inode and silently kills
/// a file-level (`vnode`) watch.
public final class SnapshotWatcher<S: Sendable>: @unchecked Sendable {
    private let fileURL: URL
    private let directoryURL: URL
    private let codec: SnapshotCodec<S>
    private let debounce: TimeInterval
    private let callbackQueue: DispatchQueue
    private let onChange: @Sendable (SnapshotState<S>) -> Void
    private let internalQueue = DispatchQueue(label: "com.yasyf.daemonkit.SnapshotWatcher")
    private let decoder: JSONDecoder
    private let lock = NSLock()
    private var source: DispatchSourceFileSystemObject?
    private var pending: DispatchWorkItem?

    public init(
        fileURL: URL,
        codec: SnapshotCodec<S>,
        debounce: TimeInterval = 0.2,
        callbackQueue: DispatchQueue,
        onChange: @escaping @Sendable (SnapshotState<S>) -> Void
    ) {
        self.fileURL = fileURL
        directoryURL = fileURL.deletingLastPathComponent()
        self.codec = codec
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
        src.setRegistrationHandler { [weak self] in self?.deliverCurrentState() }
        src.setCancelHandler { close(dirFD) }
        lock.lock()
        source = src
        lock.unlock()
        src.resume()
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
        return Self.decodedState(from: data, codec: codec, decoder: decoder)
    }

    static func decodedState(
        from data: Data,
        codec: SnapshotCodec<S>,
        decoder: JSONDecoder
    ) -> SnapshotState<S> {
        let probe: SnapshotSchemaProbe
        do {
            probe = try decoder.decode(SnapshotSchemaProbe.self, from: data)
        } catch {
            return .malformed(SnapshotDecodeError(error))
        }
        let found: SnapshotSchema
        do {
            found = try SnapshotSchema(identity: probe.identity, fingerprint: probe.fingerprint)
        } catch {
            return .malformed(SnapshotDecodeError(error))
        }
        guard found == codec.schema, probe.schemaVersion == codec.schema.version else {
            return .schemaSkew(
                expected: codec.schema,
                foundIdentity: probe.identity,
                foundVersion: probe.schemaVersion,
                foundFingerprint: probe.fingerprint
            )
        }
        do {
            return try .loaded(codec.decode(data, decoder))
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
    let identity: String
    let schemaVersion: Int
    let fingerprint: String
    enum CodingKeys: String, CodingKey {
        case identity
        case schemaVersion = "schema_version"
        case fingerprint
    }
}

/// Used only from the serial decode path, so the @unchecked Sendable is sound.
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
