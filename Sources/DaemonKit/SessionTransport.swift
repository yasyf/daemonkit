import Darwin
import Foundation

/// Exact protocol version shared by daemonkit's Go and Swift session transports.
public let daemonKitSessionProtocolVersion: UInt16 = 3

/// Default maximum encoded frame body: 4 MiB.
public let daemonKitDefaultMaximumFrameBytes = 4 * 1024 * 1024

/// A v3 session frame kind.
public enum SessionFrameKind: UInt8, Sendable {
    case hello = 1
    case helloAck
    case request
    case response
    case cancel
    case event
    case stream
    case goAway
    case window
}

/// Flags carried by a v3 session frame.
public struct SessionFrameFlags: OptionSet, Sendable {
    public let rawValue: UInt8

    public init(rawValue: UInt8) {
        self.rawValue = rawValue
    }

    /// Marks the final request or response stream payload.
    public static let end = SessionFrameFlags(rawValue: 1)
}

/// One exact-v3 length-prefixed session frame.
public struct SessionFrame: Sendable {
    public var kind: SessionFrameKind
    public var flags: SessionFrameFlags
    public var id: UInt64
    public var sequence: UInt32
    public var deadlineUnixMilliseconds: Int64
    public var operation: String
    public var tenant: String
    public var payload: Data

    public init(
        kind: SessionFrameKind,
        flags: SessionFrameFlags = [],
        id: UInt64 = 0,
        sequence: UInt32 = 0,
        deadlineUnixMilliseconds: Int64 = 0,
        operation: String = "",
        tenant: String = "",
        payload: Data = Data()
    ) {
        self.kind = kind
        self.flags = flags
        self.id = id
        self.sequence = sequence
        self.deadlineUnixMilliseconds = deadlineUnixMilliseconds
        self.operation = operation
        self.tenant = tenant
        self.payload = payload
    }
}

/// Fail-closed v3 codec errors.
public enum SessionTransportError: Error, Equatable, Sendable {
    case truncatedFrame
    case frameTooLarge(actual: Int, maximum: Int)
    case invalidFrame(String)
    case unsupportedProtocolVersion(UInt16)
    case systemCall(operation: String, errno: Int32)
    case handshake(String)
    case duplicateRequestID(UInt64)
    case streamSequence(id: UInt64, got: UInt32, want: UInt32)
    case cancellationDidNotSettle
    case disconnected
}

struct SessionBuildIdentity: Codable, Sendable {
    let protocolVersion: UInt16
    let build: String

    enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol"
        case build
    }
}

struct SessionSequence: Sendable {
    private var next: UInt32
    private var exhausted = false

    init(next: UInt32 = 0) {
        self.next = next
    }

    mutating func take() throws -> UInt32 {
        guard !exhausted else {
            throw SessionTransportError.invalidFrame("stream sequence exhausted")
        }
        let value = next
        if next == .max {
            exhausted = true
        } else {
            next += 1
        }
        return value
    }
}

final class SessionFrameCodec: @unchecked Sendable {
    static let defaultMaximumFrameBytes = daemonKitDefaultMaximumFrameBytes
    private static let headerBytes = 32
    private static let magic = Data("DKS3".utf8)

    private let descriptor: Int32
    private let maximumFrameBytes: Int
    private let writeLock = NSLock()

    init(descriptor: Int32, maximumFrameBytes: Int = defaultMaximumFrameBytes) {
        self.descriptor = descriptor
        self.maximumFrameBytes = maximumFrameBytes
    }

    func read() throws -> SessionFrame {
        let prefix = try readExactly(4)
        let length = Int(prefix.uint32(at: 0))
        guard length <= maximumFrameBytes else {
            throw SessionTransportError.frameTooLarge(actual: length, maximum: maximumFrameBytes)
        }
        guard length >= Self.headerBytes else {
            throw SessionTransportError.invalidFrame("body length \(length)")
        }
        return try Self.decode(readExactly(length))
    }

    func write(_ frame: SessionFrame) throws {
        let body = try Self.encode(frame)
        guard body.count <= maximumFrameBytes else {
            throw SessionTransportError.frameTooLarge(actual: body.count, maximum: maximumFrameBytes)
        }
        var packet = Data()
        packet.appendUInt32(UInt32(body.count))
        packet.append(body)
        writeLock.lock()
        defer { writeLock.unlock() }
        try writeAll(packet)
    }

    static func encode(_ frame: SessionFrame) throws -> Data {
        try validate(frame)
        let operation = Data(frame.operation.utf8)
        let tenant = Data(frame.tenant.utf8)
        guard operation.count <= Int(UInt16.max), tenant.count <= Int(UInt16.max) else {
            throw SessionTransportError.invalidFrame("routing field too long")
        }
        var body = Data()
        body.append(magic)
        body.appendUInt16(daemonKitSessionProtocolVersion)
        body.append(frame.kind.rawValue)
        body.append(frame.flags.rawValue)
        body.appendUInt64(frame.id)
        body.appendUInt32(frame.sequence)
        body.appendUInt64(UInt64(bitPattern: frame.deadlineUnixMilliseconds))
        body.appendUInt16(UInt16(operation.count))
        body.appendUInt16(UInt16(tenant.count))
        body.append(operation)
        body.append(tenant)
        body.append(frame.payload)
        return body
    }

    static func decode(_ body: Data) throws -> SessionFrame {
        guard body.count >= headerBytes else {
            throw SessionTransportError.invalidFrame("short header")
        }
        guard body.prefix(4) == magic else {
            throw SessionTransportError.invalidFrame("magic")
        }
        let version = body.uint16(at: 4)
        guard version == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(version)
        }
        guard let kind = SessionFrameKind(rawValue: body[6]) else {
            throw SessionTransportError.invalidFrame("kind")
        }
        let flags = SessionFrameFlags(rawValue: body[7])
        guard flags.subtracting(.end).isEmpty else {
            throw SessionTransportError.invalidFrame("flags")
        }
        let operationLength = Int(body.uint16(at: 28))
        let tenantLength = Int(body.uint16(at: 30))
        let routingEnd = headerBytes + operationLength + tenantLength
        guard routingEnd <= body.count else {
            throw SessionTransportError.invalidFrame("routing lengths")
        }
        let operationRange = headerBytes ..< headerBytes + operationLength
        let tenantRange = operationRange.upperBound ..< routingEnd
        guard let operation = String(data: body[operationRange], encoding: .utf8),
              let tenant = String(data: body[tenantRange], encoding: .utf8)
        else {
            throw SessionTransportError.invalidFrame("routing UTF-8")
        }
        let frame = SessionFrame(
            kind: kind,
            flags: flags,
            id: body.uint64(at: 8),
            sequence: body.uint32(at: 16),
            deadlineUnixMilliseconds: Int64(bitPattern: body.uint64(at: 20)),
            operation: operation,
            tenant: tenant,
            payload: body.subdata(in: routingEnd ..< body.count)
        )
        try validate(frame)
        return frame
    }

    private static func validate(_ frame: SessionFrame) throws {
        guard frame.flags.subtracting(.end).isEmpty else {
            throw SessionTransportError.invalidFrame("unknown flags")
        }
        switch frame.kind {
        case .hello, .helloAck:
            guard frame.flags == .end, frame.id == 0, frame.sequence == 0,
                  frame.deadlineUnixMilliseconds == 0, frame.operation.isEmpty,
                  frame.tenant.isEmpty, !frame.payload.isEmpty
            else { throw SessionTransportError.invalidFrame("handshake frame") }
        case .request:
            guard frame.id != 0, frame.sequence == 0, frame.deadlineUnixMilliseconds >= 0,
                  !frame.operation.isEmpty
            else { throw SessionTransportError.invalidFrame("request frame") }
        case .response:
            guard frame.flags == .end, frame.id != 0, frame.sequence == 0,
                  frame.deadlineUnixMilliseconds == 0, frame.operation.isEmpty,
                  frame.tenant.isEmpty, !frame.payload.isEmpty
            else { throw SessionTransportError.invalidFrame("response frame") }
        case .cancel:
            guard frame.flags == .end, frame.id != 0, frame.sequence == 0,
                  frame.deadlineUnixMilliseconds == 0, frame.operation.isEmpty,
                  frame.tenant.isEmpty, frame.payload.isEmpty
            else { throw SessionTransportError.invalidFrame("cancel frame") }
        case .event:
            guard frame.flags == .end, frame.id == 0, frame.sequence == 0,
                  frame.deadlineUnixMilliseconds == 0, !frame.operation.isEmpty,
                  frame.tenant.isEmpty
            else { throw SessionTransportError.invalidFrame("event frame") }
        case .stream:
            guard frame.id != 0, frame.deadlineUnixMilliseconds == 0,
                  frame.operation.isEmpty, frame.tenant.isEmpty
            else { throw SessionTransportError.invalidFrame("stream frame") }
        case .goAway:
            guard frame.flags == .end, frame.id == 0, frame.sequence == 0,
                  frame.deadlineUnixMilliseconds == 0, frame.operation.isEmpty,
                  frame.tenant.isEmpty, frame.payload.isEmpty
            else { throw SessionTransportError.invalidFrame("go-away frame") }
        case .window:
            guard frame.flags.isEmpty, frame.sequence > 0, frame.deadlineUnixMilliseconds == 0,
                  frame.operation.isEmpty, frame.tenant.isEmpty, frame.payload.isEmpty
            else { throw SessionTransportError.invalidFrame("window frame") }
        }
    }

    private func readExactly(_ count: Int) throws -> Data {
        var data = Data(count: count)
        var offset = 0
        while offset < count {
            let readCount = data.withUnsafeMutableBytes { buffer in
                Darwin.read(descriptor, buffer.baseAddress?.advanced(by: offset), count - offset)
            }
            if readCount == 0 {
                if offset == 0 {
                    throw SessionTransportError.disconnected
                }
                throw SessionTransportError.truncatedFrame
            }
            if readCount < 0 {
                if errno == EINTR {
                    continue
                }
                throw SessionTransportError.systemCall(operation: "read", errno: errno)
            }
            offset += readCount
        }
        return data
    }

    private func writeAll(_ data: Data) throws {
        var offset = 0
        while offset < data.count {
            let written = data.withUnsafeBytes { buffer in
                Darwin.write(descriptor, buffer.baseAddress?.advanced(by: offset), data.count - offset)
            }
            if written < 0 {
                if errno == EINTR {
                    continue
                }
                throw SessionTransportError.systemCall(operation: "write", errno: errno)
            }
            if written == 0 {
                throw SessionTransportError.truncatedFrame
            }
            offset += written
        }
    }
}

private extension Data {
    mutating func appendUInt16(_ value: UInt16) {
        var bigEndian = value.bigEndian
        Swift.withUnsafeBytes(of: &bigEndian) { append(contentsOf: $0) }
    }

    mutating func appendUInt32(_ value: UInt32) {
        var bigEndian = value.bigEndian
        Swift.withUnsafeBytes(of: &bigEndian) { append(contentsOf: $0) }
    }

    mutating func appendUInt64(_ value: UInt64) {
        var bigEndian = value.bigEndian
        Swift.withUnsafeBytes(of: &bigEndian) { append(contentsOf: $0) }
    }

    func uint16(at offset: Int) -> UInt16 {
        withUnsafeBytes { UInt16(bigEndian: $0.loadUnaligned(fromByteOffset: offset, as: UInt16.self)) }
    }

    func uint32(at offset: Int) -> UInt32 {
        withUnsafeBytes { UInt32(bigEndian: $0.loadUnaligned(fromByteOffset: offset, as: UInt32.self)) }
    }

    func uint64(at offset: Int) -> UInt64 {
        withUnsafeBytes { UInt64(bigEndian: $0.loadUnaligned(fromByteOffset: offset, as: UInt64.self)) }
    }
}
