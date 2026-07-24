import Foundation

/// OwnerGeneration is one exact process-owner generation.
public struct OwnerGeneration: Codable, CustomStringConvertible, Equatable, Hashable, Sendable {
    public let value: String

    public init(_ value: String) throws {
        let bytes = Array(value.utf8)
        guard bytes.count == 32,
              bytes.allSatisfy({ (48 ... 57).contains($0) || (97 ... 102).contains($0) }),
              bytes.contains(where: { $0 != 48 })
        else {
            throw OwnerGenerationValidationError()
        }
        self.value = value
    }

    public var description: String {
        value
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        let value = try container.decode(String.self)
        do {
            try self.init(value)
        } catch {
            throw DecodingError.dataCorruptedError(
                in: container,
                debugDescription: "owner generation must be 32 lowercase nonzero hexadecimal bytes"
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        try container.encode(value)
    }
}

/// OwnerGenerationValidationError reports a noncanonical process-owner generation.
public struct OwnerGenerationValidationError: Error, Equatable, Sendable {}
