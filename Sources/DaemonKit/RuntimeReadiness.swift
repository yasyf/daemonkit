import Dispatch
import Foundation

/// Maximum opaque product detail carried by one readiness snapshot.
public let daemonKitMaxReadinessDetailBytes = 4096
let runtimeReadinessSubscribeOperation = "daemon.control.readiness.subscribe"
let runtimeReceiptOperation = "daemon.control.runtime.receipt"

/// RuntimeIdentity identifies one exact product runtime process.
public struct RuntimeIdentity: Codable, Equatable, Sendable {
    public let runtimeBuild: String
    public let processGeneration: OwnerGeneration

    public init(runtimeBuild: String, processGeneration: OwnerGeneration) {
        self.runtimeBuild = runtimeBuild
        self.processGeneration = processGeneration
    }

    enum CodingKeys: String, CodingKey {
        case runtimeBuild = "runtime_build"
        case processGeneration = "process_generation"
    }
}

/// RuntimeProcessReceipt authenticates the exact process behind one runtime-bound session.
public struct RuntimeProcessReceipt: Equatable, Sendable {
    public let runtimeIdentity: RuntimeIdentity
}

/// RuntimeReceiptUnavailableError reports a session without a runtime receipt route.
public struct RuntimeReceiptUnavailableError: Error, Equatable, Sendable {}

/// RuntimeReadinessState is one exact lifecycle publication state.
public enum RuntimeReadinessState: String, Codable, Sendable {
    case starting = "runtime_starting"
    case ready = "runtime_ready"
    case failed = "runtime_failed"
    case draining = "runtime_draining"
}

/// ReadinessProgress is daemonkit's immutable lifecycle progress snapshot.
public struct ReadinessProgress: Codable, Equatable, Sendable {
    public let sequence: UInt64
    public let state: RuntimeReadinessState
    public let detail: Data

    public init(sequence: UInt64, state: RuntimeReadinessState, detail: Data) {
        self.sequence = sequence
        self.state = state
        self.detail = detail
    }
}

/// RuntimeLifecycleSnapshot is one retained identity and progress publication.
public struct RuntimeLifecycleSnapshot: Equatable, Sendable {
    public let identity: RuntimeIdentity
    public let progress: ReadinessProgress

    public init(identity: RuntimeIdentity, progress: ReadinessProgress) {
        self.identity = identity
        self.progress = progress
    }
}

/// ReadinessNoProgressError reports the last immutable lifecycle snapshot.
public struct ReadinessNoProgressError: Error, Equatable, Sendable {
    public let snapshot: RuntimeLifecycleSnapshot?
}

/// RuntimeFailedError reports the terminal immutable lifecycle snapshot.
public struct RuntimeFailedError: Error, Equatable, Sendable {
    public let snapshot: RuntimeLifecycleSnapshot
}

/// RuntimeReadinessValidationError is an exact lifecycle contract violation.
public enum RuntimeReadinessValidationError: Error, Equatable, Sendable {
    case invalidResponse(String)
    case runtimeIdentity(got: RuntimeIdentity, want: RuntimeIdentity)
    case sequenceRegression(got: UInt64, previous: UInt64)
    case sequenceMutation(UInt64)
    case draining(RuntimeLifecycleSnapshot)
}

private struct RuntimeReadinessSubscribeMessage: Codable, Sendable {
    let protocolVersion: UInt16

    enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol"
    }
}

private struct RuntimeReceiptResponse: Codable, Sendable {
    let protocolVersion: UInt16
    let runtimeIdentity: RuntimeIdentity

    enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol"
        case runtimeIdentity = "runtime_identity"
    }
}

struct RuntimeReadinessEvent: Codable, Sendable {
    let protocolVersion: UInt16
    let wireBuild: String
    let runtimeIdentity: RuntimeIdentity
    let progress: ReadinessProgress

    enum CodingKeys: String, CodingKey {
        case protocolVersion = "protocol"
        case wireBuild = "wire_build"
        case runtimeIdentity = "runtime_identity"
        case progress
    }

    var snapshot: RuntimeLifecycleSnapshot {
        RuntimeLifecycleSnapshot(identity: runtimeIdentity, progress: progress)
    }
}

enum RuntimeReadinessCodec {
    static func encodeSubscribe() throws -> Data {
        try JSONEncoder().encode(RuntimeReadinessSubscribeMessage(
            protocolVersion: daemonKitSessionProtocolVersion
        ))
    }

    static func decodeSubscribeAck(_ data: Data) throws {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(object.keys) == ["protocol"]
        else {
            throw RuntimeReadinessValidationError.invalidResponse("readiness subscription fields")
        }
        let response: RuntimeReadinessSubscribeMessage
        do {
            response = try JSONDecoder().decode(RuntimeReadinessSubscribeMessage.self, from: data)
        } catch {
            throw RuntimeReadinessValidationError.invalidResponse("readiness subscription types")
        }
        guard response.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(response.protocolVersion)
        }
    }

    static func decodeEvent(_ data: Data) throws -> RuntimeReadinessEvent {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(object.keys) == ["protocol", "wire_build", "runtime_identity", "progress"],
              let identity = object["runtime_identity"] as? [String: Any],
              Set(identity.keys) == ["runtime_build", "process_generation"],
              let progress = object["progress"] as? [String: Any],
              Set(progress.keys) == ["sequence", "state", "detail"]
        else {
            throw RuntimeReadinessValidationError.invalidResponse("readiness event fields")
        }
        let event: RuntimeReadinessEvent
        do {
            event = try JSONDecoder().decode(RuntimeReadinessEvent.self, from: data)
        } catch {
            throw RuntimeReadinessValidationError.invalidResponse("readiness event types")
        }
        guard event.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(event.protocolVersion)
        }
        guard !event.wireBuild.isEmpty,
              !event.runtimeIdentity.runtimeBuild.isEmpty,
              event.progress.sequence > 0,
              event.progress.detail.count <= daemonKitMaxReadinessDetailBytes
        else {
            throw RuntimeReadinessValidationError.invalidResponse("readiness event values")
        }
        return event
    }
}

enum RuntimeReceiptCodec {
    static func encodeRequest() throws -> Data {
        try JSONEncoder().encode(RuntimeReadinessSubscribeMessage(
            protocolVersion: daemonKitSessionProtocolVersion
        ))
    }

    static func decodeRequest(_ data: Data) throws {
        try RuntimeReadinessCodec.decodeSubscribeAck(data)
    }

    static func encodeResponse(_ identity: RuntimeIdentity) throws -> Data {
        guard !identity.runtimeBuild.isEmpty else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime receipt identity values")
        }
        return try JSONEncoder().encode(RuntimeReceiptResponse(
            protocolVersion: daemonKitSessionProtocolVersion,
            runtimeIdentity: identity
        ))
    }

    static func decodeResponse(_ data: Data) throws -> RuntimeProcessReceipt {
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              Set(object.keys) == ["protocol", "runtime_identity"],
              let identity = object["runtime_identity"] as? [String: Any],
              Set(identity.keys) == ["runtime_build", "process_generation"]
        else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime receipt fields")
        }
        let response: RuntimeReceiptResponse
        do {
            response = try JSONDecoder().decode(RuntimeReceiptResponse.self, from: data)
        } catch {
            throw RuntimeReadinessValidationError.invalidResponse("runtime receipt types")
        }
        guard response.protocolVersion == daemonKitSessionProtocolVersion else {
            throw SessionTransportError.unsupportedProtocolVersion(response.protocolVersion)
        }
        guard !response.runtimeIdentity.runtimeBuild.isEmpty
        else {
            throw RuntimeReadinessValidationError.invalidResponse("runtime receipt identity values")
        }
        return RuntimeProcessReceipt(runtimeIdentity: response.runtimeIdentity)
    }
}

func validateReadinessTransition(from current: ReadinessProgress, to next: ReadinessProgress) throws {
    switch current.state {
    case .starting:
        return
    case .ready:
        guard next.state == .failed || next.state == .draining else {
            throw RuntimeReadinessValidationError.invalidResponse(
                "runtime lifecycle transition \(current.state.rawValue) -> \(next.state.rawValue)"
            )
        }
    case .failed, .draining:
        throw RuntimeReadinessValidationError.invalidResponse(
            "runtime lifecycle advanced after terminal state \(current.state.rawValue)"
        )
    }
}

final class RuntimeLifecycleSequenceValidator: @unchecked Sendable {
    private let lock = NSLock()
    private var current: RuntimeReadinessEvent?

    func accept(_ payload: Data) throws -> Bool {
        let event = try RuntimeReadinessCodec.decodeEvent(payload)
        return try lock.withLock {
            if let current {
                guard event.runtimeIdentity == current.runtimeIdentity else {
                    throw RuntimeReadinessValidationError.invalidResponse(
                        "runtime identity changed on one authenticated session"
                    )
                }
                switch event.progress.sequence {
                case ..<current.progress.sequence:
                    throw RuntimeReadinessValidationError.sequenceRegression(
                        got: event.progress.sequence,
                        previous: current.progress.sequence
                    )
                case current.progress.sequence:
                    guard event.progress == current.progress else {
                        throw RuntimeReadinessValidationError.sequenceMutation(event.progress.sequence)
                    }
                    return false
                default:
                    try validateReadinessTransition(from: current.progress, to: event.progress)
                }
            }
            current = event
            return true
        }
    }
}

struct RuntimeReadinessClock: Sendable {
    let nowNanoseconds: @Sendable () -> UInt64

    static let continuous = RuntimeReadinessClock {
        DispatchTime.now().uptimeNanoseconds
    }
}

final class RuntimeProgressTracker {
    let wireBuild: String
    private(set) var expected: RuntimeIdentity?
    let noProgressTimeout: TimeInterval
    private let clock: RuntimeReadinessClock
    private let timeoutNanoseconds: UInt64
    private(set) var snapshot: RuntimeLifecycleSnapshot?
    private(set) var deadlineNanoseconds: UInt64

    init(
        wireBuild: String,
        expected: RuntimeIdentity?,
        noProgressTimeout: TimeInterval,
        clock: RuntimeReadinessClock = .continuous
    ) {
        self.wireBuild = wireBuild
        self.expected = expected
        self.noProgressTimeout = noProgressTimeout
        self.clock = clock
        timeoutNanoseconds = UInt64(min(
            noProgressTimeout * 1_000_000_000,
            Double(UInt64.max)
        ))
        deadlineNanoseconds = Self.deadline(
            now: clock.nowNanoseconds(),
            timeout: timeoutNanoseconds
        )
    }

    func observe(
        _ event: RuntimeReadinessEvent,
        allowSuccessor: Bool
    ) throws -> Bool {
        let now = clock.nowNanoseconds()
        guard now < deadlineNanoseconds else { throw noProgressError() }
        guard event.wireBuild == wireBuild else {
            throw SocketWireBuildMismatchError(server: event.wireBuild, client: wireBuild)
        }
        if let expected, event.runtimeIdentity != expected {
            throw RuntimeReadinessValidationError.runtimeIdentity(got: event.runtimeIdentity, want: expected)
        }

        let next = event.snapshot
        if let current = snapshot {
            if current.identity != next.identity {
                guard allowSuccessor else {
                    throw RuntimeReadinessValidationError.runtimeIdentity(
                        got: next.identity,
                        want: current.identity
                    )
                }
                snapshot = next
            } else {
                switch next.progress.sequence {
                case ..<current.progress.sequence:
                    throw RuntimeReadinessValidationError.sequenceRegression(
                        got: next.progress.sequence,
                        previous: current.progress.sequence
                    )
                case current.progress.sequence:
                    guard next.progress == current.progress else {
                        throw RuntimeReadinessValidationError.sequenceMutation(next.progress.sequence)
                    }
                default:
                    try validateReadinessTransition(from: current.progress, to: next.progress)
                    snapshot = next
                    deadlineNanoseconds = Self.deadline(now: now, timeout: timeoutNanoseconds)
                }
            }
        } else {
            snapshot = next
        }

        switch next.progress.state {
        case .starting:
            return false
        case .ready:
            return true
        case .failed:
            throw RuntimeFailedError(snapshot: next)
        case .draining:
            throw RuntimeReadinessValidationError.draining(next)
        }
    }

    func pin(_ identity: RuntimeIdentity) {
        if expected != identity {
            expected = identity
            snapshot = nil
        }
    }

    func adopt(
        _ snapshot: RuntimeLifecycleSnapshot,
        allowSuccessor: Bool
    ) throws -> Bool {
        try observe(RuntimeReadinessEvent(
            protocolVersion: daemonKitSessionProtocolVersion,
            wireBuild: wireBuild,
            runtimeIdentity: snapshot.identity,
            progress: snapshot.progress
        ), allowSuccessor: allowSuccessor)
    }

    func checkDeadline() throws {
        guard clock.nowNanoseconds() < deadlineNanoseconds else { throw noProgressError() }
    }

    func remainingTimeInterval() throws -> TimeInterval {
        let now = clock.nowNanoseconds()
        guard now < deadlineNanoseconds else { throw noProgressError() }
        return TimeInterval(deadlineNanoseconds - now) / 1_000_000_000
    }

    func noProgressError() -> ReadinessNoProgressError {
        ReadinessNoProgressError(snapshot: snapshot)
    }

    private static func deadline(now: UInt64, timeout: UInt64) -> UInt64 {
        let result = now.addingReportingOverflow(timeout)
        return result.overflow ? .max : result.partialValue
    }
}
