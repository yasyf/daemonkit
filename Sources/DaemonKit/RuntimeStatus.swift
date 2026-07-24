import Foundation

/// State is one exact product-reported health value.
enum State: String, Codable, Sendable {
    case healthy
    case degraded
    case failed
}

/// HealthStatus is one copied product health snapshot.
struct HealthStatus: Equatable, Sendable {
    let state: State
    let detail: Data

    init(state: State, detail: Data = Data()) {
        self.state = state
        self.detail = Data(detail)
    }
}

/// RuntimeStatusSnapshot is an O(1) runtime-owned health and activity snapshot.
struct RuntimeStatusSnapshot: Equatable, Sendable {
    let health: HealthStatus?
    let busy: Bool
    let admissions: Int
    let workers: Int
    let activities: Int
}

/// StatusReporter is one generation-fenced health and background-activity capability.
struct StatusReporter: Sendable {
    let controller: RuntimeLifecycleController
    let activationID: UUID

    /// Stores a copied health snapshot. Identical updates are exact-idempotent.
    func update(_ status: HealthStatus) throws {
        try controller.updateHealth(activationID: activationID, status: status)
    }

    /// Begins explicit background activity for the same Ready runtime generation.
    func beginActivity() throws -> ActivityLease {
        try controller.beginActivity(activationID: activationID)
    }
}

/// ActivityLease is an exact-idempotent runtime-owned background activity lease.
final class ActivityLease: @unchecked Sendable {
    let id: UUID
    let controller: RuntimeLifecycleController
    private let lock = NSLock()
    private var released = false

    init(id: UUID, controller: RuntimeLifecycleController) {
        self.id = id
        self.controller = controller
    }

    /// Releases this lease. Duplicate and terminal-forced release are successful no-ops.
    func release() throws {
        let first = lock.withLock {
            guard !released else { return false }
            released = true
            return true
        }
        if first {
            controller.releaseActivity(id: id)
        }
    }
}
