import Foundation
import ServiceManagement

/// Registration status of a login item, mirroring `SMAppService.Status` behind
/// the ``LoginItemService`` seam.
public enum LoginItemStatus: Sendable, Equatable {
    case enabled
    case requiresApproval
    case notRegistered
    case notFound
}

/// The outcome of reconciling a login item's desired-registered state.
public enum LoginItemState: Sendable, Equatable {
    /// Already enabled — nothing to do.
    case active
    /// The user must approve the item; the Login Items settings pane was opened.
    case pendingApproval
    /// Registered during this reconciliation pass.
    case registered
}

/// Errors surfaced while reconciling a login item.
public enum LoginItemError: Error {
    /// `register()` failed; carries the underlying framework error.
    case registrationFailed(underlying: any Error)
}

/// The seam over `SMAppService` so tests can inject a fake — `SMAppService`
/// itself cannot run under `swift test`.
public protocol LoginItemService: Sendable {
    /// The item's current registration status.
    var status: LoginItemStatus { get }
    /// Registers the item; throws on failure.
    func register() throws
    /// Opens the System Settings Login Items pane for user approval.
    func openSettingsLoginItems()
}

/// The real ``LoginItemService`` backed by an agent plist
/// (`SMAppService.agent(plistName:)`).
///
/// Only the `plistName` is stored (keeping the value `Sendable`); the
/// non-`Sendable` `SMAppService` handle is re-derived per call — the handle is a
/// thin wrapper over the name, so this is free.
public struct AgentLoginItemService: LoginItemService {
    private let plistName: String

    /// Wraps the agent registered under `plistName` in the app's
    /// `Contents/Library/LaunchAgents`.
    public init(plistName: String) {
        self.plistName = plistName
    }

    private var service: SMAppService {
        SMAppService.agent(plistName: plistName)
    }

    public var status: LoginItemStatus {
        switch service.status {
        case .enabled: .enabled
        case .requiresApproval: .requiresApproval
        case .notRegistered: .notRegistered
        case .notFound: .notFound
        @unknown default: .notRegistered
        }
    }

    public func register() throws {
        try service.register()
    }

    public func openSettingsLoginItems() {
        SMAppService.openSystemSettingsLoginItems()
    }
}

/// Reconciles a login item toward the registered/enabled state.
///
/// This is **reconciliation, not a one-shot flag**: ``reconcile()`` switches on
/// the live status every call. A one-shot "have I registered?" boolean would
/// wedge forever the first time the status reads `.notFound` (the item was never
/// seen), because that boolean is never set — reconciling on the real status
/// each time is the bug this design kills.
public struct LoginItem {
    private let service: LoginItemService

    /// Reconciles the item exposed by `service`.
    public init(service: LoginItemService) {
        self.service = service
    }

    /// Reconciles the agent registered under `plistName`.
    public init(plistName: String) {
        self.init(service: AgentLoginItemService(plistName: plistName))
    }

    /// Drives the item toward enabled and reports the resulting state.
    ///
    /// - `.enabled` → ``LoginItemState/active``.
    /// - `.requiresApproval` → opens the settings pane, returns
    ///   ``LoginItemState/pendingApproval``.
    /// - `.notFound` / `.notRegistered` → attempts `register()` and returns
    ///   ``LoginItemState/registered`` (or throws ``LoginItemError``).
    public func reconcile() throws -> LoginItemState {
        switch service.status {
        case .enabled:
            return .active
        case .requiresApproval:
            service.openSettingsLoginItems()
            return .pendingApproval
        case .notFound, .notRegistered:
            do {
                try service.register()
            } catch {
                throw LoginItemError.registrationFailed(underlying: error)
            }
            return .registered
        }
    }
}
