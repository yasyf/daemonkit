import Foundation

/// A state-triggered reload gate for WidgetKit consumers.
///
/// Widgets that reload on every state change quickly exhaust the WidgetKit
/// refresh budget. ``record(trigger:)`` requests a reload, but actual
/// fire-throughs coalesce to **at most one per** ``interval`` (default 5
/// minutes): the first request in a window fires immediately (leading edge) and
/// every later request in the same window is dropped.
///
/// This type does **not** import WidgetKit — the consumer supplies the closure
/// that calls `WidgetCenter.shared.reloadTimelines(...)`. The `now` seam makes
/// the coalescing window testable without a real clock.
public final class ReloadCoalescer: @unchecked Sendable {
    /// The action run when a reload fires; receives the coalesced trigger.
    public typealias ReloadAction = @Sendable (_ trigger: String) -> Void

    private let interval: TimeInterval
    private let now: @Sendable () -> Date
    private let reload: ReloadAction
    private let lock = NSLock()
    private var lastFire: Date?

    /// - Parameters:
    ///   - interval: Minimum spacing between fire-throughs (default 5 minutes).
    ///   - now: Clock seam; defaults to the wall clock.
    ///   - reload: The consumer's WidgetKit reload call.
    public init(
        interval: TimeInterval = 300,
        now: @escaping @Sendable () -> Date = { Date() },
        reload: @escaping ReloadAction
    ) {
        self.interval = interval
        self.now = now
        self.reload = reload
    }

    /// Requests a reload. Fires ``ReloadAction`` immediately when at least
    /// ``interval`` has elapsed since the last fire; otherwise coalesces away.
    public func record(trigger: String) {
        let shouldFire: Bool
        lock.lock()
        let instant = now()
        if let last = lastFire, instant.timeIntervalSince(last) < interval {
            shouldFire = false
        } else {
            lastFire = instant
            shouldFire = true
        }
        lock.unlock()

        if shouldFire {
            reload(trigger)
        }
    }
}
