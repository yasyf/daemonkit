import Foundation

/// A state-triggered reload gate for WidgetKit consumers, coalescing
/// fire-throughs to at most one per ``interval`` (leading edge) so widgets
/// never exhaust the WidgetKit refresh budget. Does not import WidgetKit —
/// the consumer supplies the reload closure.
public final class ReloadCoalescer: @unchecked Sendable {
    /// The action run when a reload fires; receives the coalesced trigger.
    public typealias ReloadAction = @Sendable (_ trigger: String) -> Void

    private let interval: TimeInterval
    private let now: @Sendable () -> Date
    private let reload: ReloadAction
    private let lock = NSLock()
    private var lastFire: Date?

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
