@testable import DaemonKit
import Foundation
import Testing

private final class MutableClock: @unchecked Sendable {
    private let lock = NSLock()
    private var instant: Date
    init(_ start: Date) {
        instant = start
    }

    var now: Date {
        get { lock.lock(); defer { lock.unlock() }; return instant }
        set { lock.lock(); instant = newValue; lock.unlock() }
    }
}

private final class FireRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var _triggers: [String] = []
    func record(_ trigger: String) {
        lock.lock(); _triggers.append(trigger); lock.unlock()
    }

    var triggers: [String] {
        lock.lock(); defer { lock.unlock() }; return _triggers
    }
}

@Suite(.timeLimit(.minutes(1)))
struct ReloadCoalescerTests {
    @Test func coalescerCollapsesToOneFirePerInterval() {
        let clock = MutableClock(Date(timeIntervalSince1970: 0))
        let recorder = FireRecorder()
        let coalescer = ReloadCoalescer(
            interval: 300,
            now: { clock.now },
            reload: { recorder.record($0) }
        )

        coalescer.record(trigger: "a")
        coalescer.record(trigger: "b")
        #expect(recorder.triggers == ["a"])

        clock.now = Date(timeIntervalSince1970: 299)
        coalescer.record(trigger: "c")
        #expect(recorder.triggers == ["a"])

        clock.now = Date(timeIntervalSince1970: 300)
        coalescer.record(trigger: "d")
        #expect(recorder.triggers == ["a", "d"])

        clock.now = Date(timeIntervalSince1970: 601)
        coalescer.record(trigger: "e")
        #expect(recorder.triggers == ["a", "d", "e"])
    }

    @Test func coalescerFiresFirstRecordImmediately() {
        let recorder = FireRecorder()
        let coalescer = ReloadCoalescer(
            interval: 300,
            now: { Date(timeIntervalSince1970: 0) },
            reload: { recorder.record($0) }
        )
        coalescer.record(trigger: "boot")
        #expect(recorder.triggers == ["boot"])
    }
}
