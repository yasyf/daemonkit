import Foundation

enum SessionTime {
    static func unixMilliseconds(_ date: Date) -> Int64 {
        let milliseconds = date.timeIntervalSince1970 * 1000
        if milliseconds.isNaN {
            return 1
        }
        guard milliseconds.isFinite else { return milliseconds.sign == .minus ? 1 : .max }
        if milliseconds <= 1 {
            return 1
        }
        if milliseconds >= Double(Int64.max) {
            return .max
        }
        return Int64(milliseconds.rounded(.down))
    }

    static func remainingMilliseconds(until deadline: Int64, now: Date = Date()) -> Int64 {
        let current = unixMilliseconds(now)
        guard deadline > current else { return 0 }
        return deadline - current
    }
}
