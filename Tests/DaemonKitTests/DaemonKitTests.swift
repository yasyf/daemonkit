@testable import DaemonKit
import Testing

@Suite(.timeLimit(.minutes(1)))
struct DaemonKitTests {
    @Test func lifeProtocolVersionIsPinned() {
        #expect(DaemonKit.lifeProtocolVersion == 1)
    }
}
