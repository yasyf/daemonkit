@testable import DaemonKit
import Testing

@Test func lifeProtocolVersionIsPinned() {
    #expect(DaemonKit.lifeProtocolVersion == 1)
}
