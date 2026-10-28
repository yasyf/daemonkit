@testable import DaemonKit
import Foundation
import Testing

struct SessionPublicBoundsTests {
    @Test func invalidPublicTransportBoundsFailBeforeIO() async {
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                schema: "suite.v1",
                configuration: .init(maximumFrameBytes: 0)
            )
        }
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                schema: "suite.v1",
                configuration: .init(handshakeTimeout: .infinity)
            )
        }
    }
}
