@testable import DaemonKit
import Foundation
import Testing

struct SessionPublicBoundsTests {
    @Test func invalidPublicTransportBoundsFailBeforeIO() async {
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                schema: "suite.v1",
                lane: .business,
                configuration: .init(maximumFrameBytes: 0)
            )
        }
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                schema: "suite.v1",
                lane: .business,
                configuration: .init(handshakeTimeout: .infinity)
            )
        }
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                schema: "",
                lane: .business
            )
        }
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                schema: "control-carries-schema",
                lane: .control
            )
        }
    }
}
