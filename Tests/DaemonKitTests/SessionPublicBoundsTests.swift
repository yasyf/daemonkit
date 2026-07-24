@testable import DaemonKit
import Foundation
import Testing

extension SocketTransportTests.SessionAsyncRuntimeTests {
    @Test func invalidPublicTransportBoundsFailBeforeIO() async {
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                wireBuild: "invalid",
                role: SessionPeerRole.unprotected,
                configuration: .init(maximumFrameBytes: 0)
            )
        }
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                wireBuild: "invalid",
                role: SessionPeerRole.unprotected,
                configuration: .init(handshakeTimeout: .infinity)
            )
        }
        await #expect(throws: SessionTransportError.self) {
            _ = try await SocketClient(
                path: "/tmp/never-connect.sock",
                wireBuild: "invalid",
                role: ""
            )
        }
        let server = SocketServer(
            path: "/tmp/never-bind.sock",
            wireBuild: "invalid",
            configuration: .init(maximumActiveRequests: 0, maximumSessions: 0, writeTimeout: .nan)
        ) { _ in .terminal(SocketTerminal()) }
        await #expect(throws: SessionTransportError.self) { try await server.start() }
        await server.stop()
    }
}
