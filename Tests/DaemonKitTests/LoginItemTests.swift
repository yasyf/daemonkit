@testable import DaemonKit
import Foundation
import Testing

private struct RegistrationBoom: Error {}

private final class FakeLoginItemService: LoginItemService, @unchecked Sendable {
    let status: LoginItemStatus
    private let registerError: (any Error)?
    private let lock = NSLock()
    private var registerCalls = 0
    private var settingsOpened = false

    init(status: LoginItemStatus, registerError: (any Error)? = nil) {
        self.status = status
        self.registerError = registerError
    }

    func register() throws {
        lock.lock()
        registerCalls += 1
        lock.unlock()
        if let registerError {
            throw registerError
        }
    }

    func openSettingsLoginItems() {
        lock.lock()
        settingsOpened = true
        lock.unlock()
    }

    var registerCount: Int {
        lock.lock(); defer { lock.unlock() }; return registerCalls
    }

    var openedSettings: Bool {
        lock.lock(); defer { lock.unlock() }; return settingsOpened
    }
}

@Test func enabledReconcilesToActiveWithoutSideEffects() throws {
    let service = FakeLoginItemService(status: .enabled)
    #expect(try LoginItem(service: service).reconcile() == .active)
    #expect(service.registerCount == 0)
    #expect(service.openedSettings == false)
}

@Test func requiresApprovalOpensSettingsAndPends() throws {
    let service = FakeLoginItemService(status: .requiresApproval)
    #expect(try LoginItem(service: service).reconcile() == .pendingApproval)
    #expect(service.openedSettings == true)
    #expect(service.registerCount == 0)
}

@Test(arguments: [LoginItemStatus.notFound, LoginItemStatus.notRegistered])
func unregisteredStatusesRegister(status: LoginItemStatus) throws {
    let service = FakeLoginItemService(status: status)
    #expect(try LoginItem(service: service).reconcile() == .registered)
    #expect(service.registerCount == 1)
    #expect(service.openedSettings == false)
}

@Test func registrationFailureSurfacesTypedError() {
    let service = FakeLoginItemService(status: .notRegistered, registerError: RegistrationBoom())
    #expect(throws: LoginItemError.self) {
        _ = try LoginItem(service: service).reconcile()
    }
    #expect(service.registerCount == 1)
}
