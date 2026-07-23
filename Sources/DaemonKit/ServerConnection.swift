import Darwin
import Foundation

final class ServerConnection: @unchecked Sendable, Hashable {
    let descriptor: Int32
    private let lock = NSLock()
    private var closed = false

    init(descriptor: Int32) {
        self.descriptor = descriptor
    }

    static func == (lhs: ServerConnection, rhs: ServerConnection) -> Bool {
        lhs === rhs
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(ObjectIdentifier(self))
    }

    func shutdown() {
        lock.withLock {
            if !closed {
                Darwin.shutdown(descriptor, SHUT_RDWR)
            }
        }
    }

    func close() {
        lock.withLock {
            guard !closed else { return }
            closed = true
            Darwin.close(descriptor)
        }
    }
}
