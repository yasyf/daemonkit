import Darwin
import Foundation
import os

private let log = Logger(subsystem: DaemonKit.loggingSubsystem, category: "SocketServer")

/// Errors thrown while binding or running a ``SocketServer``.
public enum SocketServerError: Error, Sendable {
    /// The socket path exceeds the `sockaddr_un.sun_path` capacity.
    case pathTooLong(path: String, limit: Int)
    /// A live peer answered a ping at `path`; refusing to steal the address.
    case addressInUse(path: String)
    /// `socket(2)` failed.
    case socketFailed(errno: Int32)
    /// `bind(2)` failed.
    case bindFailed(path: String, errno: Int32)
    /// `listen(2)` failed.
    case listenFailed(errno: Int32)
    /// ``SocketServer/start()`` was called on an already-started server.
    case alreadyRunning
}

/// A unix-domain line server for helper daemons.
///
/// One request line in, one reply line out, per connection: reads are framed on
/// `\n` and capped (``Configuration/maxLineBytes``, default 64 KiB); the handler
/// receives the request `Data` (newline stripped) and returns the reply `Data`
/// (the framing `\n` is appended by the server).
///
/// The **accept loop runs on a serial `DispatchQueue`** — never a concurrent one.
/// A blocking `accept(2)` on a concurrent queue makes GCD spawn a fresh worker
/// thread per pending block, exploding the thread pool; the serial queue holds
/// exactly one blocked accept. Connections are then handed to a concurrent
/// handler queue, each socket carrying a receive timeout so a silent client
/// cannot wedge a handler thread. Shutdown is clean: the listener stops
/// accepting and in-flight handlers drain before the path is unlinked.
public final class SocketServer: @unchecked Sendable {
    /// Per-server tuning.
    public struct Configuration: Sendable {
        /// Maximum request-line length in bytes (excluding the `\n`).
        public var maxLineBytes: Int
        /// Per-connection receive timeout; bounds handler drain at shutdown.
        public var readTimeout: TimeInterval

        public init(maxLineBytes: Int = 64 * 1024, readTimeout: TimeInterval = 5) {
            self.maxLineBytes = maxLineBytes
            self.readTimeout = readTimeout
        }
    }

    private enum State: Equatable {
        case idle
        case serving
        case stopped
    }

    private let path: String
    private let configuration: Configuration
    private let handler: @Sendable (Data) -> Data
    private let acceptQueue = DispatchQueue(label: "com.yasyf.daemonkit.SocketServer.accept")
    private let handlerQueue = DispatchQueue(
        label: "com.yasyf.daemonkit.SocketServer.handlers",
        attributes: .concurrent
    )
    private let handlerGroup = DispatchGroup()
    private let lock = NSLock()
    private var state: State = .idle
    private var listenerFD: Int32 = -1
    private var shutdownReadFD: Int32 = -1
    private var shutdownWriteFD: Int32 = -1

    /// - Parameters:
    ///   - path: Filesystem path to bind the unix socket at.
    ///   - configuration: Line cap and read timeout.
    ///   - handler: Maps one request line to one reply line.
    public init(
        path: String,
        configuration: Configuration = .init(),
        handler: @escaping @Sendable (Data) -> Data
    ) {
        self.path = path
        self.configuration = configuration
        self.handler = handler
    }

    /// Reclaims any stale socket, binds `path`, `chmod`s it to `0600` before
    /// accepting, and starts the accept loop.
    ///
    /// If `path` already exists, a live peer answering a connect ping means the
    /// bind is refused with ``SocketServerError/addressInUse(path:)``; only a
    /// dead socket (connect fails) is unlinked and rebound.
    public func start() throws {
        lock.lock()
        guard state == .idle else {
            lock.unlock()
            throw SocketServerError.alreadyRunning
        }
        state = .serving
        lock.unlock()

        do {
            let listener = try bind()
            var pipeFDs: [Int32] = [-1, -1]
            guard pipe(&pipeFDs) == 0 else {
                let err = errno
                close(listener)
                throw SocketServerError.socketFailed(errno: err)
            }
            lock.lock()
            listenerFD = listener
            shutdownReadFD = pipeFDs[0]
            shutdownWriteFD = pipeFDs[1]
            lock.unlock()
        } catch {
            lock.lock()
            state = .idle
            lock.unlock()
            throw error
        }

        acceptQueue.async { [weak self] in self?.acceptLoop() }
    }

    /// Stops accepting, drains in-flight handlers, closes the listener, and
    /// unlinks the socket path. Safe to call more than once.
    public func stop() {
        lock.lock()
        guard state == .serving else {
            lock.unlock()
            return
        }
        state = .stopped
        let wfd = shutdownWriteFD
        lock.unlock()

        var byte: UInt8 = 1
        var written: Int
        repeat {
            written = write(wfd, &byte, 1)
        } while written == -1 && errno == EINTR
        precondition(written == 1, "shutdown pipe write failed: errno \(errno)")
        acceptQueue.sync {}
        handlerGroup.wait()

        lock.lock()
        close(shutdownReadFD)
        close(shutdownWriteFD)
        shutdownReadFD = -1
        shutdownWriteFD = -1
        lock.unlock()
        unlink(path)
    }

    private func bind() throws -> Int32 {
        if access(path, F_OK) == 0 {
            if isSocketLive(at: path) {
                throw SocketServerError.addressInUse(path: path)
            }
            unlink(path)
        }
        var addr = try Self.makeAddress(path: path)
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw SocketServerError.socketFailed(errno: errno) }
        let bound = withAddress(&addr) { Darwin.bind(descriptor, $0, $1) }
        guard bound == 0 else {
            let err = errno
            close(descriptor)
            throw SocketServerError.bindFailed(path: path, errno: err)
        }
        chmod(path, 0o600)
        guard listen(descriptor, 64) == 0 else {
            let err = errno
            close(descriptor)
            throw SocketServerError.listenFailed(errno: err)
        }
        return descriptor
    }

    private func isSocketLive(at path: String) -> Bool {
        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { return false }
        defer { close(descriptor) }
        guard var addr = try? Self.makeAddress(path: path) else { return false }
        return withAddress(&addr) { connect(descriptor, $0, $1) } == 0
    }

    private func acceptLoop() {
        while true {
            var fds = [
                pollfd(fd: listenerFD, events: Int16(POLLIN), revents: 0),
                pollfd(fd: shutdownReadFD, events: Int16(POLLIN), revents: 0),
            ]
            let ready = poll(&fds, nfds_t(fds.count), -1)
            if ready < 0 {
                if errno == EINTR {
                    continue
                }
                break
            }
            if fds[1].revents != 0 {
                break
            }
            guard fds[0].revents & Int16(POLLIN) != 0 else { continue }
            let conn = accept(listenerFD, nil, nil)
            if conn < 0 {
                if errno == EINTR || errno == ECONNABORTED {
                    continue
                }
                log.error("accept failed: \(String(cString: strerror(errno)), privacy: .public)")
                continue
            }
            handlerGroup.enter()
            handlerQueue.async {
                self.serve(conn)
                self.handlerGroup.leave()
            }
        }
        close(listenerFD)
    }

    private func serve(_ descriptor: Int32) {
        defer { close(descriptor) }
        configureConnection(descriptor)
        guard let request = readLine(descriptor: descriptor, cap: configuration.maxLineBytes) else { return }
        var reply = handler(request)
        reply.append(0x0A)
        writeAll(descriptor: descriptor, reply)
    }

    private func configureConnection(_ descriptor: Int32) {
        var enable: Int32 = 1
        setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &enable, socklen_t(MemoryLayout<Int32>.size))
        var timeout = timeval(
            tv_sec: Int(configuration.readTimeout),
            tv_usec: Int32((configuration.readTimeout - Double(Int(configuration.readTimeout))) * 1_000_000)
        )
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
    }

    private func readLine(descriptor: Int32, cap: Int) -> Data? {
        var line = Data()
        var chunk = [UInt8](repeating: 0, count: 4096)
        while true {
            let bytesRead = chunk.withUnsafeMutableBytes { read(descriptor, $0.baseAddress, $0.count) }
            if bytesRead <= 0 {
                return nil
            }
            if let newline = chunk[0 ..< bytesRead].firstIndex(of: 0x0A) {
                line.append(contentsOf: chunk[0 ..< newline])
                return line.count <= cap ? line : nil
            }
            line.append(contentsOf: chunk[0 ..< bytesRead])
            if line.count > cap {
                log.debug("request line exceeded cap of \(cap) bytes; dropping connection")
                return nil
            }
        }
    }

    private func writeAll(descriptor: Int32, _ data: Data) {
        var remaining = data
        while !remaining.isEmpty {
            let written = remaining.withUnsafeBytes { buffer -> Int in
                guard let base = buffer.baseAddress else { return 0 }
                return write(descriptor, base, buffer.count)
            }
            if written <= 0 {
                return
            }
            remaining.removeFirst(written)
        }
    }

    private static func makeAddress(path: String) throws -> sockaddr_un {
        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let capacity = MemoryLayout.size(ofValue: addr.sun_path)
        let bytes = Array(path.utf8)
        guard bytes.count < capacity else {
            throw SocketServerError.pathTooLong(path: path, limit: capacity - 1)
        }
        withUnsafeMutableBytes(of: &addr.sun_path) { dst in
            bytes.withUnsafeBytes { dst.copyMemory(from: $0) }
        }
        return addr
    }

    private func withAddress<R>(
        _ addr: inout sockaddr_un,
        _ body: (UnsafePointer<sockaddr>, socklen_t) -> R
    ) -> R {
        withUnsafePointer(to: &addr) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { rebound in
                body(rebound, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
    }
}
