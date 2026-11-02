import ComradexWidgetCore
import Foundation
import OSLog

/// Serves the widget's usage snapshot as `GET /usage` for the iPhone app.
///
/// The listener binds only to loopback. Expose it to the tailnet with
/// `tailscale serve`, which adds TLS and keeps it off the public internet.
@MainActor
final class UsageShareServer {
    static let defaultPort: UInt16 = 4363
    static let enabledDefaultsKey = "UsageSharingEnabled"
    static let portDefaultsKey = "UsageSharePort"
    /// Requests are a single small GET; anything larger is rejected.
    static let maximumRequestSize = 8_192

    enum State: Equatable {
        case stopped
        case starting
        case listening(port: UInt16)
        case failed(String)
    }

    private(set) var state = State.stopped {
        didSet { if state != oldValue { onStateChange?(state) } }
    }
    var onStateChange: (@MainActor (State) -> Void)?

    private let requestedPort: UInt16
    private let snapshot: @MainActor () -> UsageSnapshot?
    private var listener: DispatchSourceRead?
    private let logger = Logger(subsystem: "com.nicosuave.comradex.menu", category: "usage-share")

    /// Port 0 picks an ephemeral port, for tests.
    init(port: UInt16, snapshot: @escaping @MainActor () -> UsageSnapshot?) {
        requestedPort = port
        self.snapshot = snapshot
    }

    func start() {
        guard listener == nil else { return }
        // A BSD socket, not NWListener: Network.framework refuses a port that any address
        // already uses, so `tailscale serve` on the tailnet address would block loopback.
        let descriptor: Int32
        let port: UInt16
        do {
            (descriptor, port) = try Self.listenOnLoopback(port: requestedPort)
        } catch {
            logger.error("Usage sharing listener failed: \(error.localizedDescription, privacy: .public)")
            state = .failed(error.localizedDescription)
            return
        }
        let source = DispatchSource.makeReadSource(fileDescriptor: descriptor, queue: .main)
        source.setEventHandler { [weak self] in
            MainActor.assumeIsolated { self?.acceptPending(on: descriptor) }
        }
        source.setCancelHandler { Darwin.close(descriptor) }
        listener = source
        source.resume()
        state = .listening(port: port)
    }

    func stop() {
        listener?.cancel()
        listener = nil
        state = .stopped
    }

    /// Returns a nonblocking listening socket on 127.0.0.1 and the port it bound.
    private static func listenOnLoopback(port: UInt16) throws -> (Int32, UInt16) {
        let descriptor = Darwin.socket(AF_INET, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
        do {
            var reuse: Int32 = 1
            setsockopt(descriptor, SOL_SOCKET, SO_REUSEADDR, &reuse, socklen_t(MemoryLayout<Int32>.size))
            var address = sockaddr_in()
            address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
            address.sin_family = sa_family_t(AF_INET)
            address.sin_port = port.bigEndian
            address.sin_addr.s_addr = INADDR_LOOPBACK.bigEndian
            var length = socklen_t(MemoryLayout<sockaddr_in>.size)
            try withUnsafeMutablePointer(to: &address) { pointer in
                try pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                    guard Darwin.bind(descriptor, socketAddress, length) == 0,
                          Darwin.listen(descriptor, SOMAXCONN) == 0,
                          getsockname(descriptor, socketAddress, &length) == 0
                    else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
                }
            }
            _ = fcntl(descriptor, F_SETFL, fcntl(descriptor, F_GETFL) | O_NONBLOCK)
            return (descriptor, UInt16(bigEndian: address.sin_port))
        } catch {
            Darwin.close(descriptor)
            throw error
        }
    }

    private func acceptPending(on descriptor: Int32) {
        while true {
            let client = Darwin.accept(descriptor, nil, nil)
            guard client >= 0 else { return }
            var noSignal: Int32 = 1
            setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size))
            serve(client)
        }
    }

    /// Reads one request from the nonblocking client socket, answers it, and closes it.
    private func serve(_ client: Int32) {
        _ = fcntl(client, F_SETFL, fcntl(client, F_GETFL) | O_NONBLOCK)
        let source = DispatchSource.makeReadSource(fileDescriptor: client, queue: .main)
        var request = Data()
        var finished = false
        let finish: (Data?) -> Void = { response in
            guard !finished else { return }
            finished = true
            if let response { Self.writeAll(response, to: client) }
            source.cancel()
        }
        source.setEventHandler { [weak self] in
            MainActor.assumeIsolated {
                var buffer = [UInt8](repeating: 0, count: 4_096)
                let count = Darwin.read(client, &buffer, buffer.count)
                if count < 0, errno == EAGAIN { return }
                guard count > 0, let self else { return finish(nil) }
                request.append(contentsOf: buffer[0..<count])
                if request.range(of: Data("\r\n\r\n".utf8)) != nil {
                    finish(Self.response(to: request, snapshot: self.snapshot()))
                } else if request.count > Self.maximumRequestSize {
                    finish(Self.response(status: "431 Request Header Fields Too Large"))
                }
            }
        }
        source.setCancelHandler { Darwin.close(client) }
        source.resume()
        // A client that never finishes its request must not hold the connection open.
        DispatchQueue.main.asyncAfter(deadline: .now() + 10) { finish(nil) }
    }

    /// Responses are a few kilobytes, so a short blocking write is enough.
    private static func writeAll(_ data: Data, to client: Int32) {
        _ = fcntl(client, F_SETFL, fcntl(client, F_GETFL) & ~O_NONBLOCK)
        var timeout = timeval(tv_sec: 2, tv_usec: 0)
        setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        data.withUnsafeBytes { bytes in
            var offset = 0
            while offset < bytes.count {
                let written = Darwin.write(client, bytes.baseAddress! + offset, bytes.count - offset)
                guard written > 0 else { return }
                offset += written
            }
        }
    }

    /// Builds the HTTP response for a raw request. Only `GET /usage` succeeds.
    nonisolated static func response(to request: Data, snapshot: UsageSnapshot?) -> Data {
        let requestLine = String(decoding: request.prefix { $0 != UInt8(ascii: "\r") }, as: UTF8.self)
        let parts = requestLine.split(separator: " ")
        guard parts.count == 3, parts[2].hasPrefix("HTTP/1.") else {
            return response(status: "400 Bad Request")
        }
        let path = parts[1].split(separator: "?", maxSplits: 1).first.map(String.init) ?? ""
        guard path == "/usage" else { return response(status: "404 Not Found") }
        guard parts[0] == "GET" || parts[0] == "HEAD" else {
            return response(status: "405 Method Not Allowed", headers: ["Allow": "GET, HEAD"])
        }
        // The menu app has not read the daemon yet.
        guard let snapshot, let body = try? UsageSnapshotStore.encode(snapshot) else {
            return response(status: "503 Service Unavailable")
        }
        return response(
            status: "200 OK",
            headers: ["Content-Type": "application/json", "Cache-Control": "no-store"],
            body: body,
            includeBody: parts[0] == "GET"
        )
    }

    nonisolated static func response(
        status: String,
        headers: [String: String] = [:],
        body: Data = Data(),
        includeBody: Bool = true
    ) -> Data {
        var head = "HTTP/1.1 \(status)\r\n"
        let allHeaders = headers.merging(["Content-Length": "\(body.count)", "Connection": "close"]) { current, _ in current }
        for (name, value) in allHeaders.sorted(by: { $0.key < $1.key }) {
            head += "\(name): \(value)\r\n"
        }
        head += "\r\n"
        return Data(head.utf8) + (includeBody ? body : Data())
    }
}
