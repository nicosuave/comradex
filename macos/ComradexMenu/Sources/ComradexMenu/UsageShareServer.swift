import ComradexWidgetCore
import Foundation
import OSLog

/// Serves the widget's usage snapshot as `GET /usage` for the iPhone app.
///
/// The iPhone reaches it directly over Tailscale, whose tunnel already encrypts the
/// traffic. The socket listens on every interface so it follows Tailscale's address,
/// but only this Mac and tailnet peers get an answer.
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
        // A BSD socket, not NWListener, so each connection's peer address can be checked
        // before anything is read from it.
        let descriptor: Int32
        let port: UInt16
        do {
            (descriptor, port) = try Self.listen(port: requestedPort)
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

    /// Returns a nonblocking dual-stack listening socket on every interface and the port it bound.
    private static func listen(port: UInt16) throws -> (Int32, UInt16) {
        let descriptor = Darwin.socket(AF_INET6, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
        do {
            var on: Int32 = 1
            var off: Int32 = 0
            setsockopt(descriptor, SOL_SOCKET, SO_REUSEADDR, &on, socklen_t(MemoryLayout<Int32>.size))
            setsockopt(descriptor, IPPROTO_IPV6, IPV6_V6ONLY, &off, socklen_t(MemoryLayout<Int32>.size))
            var address = sockaddr_in6()
            address.sin6_len = UInt8(MemoryLayout<sockaddr_in6>.size)
            address.sin6_family = sa_family_t(AF_INET6)
            address.sin6_port = port.bigEndian
            address.sin6_addr = in6addr_any
            var length = socklen_t(MemoryLayout<sockaddr_in6>.size)
            try withUnsafeMutablePointer(to: &address) { pointer in
                try pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                    guard Darwin.bind(descriptor, socketAddress, length) == 0,
                          Darwin.listen(descriptor, SOMAXCONN) == 0,
                          getsockname(descriptor, socketAddress, &length) == 0
                    else { throw POSIXError(.init(rawValue: errno) ?? .EIO) }
                }
            }
            _ = fcntl(descriptor, F_SETFL, fcntl(descriptor, F_GETFL) | O_NONBLOCK)
            return (descriptor, UInt16(bigEndian: address.sin6_port))
        } catch {
            Darwin.close(descriptor)
            throw error
        }
    }

    private func acceptPending(on descriptor: Int32) {
        while true {
            var peer = sockaddr_in6()
            var length = socklen_t(MemoryLayout<sockaddr_in6>.size)
            let client = withUnsafeMutablePointer(to: &peer) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.accept(descriptor, $0, &length) }
            }
            guard client >= 0 else { return }
            guard Self.isTrustedPeer(peer.sin6_addr) else {
                Darwin.close(client)
                continue
            }
            var noSignal: Int32 = 1
            setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size))
            serve(client)
        }
    }

    /// Loopback and Tailscale's address ranges: 100.64.0.0/10 and fd7a:115c:a1e0::/48.
    /// IPv4 peers arrive as IPv4-mapped IPv6 addresses on the dual-stack socket.
    nonisolated static func isTrustedPeer(_ address: in6_addr) -> Bool {
        let bytes = withUnsafeBytes(of: address) { Array($0) }
        let mappedPrefix: [UInt8] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF]
        if Array(bytes[0..<12]) == mappedPrefix {
            return bytes[12] == 127 || (bytes[12] == 100 && bytes[13] & 0xC0 == 64)
        }
        if bytes == Array(repeating: 0, count: 15) + [1] { return true }
        return Array(bytes[0..<6]) == [0xFD, 0x7A, 0x11, 0x5C, 0xA1, 0xE0]
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
