import ComradexWidgetCore
import Foundation
import Network
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
    private var listener: NWListener?
    private let logger = Logger(subsystem: "com.nicosuave.comradex.menu", category: "usage-share")

    /// Port 0 picks an ephemeral port, for tests.
    init(port: UInt16, snapshot: @escaping @MainActor () -> UsageSnapshot?) {
        requestedPort = port
        self.snapshot = snapshot
    }

    func start() {
        guard listener == nil else { return }
        let parameters = NWParameters.tcp
        parameters.allowLocalEndpointReuse = true
        parameters.requiredLocalEndpoint = .hostPort(
            host: .ipv4(.loopback),
            port: NWEndpoint.Port(rawValue: requestedPort) ?? .any
        )
        let listener: NWListener
        do {
            listener = try NWListener(using: parameters)
        } catch {
            state = .failed(error.localizedDescription)
            return
        }
        self.listener = listener
        state = .starting
        listener.stateUpdateHandler = { [weak self] update in
            MainActor.assumeIsolated { self?.listenerChanged(update) }
        }
        listener.newConnectionHandler = { [weak self] connection in
            MainActor.assumeIsolated { self?.accept(connection) }
        }
        listener.start(queue: .main)
    }

    func stop() {
        listener?.cancel()
        listener = nil
        state = .stopped
    }

    private func listenerChanged(_ update: NWListener.State) {
        switch update {
        case .ready:
            state = .listening(port: listener?.port?.rawValue ?? requestedPort)
        case let .failed(error), let .waiting(error):
            logger.error("Usage sharing listener failed: \(error.localizedDescription, privacy: .public)")
            listener?.cancel()
            listener = nil
            state = .failed(error.localizedDescription)
        default:
            break
        }
    }

    private func accept(_ connection: NWConnection) {
        connection.start(queue: .main)
        // A client that never finishes its request must not hold the connection open.
        DispatchQueue.main.asyncAfter(deadline: .now() + 10) { connection.cancel() }
        receive(on: connection, buffered: Data())
    }

    private func receive(on connection: NWConnection, buffered: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: Self.maximumRequestSize) { [weak self] data, _, isComplete, error in
            MainActor.assumeIsolated {
                guard let self else { return connection.cancel() }
                var request = buffered
                if let data { request.append(data) }
                if request.range(of: Data("\r\n\r\n".utf8)) != nil {
                    self.respond(on: connection, to: request)
                } else if request.count > Self.maximumRequestSize {
                    self.send(Self.response(status: "431 Request Header Fields Too Large"), on: connection)
                } else if isComplete || error != nil {
                    connection.cancel()
                } else {
                    self.receive(on: connection, buffered: request)
                }
            }
        }
    }

    private func respond(on connection: NWConnection, to request: Data) {
        send(Self.response(to: request, snapshot: snapshot()), on: connection)
    }

    private func send(_ response: Data, on connection: NWConnection) {
        connection.send(content: response, completion: .contentProcessed { _ in connection.cancel() })
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
