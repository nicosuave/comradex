import Darwin
import Foundation

enum UIControlCommand: Equatable, Sendable {
    case status
    case setPreferred(pool: String, account: String?)
    case startLogin(account: String)
    case loginStatus(sessionID: String)

    func encoded() throws -> Data {
        let object: [String: Any]
        switch self {
        case .status:
            object = ["command": "ui_status"]
        case .setPreferred(let pool, let account):
            object = [
                "command": "ui_set_preferred",
                "pool": pool,
                "account": account ?? NSNull(),
            ]
        case .startLogin(let account):
            object = ["command": "ui_start_login", "account": account]
        case .loginStatus(let sessionID):
            object = ["command": "ui_login_status", "session_id": sessionID]
        }
        var data = try JSONSerialization.data(withJSONObject: object, options: [.sortedKeys])
        data.append(0x0A)
        return data
    }
}

enum ControlSocketError: LocalizedError {
    case invalidPath(String)
    case socketCreation(Int32)
    case connection(String, Int32)
    case writeFailed(Int32)
    case readFailed(Int32)
    case oversizedResponse
    case emptyResponse
    case malformedResponse
    case daemon(String)

    var errorDescription: String? {
        switch self {
        case .invalidPath(let path): return "Control socket path is too long: \(path)"
        case .socketCreation(let code): return "Could not create control socket (errno \(code))."
        case .connection(let path, let code): return "Could not connect to \(path) (errno \(code))."
        case .writeFailed(let code): return "Could not send control request (errno \(code))."
        case .readFailed(let code): return "Could not read control response (errno \(code))."
        case .oversizedResponse: return "The daemon returned an oversized response."
        case .emptyResponse: return "The daemon returned an empty response."
        case .malformedResponse: return "The daemon returned an invalid response."
        case .daemon(let message): return message
        }
    }
}

protocol ControlServing: Sendable {
    func status() async throws -> UIStatusSnapshot
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot?
    func startLogin(account: String) async throws -> LoginSnapshot
    func loginStatus(sessionID: String) async throws -> LoginSnapshot
}

final class ControlSocketClient: ControlServing, @unchecked Sendable {
    static let maximumResponseBytes = 1_048_576

    let socketPath: String

    init(environment: [String: String] = ProcessInfo.processInfo.environment) {
        if let override = environment["COMRADEX_CONTROL_SOCKET"], !override.isEmpty {
            socketPath = (override as NSString).expandingTildeInPath
        } else {
            socketPath = FileManager.default.homeDirectoryForCurrentUser
                .appendingPathComponent(".config/comradex/state/control.sock").path
        }
    }

    init(socketPath: String) {
        self.socketPath = socketPath
    }

    func status() async throws -> UIStatusSnapshot {
        let data = try await request(.status)
        return try Self.decode(UIStatusSnapshot.self, from: data, preferredKeys: ["status", "ui_status", "snapshot", "payload", "result"])
    }

    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? {
        _ = try await request(.setPreferred(pool: pool, account: account))
        return nil
    }

    func startLogin(account: String) async throws -> LoginSnapshot {
        let data = try await request(.startLogin(account: account))
        return try Self.decode(LoginSnapshot.self, from: data, preferredKeys: ["login", "login_status", "payload", "result"])
    }

    func loginStatus(sessionID: String) async throws -> LoginSnapshot {
        let data = try await request(.loginStatus(sessionID: sessionID))
        return try Self.decode(LoginSnapshot.self, from: data, preferredKeys: ["login", "login_status", "payload", "result"])
    }

    private func request(_ command: UIControlCommand) async throws -> Data {
        let path = socketPath
        let requestData = try command.encoded()
        return try await Task.detached(priority: .userInitiated) {
            try Self.send(requestData, to: path)
        }.value
    }

    static func decode<T: Decodable>(_ type: T.Type, from data: Data, preferredKeys: [String]) throws -> T {
        let root = try JSONSerialization.jsonObject(with: data)
        guard let object = root as? [String: Any] else { throw ControlSocketError.malformedResponse }
        if object["ok"] as? Bool == false {
            throw ControlSocketError.daemon(object["error"] as? String ?? "Control request failed.")
        }
        var payload: Any = object
        for key in preferredKeys {
            if let candidate = object[key], !(candidate is NSNull) {
                payload = candidate
                break
            }
        }
        let payloadData = try JSONSerialization.data(withJSONObject: payload)
        return try JSONDecoder().decode(type, from: payloadData)
    }

    static func send(_ data: Data, to path: String) throws -> Data {
        let utf8 = Array(path.utf8CString)
        guard utf8.count <= MemoryLayout.size(ofValue: sockaddr_un().sun_path) else {
            throw ControlSocketError.invalidPath(path)
        }

        let descriptor = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw ControlSocketError.socketCreation(errno) }
        defer { Darwin.close(descriptor) }

        var timeout = timeval(tv_sec: 2, tv_usec: 0)
        setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        setsockopt(descriptor, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        var noSignal: Int32 = 1
        setsockopt(descriptor, SOL_SOCKET, SO_NOSIGPIPE, &noSignal, socklen_t(MemoryLayout<Int32>.size))

        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            utf8.withUnsafeBytes { source in
                destination.copyBytes(from: source)
            }
        }
        let addressLength = socklen_t(MemoryLayout<sa_family_t>.size + utf8.count)
        address.sun_len = UInt8(addressLength)
        let connected = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(descriptor, $0, addressLength)
            }
        }
        guard connected == 0 else { throw ControlSocketError.connection(path, errno) }

        try data.withUnsafeBytes { rawBuffer in
            guard let baseAddress = rawBuffer.baseAddress else { return }
            var sent = 0
            while sent < rawBuffer.count {
                let count = Darwin.write(descriptor, baseAddress.advanced(by: sent), rawBuffer.count - sent)
                guard count > 0 else { throw ControlSocketError.writeFailed(errno) }
                sent += count
            }
        }

        var response = Data()
        var buffer = [UInt8](repeating: 0, count: 4096)
        while response.count <= maximumResponseBytes {
            let count = Darwin.read(descriptor, &buffer, buffer.count)
            if count < 0 { throw ControlSocketError.readFailed(errno) }
            if count == 0 { break }
            response.append(buffer, count: count)
            if let newline = response.firstIndex(of: 0x0A) {
                response = response.prefix(upTo: newline)
                break
            }
        }
        guard response.count <= maximumResponseBytes else { throw ControlSocketError.oversizedResponse }
        guard !response.isEmpty else { throw ControlSocketError.emptyResponse }
        return response
    }
}
