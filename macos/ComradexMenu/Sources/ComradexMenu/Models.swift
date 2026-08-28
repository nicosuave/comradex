import Foundation

enum JSONValue: Codable, Equatable, Sendable {
    case string(String)
    case number(Double)
    case bool(Bool)
    case object([String: JSONValue])
    case array([JSONValue])
    case null

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()
        if container.decodeNil() { self = .null }
        else if let value = try? container.decode(Bool.self) { self = .bool(value) }
        else if let value = try? container.decode(Double.self) { self = .number(value) }
        else if let value = try? container.decode(String.self) { self = .string(value) }
        else if let value = try? container.decode([String: JSONValue].self) { self = .object(value) }
        else if let value = try? container.decode([JSONValue].self) { self = .array(value) }
        else { throw DecodingError.dataCorruptedError(in: container, debugDescription: "Unsupported JSON value") }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case .string(let value): try container.encode(value)
        case .number(let value): try container.encode(value)
        case .bool(let value): try container.encode(value)
        case .object(let value): try container.encode(value)
        case .array(let value): try container.encode(value)
        case .null: try container.encodeNil()
        }
    }

    var conciseDescription: String {
        switch self {
        case .string(let value): return value
        case .number(let value): return value.formatted()
        case .bool(let value): return value ? "Yes" : "No"
        case .object(let value):
            for key in ["state", "status", "label"] {
                if case .string(let text) = value[key] { return text }
            }
            return "Available"
        case .array(let value): return "\(value.count) items"
        case .null: return "Unknown"
        }
    }
}

struct AccountSnapshot: Codable, Equatable, Identifiable, Sendable {
    let name: String
    let kind: String
    let signedIn: Bool?
    let authState: String?
    let pools: [String]
    let available: Bool
    let unavailableReason: String?
    let retryAtUnix: Int64?
    let usagePercent: Int?

    var id: String { name }
    var isSignedIn: Bool {
        signedIn ?? ["signed_in", "authenticated", "ready"].contains(authState?.lowercased())
    }
    var isInbound: Bool {
        kind.caseInsensitiveCompare("inbound") == .orderedSame
            || authState?.caseInsensitiveCompare("inbound") == .orderedSame
    }
    var needsLoginAction: Bool {
        guard !isInbound else { return false }
        return authState?.caseInsensitiveCompare("signed_out") == .orderedSame
    }

    enum CodingKeys: String, CodingKey {
        case name, kind, pools, available
        case signedIn = "signed_in"
        case authState = "auth_state"
        case unavailableReason = "unavailable_reason"
        case retryAtUnix = "retry_at_unix"
        case usagePercent = "usage_percent"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decode(String.self, forKey: .name)
        kind = try container.decodeIfPresent(String.self, forKey: .kind) ?? ""
        signedIn = try container.decodeIfPresent(Bool.self, forKey: .signedIn)
        authState = try container.decodeIfPresent(String.self, forKey: .authState)
        pools = try container.decodeIfPresent([String].self, forKey: .pools) ?? []
        available = try container.decodeIfPresent(Bool.self, forKey: .available) ?? true
        unavailableReason = try container.decodeIfPresent(String.self, forKey: .unavailableReason)
        retryAtUnix = try container.decodeIfPresent(Int64.self, forKey: .retryAtUnix)
        usagePercent = try container.decodeIfPresent(Int.self, forKey: .usagePercent)
    }
}

struct PoolSnapshot: Codable, Equatable, Identifiable, Sendable {
    let name: String
    let members: [String]
    let preferred: String?
    let active: String?

    var id: String { name }
}

struct UIStatusSnapshot: Codable, Equatable, Sendable {
    let daemonRunning: Bool?
    let codexRouted: Bool?
    let service: JSONValue?
    let routing: JSONValue?
    let traffic: JSONValue?
    let accounts: [AccountSnapshot]
    let pools: [PoolSnapshot]

    enum CodingKeys: String, CodingKey {
        case service, routing, traffic, accounts, pools
        case daemonRunning = "daemon_running"
        case codexRouted = "codex_routed"
    }

    init(
        daemonRunning: Bool? = nil,
        codexRouted: Bool? = nil,
        service: JSONValue? = nil,
        routing: JSONValue? = nil,
        traffic: JSONValue? = nil,
        accounts: [AccountSnapshot] = [],
        pools: [PoolSnapshot] = []
    ) {
        self.daemonRunning = daemonRunning
        self.codexRouted = codexRouted
        self.service = service
        self.routing = routing
        self.traffic = traffic
        self.accounts = accounts
        self.pools = pools
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        daemonRunning = try container.decodeIfPresent(Bool.self, forKey: .daemonRunning)
        codexRouted = try container.decodeIfPresent(Bool.self, forKey: .codexRouted)
        service = try container.decodeIfPresent(JSONValue.self, forKey: .service)
        routing = try container.decodeIfPresent(JSONValue.self, forKey: .routing)
        traffic = try container.decodeIfPresent(JSONValue.self, forKey: .traffic)
        accounts = try container.decodeIfPresent([AccountSnapshot].self, forKey: .accounts) ?? []
        pools = try container.decodeIfPresent([PoolSnapshot].self, forKey: .pools) ?? []
    }
}

enum LoginState: String, Codable, Sendable {
    case idle, running, succeeded, failed
    case notStarted = "not_started"
}

struct LoginSnapshot: Codable, Equatable, Sendable {
    let account: String
    let sessionID: String?
    let state: LoginState
    let verificationURI: String?
    let userCode: String?
    let error: String?

    init(
        account: String,
        sessionID: String? = nil,
        state: LoginState,
        verificationURI: String? = nil,
        userCode: String? = nil,
        error: String? = nil
    ) {
        self.account = account
        self.sessionID = sessionID
        self.state = state
        self.verificationURI = verificationURI
        self.userCode = userCode
        self.error = error
    }

    enum CodingKeys: String, CodingKey {
        case account, state, error
        case sessionID = "session_id"
        case verificationURI = "verification_uri"
        case userCode = "user_code"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        account = try container.decodeIfPresent(String.self, forKey: .account) ?? ""
        sessionID = try container.decodeIfPresent(String.self, forKey: .sessionID)
        state = try container.decodeIfPresent(LoginState.self, forKey: .state) ?? .idle
        verificationURI = try container.decodeIfPresent(String.self, forKey: .verificationURI)
        userCode = try container.decodeIfPresent(String.self, forKey: .userCode)
        error = try container.decodeIfPresent(String.self, forKey: .error)
    }

    var safeVerificationURL: URL {
        guard let verificationURI,
              let url = URL(string: verificationURI),
              url.scheme?.lowercased() == "https",
              url.host?.lowercased() == "auth.openai.com"
        else { return URL(string: "https://auth.openai.com/codex/device")! }
        return url
    }
}
