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
    let reauthRequired: Bool
    let name: String
    let kind: String
    let signedIn: Bool?
    let authState: String?
    let pools: [String]
    let available: Bool
    let unavailableReason: String?
    let retryAtUnix: Int64?
    let usagePercent: Int?
    let usageUpdatedAtUnix: Int64?
    let usageWindows: [String: UsageWindowSnapshot]
    let reportingUsage: [String: JSONValue]?
    let resetCredits: ResetCreditsSnapshot?

    var id: String { name }
    var isClaude: Bool { kind.lowercased().hasPrefix("claude_") }
    var isSignedIn: Bool {
        signedIn ?? ["signed_in", "authenticated", "ready"].contains(authState?.lowercased())
    }
    var isInbound: Bool {
        kind.caseInsensitiveCompare("inbound") == .orderedSame
            || authState?.caseInsensitiveCompare("inbound") == .orderedSame
    }
    var needsLoginAction: Bool {
        guard !isInbound else { return false }
        return reauthRequired
            || authState?.caseInsensitiveCompare("signed_out") == .orderedSame
            || unavailableReason?.caseInsensitiveCompare("needs_login") == .orderedSame
    }
    var isLoginInProgress: Bool {
        authState?.caseInsensitiveCompare("login_in_progress") == .orderedSame
            || unavailableReason?.caseInsensitiveCompare("login_in_progress") == .orderedSame
    }

    enum CodingKeys: String, CodingKey {
        case name, kind, pools, available
        case reauthRequired = "reauth_required"
        case signedIn = "signed_in"
        case authState = "auth_state"
        case unavailableReason = "unavailable_reason"
        case retryAtUnix = "retry_at_unix"
        case usagePercent = "usage_percent"
        case usageUpdatedAtUnix = "usage_updated_at_unix"
        case usageWindows = "usage_windows"
        case reportingUsage = "reporting_usage"
        case resetCredits = "reset_credits"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        reauthRequired = try container.decodeIfPresent(Bool.self, forKey: .reauthRequired) ?? false
        name = try container.decode(String.self, forKey: .name)
        kind = try container.decodeIfPresent(String.self, forKey: .kind) ?? ""
        signedIn = try container.decodeIfPresent(Bool.self, forKey: .signedIn)
        authState = try container.decodeIfPresent(String.self, forKey: .authState)
        pools = try container.decodeIfPresent([String].self, forKey: .pools) ?? []
        available = try container.decodeIfPresent(Bool.self, forKey: .available) ?? true
        unavailableReason = try container.decodeIfPresent(String.self, forKey: .unavailableReason)
        retryAtUnix = try container.decodeIfPresent(Int64.self, forKey: .retryAtUnix)
        usagePercent = try container.decodeIfPresent(Int.self, forKey: .usagePercent)
        usageUpdatedAtUnix = try container.decodeIfPresent(Int64.self, forKey: .usageUpdatedAtUnix)
        usageWindows = try container.decodeIfPresent([String: UsageWindowSnapshot].self, forKey: .usageWindows) ?? [:]
        reportingUsage = try container.decodeIfPresent([String: JSONValue].self, forKey: .reportingUsage)
        resetCredits = try container.decodeIfPresent(ResetCreditsSnapshot.self, forKey: .resetCredits)
    }
}

struct UsageWindowSnapshot: Codable, Equatable, Sendable {
    let usedPercent: Int?
    let resetAtUnix: Int64?
    let limitWindowSeconds: UInt64?

    enum CodingKeys: String, CodingKey {
        case usedPercent = "used_percent"
        case resetAtUnix = "reset_at_unix"
        case limitWindowSeconds = "limit_window_seconds"
    }
}

/// Which Claude usage limit the menu and widget show for each Claude account.
enum ClaudeUsageWindow: String, CaseIterable, Sendable {
    case fiveHour, weekly

    static let defaultsKey = "ClaudeUsageWindow"

    var title: String {
        switch self {
        case .fiveHour: return "5-Hour Limit"
        case .weekly: return "Weekly Limit"
        }
    }
}

extension AccountSnapshot {
    /// Claude plans without a 5-hour limit, such as Ultra, report that window as a
    /// zero-duration placeholder.
    var lacksFiveHourLimit: Bool { isClaude && usageWindows["5h"]?.limitWindowSeconds == 0 }

    /// The usage window that summarizes this account: Claude's chosen limit, or
    /// the provider's primary window. Zero-duration placeholder windows are skipped.
    func summaryWindowKey(claudeUsage: ClaudeUsageWindow) -> String? {
        let windows = usageWindows.filter { $0.value.usedPercent != nil && $0.value.limitWindowSeconds != 0 }
        if isClaude {
            let timed = windows.compactMap { key, window in window.limitWindowSeconds.map { (key: key, seconds: $0) } }
            let byLength: ((key: String, seconds: UInt64), (key: String, seconds: UInt64)) -> Bool = {
                $0.seconds != $1.seconds ? $0.seconds < $1.seconds : $0.key < $1.key
            }
            let chosen = claudeUsage == .fiveHour ? timed.min(by: byLength) : timed.max(by: byLength)
            if let chosen { return chosen.key }
        }
        return windows.keys.min { left, right in
            let leftOrder = Self.windowOrder(left)
            let rightOrder = Self.windowOrder(right)
            return leftOrder != rightOrder ? leftOrder < rightOrder : left < right
        }
    }

    private static func windowOrder(_ name: String) -> Int {
        switch name.lowercased() {
        case "primary": return 0
        case "secondary": return 1
        case "tertiary": return 2
        default: return 3
        }
    }
}

enum AccountRole: String, CaseIterable, Sendable {
    case normal, preferred, preserved

    var title: String {
        switch self {
        case .preferred: return "Preferred — use first"
        case .normal: return "Automatic — quota-aware selection"
        case .preserved: return "Preserved — use last"
        }
    }
}

struct PoolSnapshot: Codable, Equatable, Identifiable, Sendable {
    let name: String
    let members: [String]
    let preferred: String?
    let active: String?
    var preserved: String? = nil
    var wired: String? = nil

    var id: String { name }
}

struct UIStatusSnapshot: Codable, Equatable, Sendable {
    /// Choosing between Claude's 5-hour and weekly limits only matters when some Claude
    /// account has a 5-hour limit.
    var offersClaudeUsageChoice: Bool { accounts.contains { $0.isClaude && !$0.lacksFiveHourLimit } }

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
    let provider: String
    var isClaude: Bool { provider == "claude" }
    let account: String
    let sessionID: String?
    let state: LoginState
    let verificationURI: String?
    let userCode: String?
    let error: String?

    var statusLabel: String {
        switch state {
        case .idle, .notStarted: return "Idle"
        case .running:
            if isClaude { return "Complete sign-in in your browser" }
            return userCode?.isEmpty == false ? "Waiting for device authorization" : "Requesting a device code"
        case .succeeded: return "Signed in"
        case .failed: return "Login failed"
        }
    }

    init(
        account: String,
        provider: String = "codex",
        sessionID: String? = nil,
        state: LoginState,
        verificationURI: String? = nil,
        userCode: String? = nil,
        error: String? = nil
    ) {
        self.account = account
        self.provider = provider
        self.sessionID = sessionID
        self.state = state
        self.verificationURI = verificationURI
        self.userCode = userCode
        self.error = error
    }

    enum CodingKeys: String, CodingKey {
        case account, state, error, provider
        case sessionID = "session_id"
        case verificationURI = "verification_uri"
        case userCode = "user_code"
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        provider = try container.decodeIfPresent(String.self, forKey: .provider) ?? "codex"
        account = try container.decodeIfPresent(String.self, forKey: .account) ?? ""
        sessionID = try container.decodeIfPresent(String.self, forKey: .sessionID)
        state = try container.decodeIfPresent(LoginState.self, forKey: .state) ?? .idle
        verificationURI = try container.decodeIfPresent(String.self, forKey: .verificationURI)
        userCode = try container.decodeIfPresent(String.self, forKey: .userCode)
        error = try container.decodeIfPresent(String.self, forKey: .error)
    }

    var safeVerificationURL: URL {
        if isClaude {
            if let verificationURI, let url = URL(string: verificationURI),
               url.scheme == "https",
               (url.host == "claude.ai" && url.path == "/oauth/authorize")
                   || (url.host == "claude.com" && url.path == "/cai/oauth/authorize"),
               url.user == nil, url.password == nil, url.port == nil {
                return url
            }
            return URL(string: "https://claude.ai/login")!
        }
        guard let verificationURI,
              let url = URL(string: verificationURI),
              url.scheme?.lowercased() == "https",
              url.host?.lowercased() == "auth.openai.com"
        else { return URL(string: "https://auth.openai.com/codex/device")! }
        return url
    }
}

struct ResetCreditsSnapshot: Codable, Equatable, Sendable {
    let availableCount: Int
    let observedAtUnix: Int64
    let credits: [ResetCreditSnapshot]?
    let error: String?

    enum CodingKeys: String, CodingKey {
        case availableCount = "available_count"
        case observedAtUnix = "observed_at_unix"
        case credits, error
    }

    func availableCount(at now: Date = Date()) -> Int {
        credits.map { $0.filter { $0.isAvailable(at: now) }.count } ?? availableCount
    }
}

struct ResetCreditSnapshot: Codable, Equatable, Sendable {
    let id: String
    let resetType: String
    let status: String
    let grantedAt: String
    let expiresAt: String?
    let title: String?
    let description: String?

    enum CodingKeys: String, CodingKey {
        case id, status, title, description
        case resetType = "reset_type"
        case grantedAt = "granted_at"
        case expiresAt = "expires_at"
    }

    var displayTitle: String { title ?? (resetType == "codex_rate_limits" ? "Full reset" : resetType) }
    // Show the original timestamp, including fractional seconds and UTC offset.
    var expiryDescription: String { expiresAt.map { "Expires \($0)" } ?? "No expiration" }

    func isAvailable(at now: Date = Date()) -> Bool {
        guard status == "available" else { return false }
        guard let expiresAt else { return true }
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        if let expiry = formatter.date(from: expiresAt) { return expiry > now }
        formatter.formatOptions = [.withInternetDateTime]
        guard let expiry = formatter.date(from: expiresAt) else { return false }
        return expiry > now
    }

    func canRedeem(at now: Date = Date()) -> Bool {
        resetType == "codex_rate_limits" && isAvailable(at: now)
    }
}

struct ResetResultSnapshot: Decodable, Sendable {
    let code: String
    let refreshError: String?
    enum CodingKeys: String, CodingKey {
        case code
        case refreshError = "refresh_error"
    }

    var message: String {
        switch code {
        case "reset": return "Reset used successfully."
        case "already_redeemed": return "This reset request already succeeded."
        case "nothing_to_reset": return "No eligible usage window to reset. No credit used."
        case "no_credit": return "No reset credit available."
        default: return "Unknown reset result. Refresh before trying again."
        }
    }
}
