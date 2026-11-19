import Foundation

/// Account usage published by the menu app for the desktop widget.
///
/// The widget runs sandboxed and cannot reach the daemon's control socket, so the
/// menu app converts each status poll into this compact form and writes it to the
/// shared app group container.
public struct UsageSnapshot: Codable, Equatable, Sendable {
    public var publishedAt: Date
    public var accounts: [UsageAccount]

    public init(publishedAt: Date, accounts: [UsageAccount]) {
        self.publishedAt = publishedAt
        self.accounts = accounts
    }
}

public enum UsageProvider: String, Codable, Sendable {
    case codex, claude

    public var title: String {
        switch self {
        case .codex: return "Codex"
        case .claude: return "Claude"
        }
    }
}

public struct UsageAccount: Codable, Equatable, Identifiable, Sendable {
    public enum Status: String, Codable, Sendable {
        case available
        /// Rejected by the provider until a quota window resets.
        case limited
        case needsSignIn
        case signingIn
        case unavailable
    }

    public var name: String
    public var provider: UsageProvider
    public var status: Status
    /// Quota windows, shortest first.
    public var windows: [UsageWindow]
    /// When a limited account may be retried, if no exhausted window reports its reset.
    public var retryAt: Date?
    /// Index in `windows` of the window the menu shows for this account, so the widget
    /// matches it. Snapshots without one fall back to the most constrained window.
    public var summaryWindowIndex: Int?
    /// Limits scoped to one model or surface, such as Fable's weekly limit. They are shown
    /// as their own labeled buckets and never feed the account-wide summary.
    public var scopedWindows: [UsageWindow]

    public var id: String { name }

    public init(
        name: String,
        provider: UsageProvider,
        status: Status,
        windows: [UsageWindow],
        retryAt: Date? = nil,
        summaryWindowIndex: Int? = nil,
        scopedWindows: [UsageWindow] = []
    ) {
        self.name = name
        self.provider = provider
        self.status = status
        self.windows = windows
        self.retryAt = retryAt
        self.summaryWindowIndex = summaryWindowIndex
        self.scopedWindows = scopedWindows
    }

    private enum CodingKeys: String, CodingKey {
        case name, provider, status, windows, retryAt, summaryWindowIndex, scopedWindows
        /// Written by older menu apps, and still written for older iPhone apps.
        case fableWindow
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decode(String.self, forKey: .name)
        provider = try container.decode(UsageProvider.self, forKey: .provider)
        status = try container.decode(Status.self, forKey: .status)
        windows = try container.decode([UsageWindow].self, forKey: .windows)
        retryAt = try container.decodeIfPresent(Date.self, forKey: .retryAt)
        summaryWindowIndex = try container.decodeIfPresent(Int.self, forKey: .summaryWindowIndex)
        if let scoped = try container.decodeIfPresent([UsageWindow].self, forKey: .scopedWindows) {
            scopedWindows = scoped
        } else if var fable = try container.decodeIfPresent(UsageWindow.self, forKey: .fableWindow) {
            fable.label = fable.label ?? "Fable weekly"
            fable.shortLabel = fable.shortLabel ?? "Fable"
            scopedWindows = [fable]
        } else {
            scopedWindows = []
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(provider, forKey: .provider)
        try container.encode(status, forKey: .status)
        try container.encode(windows, forKey: .windows)
        try container.encodeIfPresent(retryAt, forKey: .retryAt)
        try container.encodeIfPresent(summaryWindowIndex, forKey: .summaryWindowIndex)
        try container.encode(scopedWindows, forKey: .scopedWindows)
        try container.encodeIfPresent(
            scopedWindows.first { $0.shortLabel?.caseInsensitiveCompare("Fable") == .orderedSame },
            forKey: .fableWindow
        )
    }

    /// The scoped limit closest to running out, shown as the inner ring.
    public var tightestScopedWindow: UsageWindow? {
        scopedWindows.min { $0.remainingPercent < $1.remainingPercent }
    }

    /// The window that limits the account first: the least remaining quota, preferring the
    /// shorter window on ties because it resets sooner.
    public var limitingWindow: UsageWindow? {
        windows.enumerated().min { left, right in
            if left.element.remainingPercent != right.element.remainingPercent {
                return left.element.remainingPercent < right.element.remainingPercent
            }
            return left.offset < right.offset
        }?.element
    }

    /// The window that summarizes the account in rings and captions.
    public var summaryWindow: UsageWindow? {
        if let summaryWindowIndex, windows.indices.contains(summaryWindowIndex) {
            return windows[summaryWindowIndex]
        }
        return limitingWindow
    }

    /// Remaining quota shown for the account as a whole, or nil while usage is unknown.
    public var remainingPercent: Int? {
        // A short rate limit also reports as limited, but it leaves the quota intact.
        if status == .limited, windows.isEmpty || windows.contains(where: { $0.remainingPercent == 0 }) {
            return 0
        }
        return summaryWindow?.remainingPercent
    }

    /// When the summary window resets, or when a limited account's exhausted quota returns.
    public func resetDate(at now: Date) -> Date? {
        let reset = status == .limited
            ? windows.filter { $0.remainingPercent == 0 }.compactMap(\.resetsAt).max() ?? retryAt
            : summaryWindow?.resetsAt
        guard let reset, reset > now else { return nil }
        return reset
    }
}

public struct UsageWindow: Codable, Equatable, Sendable {
    /// Window length in seconds, or nil when the provider reports only a percentage.
    public var duration: TimeInterval?
    public var usedPercent: Int
    public var resetsAt: Date?
    /// The limit's own name from the daemon, such as "Fable weekly". Without one, the
    /// name comes from the window's length.
    public var label: String?
    /// The compact form of `label`, such as "Fable".
    public var shortLabel: String?

    public init(duration: TimeInterval?, usedPercent: Int, resetsAt: Date?, label: String? = nil, shortLabel: String? = nil) {
        self.duration = duration
        self.usedPercent = usedPercent
        self.resetsAt = resetsAt
        self.label = label
        self.shortLabel = shortLabel
    }

    public var remainingPercent: Int { min(100, max(0, 100 - usedPercent)) }

    /// A short name for the limit, such as "5-hour", "Weekly", or "Fable weekly".
    public var title: String { label ?? Self.durationTitle(duration) }

    /// A compact name for tight layouts, such as "5h", "7d", or "Fable".
    public var shortTitle: String { shortLabel ?? Self.durationShortTitle(duration) }

    /// Names a window by its length, such as "5-hour" or "Weekly".
    public static func durationTitle(_ duration: TimeInterval?) -> String {
        guard let duration, duration > 0 else { return "Usage" }
        let hours = Int((duration / 3_600).rounded())
        switch hours {
        case 24: return "Daily"
        case 168: return "Weekly"
        case let hours where hours < 48: return "\(hours)-hour"
        default: return "\(Int((duration / 86_400).rounded()))-day"
        }
    }

    /// Names a window compactly by its length, such as "5h" or "7d".
    public static func durationShortTitle(_ duration: TimeInterval?) -> String {
        guard let duration, duration > 0 else { return "" }
        let hours = Int((duration / 3_600).rounded())
        return hours < 48 ? "\(hours)h" : "\(Int((duration / 86_400).rounded()))d"
    }
}

/// Formats the time until `date` as a compact countdown: "42m", "2h 13m", or "6d 18h".
public func compactCountdown(until date: Date, from now: Date) -> String {
    let seconds = Int(date.timeIntervalSince(now))
    if seconds < 60 { return "<1m" }
    if seconds < 3_600 { return "\(seconds / 60)m" }
    if seconds < 86_400 { return "\(seconds / 3_600)h \((seconds % 3_600) / 60)m" }
    return "\(seconds / 86_400)d \((seconds % 86_400) / 3_600)h"
}

extension UsageSnapshot {
    /// Representative accounts for the widget gallery and placeholders.
    public static func sample(at date: Date) -> UsageSnapshot {
        let fiveHours: TimeInterval = 5 * 3_600
        let week: TimeInterval = 7 * 86_400
        return UsageSnapshot(publishedAt: date, accounts: [
            UsageAccount(name: "personal", provider: .codex, status: .available, windows: [
                UsageWindow(duration: fiveHours, usedPercent: 38, resetsAt: date.addingTimeInterval(2 * 3_600 + 14 * 60)),
                UsageWindow(duration: week, usedPercent: 22, resetsAt: date.addingTimeInterval(4 * 86_400 + 6 * 3_600)),
            ]),
            UsageAccount(name: "work", provider: .codex, status: .available, windows: [
                UsageWindow(duration: week, usedPercent: 81, resetsAt: date.addingTimeInterval(2 * 86_400 + 3 * 3_600)),
            ]),
            UsageAccount(name: "claude", provider: .claude, status: .available, windows: [
                UsageWindow(duration: fiveHours, usedPercent: 11, resetsAt: date.addingTimeInterval(47 * 60)),
                UsageWindow(duration: week, usedPercent: 7, resetsAt: date.addingTimeInterval(5 * 86_400 + 3 * 3_600)),
            ], scopedWindows: [
                UsageWindow(duration: week, usedPercent: 42, resetsAt: date.addingTimeInterval(5 * 86_400 + 3 * 3_600),
                            label: "Fable weekly", shortLabel: "Fable"),
            ]),
        ])
    }
}
