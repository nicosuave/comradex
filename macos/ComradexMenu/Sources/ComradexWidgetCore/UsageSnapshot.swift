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
    /// Model-specific quota, never part of the account-wide limiting window.
    public var fableWindow: UsageWindow?

    public var id: String { name }

    public init(
        name: String,
        provider: UsageProvider,
        status: Status,
        windows: [UsageWindow],
        retryAt: Date? = nil,
        summaryWindowIndex: Int? = nil,
        fableWindow: UsageWindow? = nil
    ) {
        self.name = name
        self.provider = provider
        self.status = status
        self.windows = windows
        self.retryAt = retryAt
        self.summaryWindowIndex = summaryWindowIndex
        self.fableWindow = fableWindow
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

    public init(duration: TimeInterval?, usedPercent: Int, resetsAt: Date?) {
        self.duration = duration
        self.usedPercent = usedPercent
        self.resetsAt = resetsAt
    }

    public var remainingPercent: Int { min(100, max(0, 100 - usedPercent)) }

    /// A short name for the window, such as "5-hour" or "Weekly".
    public var title: String {
        guard let duration, duration > 0 else { return "Usage" }
        let hours = Int((duration / 3_600).rounded())
        switch hours {
        case 24: return "Daily"
        case 168: return "Weekly"
        case let hours where hours < 48: return "\(hours)-hour"
        default: return "\(Int((duration / 86_400).rounded()))-day"
        }
    }

    /// A compact name for tight layouts, such as "5h" or "7d".
    public var shortTitle: String {
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
            ], fableWindow: UsageWindow(duration: week, usedPercent: 42, resetsAt: date.addingTimeInterval(5 * 86_400 + 3 * 3_600))),
        ])
    }
}
