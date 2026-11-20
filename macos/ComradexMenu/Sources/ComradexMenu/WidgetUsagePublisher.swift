import ComradexWidgetCore
import Foundation
import OSLog
import WidgetKit

extension UsageSnapshot {
    /// Converts daemon status into the widget's form. Accounts keep the menu's order, Codex
    /// before Claude. Inbound accounts are omitted: they forward the client's own login, so
    /// Comradex has no usage to report for them.
    init(status: UIStatusSnapshot, publishedAt: Date, claudeUsage: ClaudeUsageWindow = .fiveHour) {
        let byName = Dictionary(status.accounts.map { ($0.name, $0) }, uniquingKeysWith: { first, _ in first })
        var seen = Set<String>()
        let ordered = (status.pools.flatMap(\.members).compactMap { byName[$0] } + status.accounts)
            .filter { seen.insert($0.name).inserted && !$0.isInbound }
            .map { UsageAccount(account: $0, claudeUsage: claudeUsage) }
        self.init(
            publishedAt: publishedAt,
            accounts: ordered.filter { $0.provider == .codex } + ordered.filter { $0.provider == .claude }
        )
    }
}

extension UsageAccount {
    /// Mirrors the menu's account states and summary window so both surfaces agree.
    init(account: AccountSnapshot, claudeUsage: ClaudeUsageWindow = .fiveHour) {
        let status: Status
        if account.isLoginInProgress {
            status = .signingIn
        } else if account.reauthRequired {
            status = .needsSignIn
        } else if !account.available {
            switch account.unavailableReason?.lowercased() {
            case "quota": status = .limited
            case "needs_login": status = .needsSignIn
            case "login_in_progress": status = .signingIn
            default: status = .unavailable
            }
        } else if account.authState?.caseInsensitiveCompare("signed_out") == .orderedSame {
            status = .needsSignIn
        } else {
            status = .available
        }

        // Account-wide limits make up the summary; scoped limits such as Fable stay separate.
        let shared = account.sharedLimits
        let summaryKey = account.summaryWindowKey(claudeUsage: claudeUsage)
        var windows = shared.map(\.widgetWindow)
        if windows.isEmpty, let usagePercent = account.usagePercent {
            windows = [UsageWindow(duration: nil, usedPercent: usagePercent, resetsAt: nil)]
        }

        self.init(
            name: account.name,
            provider: account.isClaude ? .claude : .codex,
            status: status,
            windows: windows,
            retryAt: account.retryAtUnix.map { Date(timeIntervalSince1970: TimeInterval($0)) },
            summaryWindowIndex: shared.firstIndex { $0.id == summaryKey },
            scopedWindows: account.scopedLimits.map(\.widgetWindow)
        )
    }
}

/// Publishes usage to the desktop widget after each status poll.
///
/// WidgetKit limits how often an app may reload widget timelines, so status and
/// membership changes reload promptly while usage drift reloads at most every few
/// minutes. Unchanged usage is rewritten periodically so the widget can tell fresh data
/// from a menu app that stopped running.
@MainActor
final class WidgetUsagePublisher {
    static let structuralReloadInterval: TimeInterval = 30
    static let usageReloadInterval: TimeInterval = 5 * 60
    static let heartbeatInterval: TimeInterval = 5 * 60
    /// Reset times recomputed from relative provider values drift slightly between polls.
    static let resetTolerance: TimeInterval = 120

    private let store: UsageSnapshotStore
    private let reloadTimelines: @MainActor () -> Void
    private let now: () -> Date
    private let logger = Logger(subsystem: "com.nicosuave.comradex.menu", category: "widget")
    private(set) var published: UsageSnapshot?
    private var lastReload: Date?
    private(set) var reloadDue: Date?

    init(
        store: UsageSnapshotStore = UsageSnapshotStore(),
        reloadTimelines: @escaping @MainActor () -> Void = { WidgetCenter.shared.reloadTimelines(ofKind: usageWidgetKind) },
        now: @escaping () -> Date = Date.init
    ) {
        self.store = store
        self.reloadTimelines = reloadTimelines
        self.now = now
    }

    /// `userInitiated` reloads at once, for setting changes the user expects to see.
    func publish(_ status: UIStatusSnapshot, claudeUsage: ClaudeUsageWindow, userInitiated: Bool = false) {
        // Builds without an app group have no widget data to share.
        guard store.fileURL != nil else { return }
        let date = now()
        let snapshot = UsageSnapshot(status: status, publishedAt: date, claudeUsage: claudeUsage)
        let change = published.map { Self.change(from: $0, to: snapshot) } ?? .structural
        let heartbeatDue = published.map { date.timeIntervalSince($0.publishedAt) >= Self.heartbeatInterval } ?? true

        if change != .none || heartbeatDue {
            do {
                try store.save(snapshot)
                published = snapshot
            } catch {
                logger.error("Widget usage write failed: \(error.localizedDescription, privacy: .public)")
                return
            }
        }

        let interval: TimeInterval?
        switch change {
        case .structural: interval = Self.structuralReloadInterval
        case .usage: interval = Self.usageReloadInterval
        case .none: interval = nil
        }
        if let interval {
            let due = userInitiated ? date : lastReload.map { $0.addingTimeInterval(interval) } ?? date
            reloadDue = min(reloadDue ?? due, due)
        }
        // Polling every few seconds flushes a deferred reload soon after it comes due.
        if let reloadDue, date >= reloadDue {
            reloadTimelines()
            lastReload = date
            self.reloadDue = nil
        }
    }

    enum Change { case none, usage, structural }

    static func change(from old: UsageSnapshot, to new: UsageSnapshot) -> Change {
        guard old.accounts.count == new.accounts.count else { return .structural }
        var change = Change.none
        for (before, after) in zip(old.accounts, new.accounts) {
            guard before.name == after.name, before.provider == after.provider, before.status == after.status,
                  before.windows.count == after.windows.count, before.summaryWindowIndex == after.summaryWindowIndex,
                  before.scopedWindows.map(\.title) == after.scopedWindows.map(\.title)
            else { return .structural }
            let sameUsage = zip(before.windows + before.scopedWindows, after.windows + after.scopedWindows)
                .allSatisfy { previous, current in
                    previous.duration == current.duration
                        && previous.title == current.title
                        && previous.usedPercent == current.usedPercent
                        && closeEnough(previous.resetsAt, current.resetsAt)
                } && closeEnough(before.retryAt, after.retryAt)
            if !sameUsage { change = .usage }
        }
        return change
    }

    private static func closeEnough(_ left: Date?, _ right: Date?) -> Bool {
        switch (left, right) {
        case (nil, nil): return true
        case let (left?, right?): return abs(left.timeIntervalSince(right)) <= resetTolerance
        default: return false
        }
    }
}
