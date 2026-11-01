import SwiftUI
import WidgetKit

/// Desktop widget content for every supported family.
public struct UsageWidgetView: View {
    let snapshot: UsageSnapshot?
    let date: Date
    let family: WidgetFamily

    public init(snapshot: UsageSnapshot?, date: Date, family: WidgetFamily) {
        self.snapshot = snapshot
        self.date = date
        self.family = family
    }

    public var body: some View {
        if let snapshot, !snapshot.accounts.isEmpty {
            switch family {
            case .systemSmall: SmallUsageView(accounts: snapshot.accounts, date: date)
            case .systemMedium: MediumUsageView(snapshot: snapshot, date: date)
            case .systemExtraLarge: ExtraLargeUsageView(snapshot: snapshot, date: date)
            default: LargeUsageView(snapshot: snapshot, date: date)
            }
        } else {
            EmptyUsageView(hasSnapshot: snapshot != nil)
        }
    }
}

/// Data older than this is labeled with its age; the menu app republishes every few minutes.
let staleInterval: TimeInterval = 30 * 60

#if os(iOS)
/// The iPhone widget fetches usage that the Mac's menu app serves over Tailscale.
let appName = "Comradex"
let emptyHint = "Open Comradex to connect to your Mac."
#else
let appName = "Comradex Menu"
let emptyHint = "Open Comradex Menu to show usage."
#endif

// MARK: - Levels

enum UsageLevel {
    case plenty, low, critical, attention, unknown

    init(account: UsageAccount) {
        switch account.status {
        case .limited: self = .critical
        case .available: self.init(remaining: account.remainingPercent)
        case .needsSignIn: self = .attention
        case .signingIn, .unavailable: self = .unknown
        }
    }

    init(remaining: Int?) {
        switch remaining {
        case nil: self = .unknown
        case let remaining? where remaining < 10: self = .critical
        case let remaining? where remaining < 25: self = .low
        default: self = .plenty
        }
    }

    var tint: Color {
        switch self {
        case .plenty: return .green
        case .low, .attention: return .orange
        case .critical: return .red
        case .unknown: return .gray
        }
    }
}

// MARK: - Building blocks

/// A circular gauge in the style of the system Batteries widget.
struct UsageRing<Center: View>: View {
    let fraction: Double?
    let tint: Color
    let lineWidth: CGFloat
    @ViewBuilder let center: Center
    @Environment(\.widgetRenderingMode) private var renderingMode

    var body: some View {
        ZStack {
            Circle()
                .stroke(trackColor, lineWidth: lineWidth)
            if let fraction, fraction > 0 {
                Circle()
                    .trim(from: 0, to: min(1, fraction))
                    .stroke(tint, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                    .rotationEffect(.degrees(-90))
                    .widgetAccentable()
            }
            center
                .padding(lineWidth)
        }
        .padding(lineWidth / 2)
    }

    private var trackColor: Color {
        renderingMode == .fullColor ? tint.opacity(0.24) : Color.primary.opacity(0.16)
    }
}

/// A horizontal capsule meter for one quota window.
struct UsageBar: View {
    let fraction: Double
    let tint: Color
    let height: CGFloat
    @Environment(\.widgetRenderingMode) private var renderingMode

    var body: some View {
        GeometryReader { proxy in
            ZStack(alignment: .leading) {
                Capsule()
                    .fill(renderingMode == .fullColor ? tint.opacity(0.22) : Color.primary.opacity(0.16))
                if fraction > 0 {
                    Capsule()
                        .fill(tint)
                        .frame(width: max(height, proxy.size.width * min(1, fraction)))
                        .widgetAccentable()
                }
            }
        }
        .frame(height: height)
    }
}

struct PercentText: View {
    let percent: Int
    let size: CGFloat

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 0.5) {
            Text("\(percent)")
                .font(.system(size: size, weight: .semibold, design: .rounded))
            Text("%")
                .font(.system(size: size * 0.6, weight: .semibold, design: .rounded))
                .foregroundStyle(.secondary)
        }
        .monospacedDigit()
        .lineLimit(1)
        .minimumScaleFactor(0.5)
    }
}

/// The ring's center: remaining quota, or a symbol when usage cannot be shown.
struct AccountRingCenter: View {
    let account: UsageAccount
    let size: CGFloat

    var body: some View {
        if let symbol = statusSymbol {
            Image(systemName: symbol)
                .font(.system(size: size * 0.85, weight: .bold, design: .rounded))
                .foregroundStyle(account.status == .needsSignIn ? AnyShapeStyle(.orange) : AnyShapeStyle(.secondary))
        } else if let remaining = account.remainingPercent {
            PercentText(percent: remaining, size: size)
        } else {
            Text("–")
                .font(.system(size: size, weight: .semibold, design: .rounded))
                .foregroundStyle(.secondary)
        }
    }

    private var statusSymbol: String? {
        switch account.status {
        case .needsSignIn, .unavailable: return "exclamationmark"
        case .signingIn: return "ellipsis"
        case .available, .limited: return nil
        }
    }
}

struct AccountRing: View {
    let account: UsageAccount
    let diameter: CGFloat

    var body: some View {
        UsageRing(
            fraction: account.remainingPercent.map { Double($0) / 100 },
            tint: UsageLevel(account: account).tint,
            lineWidth: max(3.5, diameter * 0.1)
        ) {
            AccountRingCenter(account: account, size: diameter * 0.27)
        }
        .frame(width: diameter, height: diameter)
    }
}

/// The status line under an account: when its quota returns, or why usage is missing.
/// The compact form drops "Resets in" so narrow columns keep a readable size.
func accountCaption(_ account: UsageAccount, at date: Date, compact: Bool = false) -> String {
    switch account.status {
    case .needsSignIn: return compact ? "Sign in" : "Sign-in required"
    case .signingIn: return "Signing in…"
    case .unavailable: return "Unavailable"
    case .available where account.windows.isEmpty: return compact ? "Pending" : "Usage pending"
    case .available, .limited:
        guard let reset = account.resetDate(at: date) else {
            return account.status == .limited ? "Limit reached" : account.summaryWindow?.title ?? ""
        }
        let countdown = compactCountdown(until: reset, from: date)
        if account.status == .limited { return "Back in \(countdown)" }
        return compact ? countdown : "Resets in \(countdown)"
    }
}

func updatedCaption(_ snapshot: UsageSnapshot, at date: Date) -> String? {
    guard date.timeIntervalSince(snapshot.publishedAt) > staleInterval else { return nil }
    return "Updated \(compactCountdown(until: date, from: snapshot.publishedAt)) ago"
}

func accessibilityDescription(_ account: UsageAccount, at date: Date) -> String {
    let usage = account.remainingPercent.map { "\($0) percent left" }
    return [account.name, account.provider.title, usage, accountCaption(account, at: date)]
        .compactMap { $0 }
        .joined(separator: ", ")
}

/// A ring with the account name and status beneath it.
struct AccountRingCell: View {
    enum Caption { case none, compact, full }

    let account: UsageAccount
    let date: Date
    let diameter: CGFloat
    var caption = Caption.full

    var body: some View {
        VStack(spacing: 5) {
            AccountRing(account: account, diameter: diameter)
            VStack(spacing: 1) {
                Text(account.name)
                    .font(.system(size: 11, weight: .semibold))
                if caption != .none {
                    captionText
                        .font(.system(size: 10, weight: .medium))
                        .monospacedDigit()
                        .foregroundStyle(.secondary)
                }
            }
            .lineLimit(1)
            .minimumScaleFactor(0.85)
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(accessibilityDescription(account, at: date))
    }

    private var captionText: Text {
        let text = accountCaption(account, at: date, compact: caption == .compact)
        // A bare countdown gets the reset glyph so it still reads as "resets in".
        if caption == .compact, account.status == .available, account.resetDate(at: date) != nil {
            return Text("\(Image(systemName: "arrow.clockwise")) \(text)")
        }
        return Text(text)
    }
}

/// Each quota window as a labeled meter with its remaining share and reset countdown.
/// Fixed column widths keep meters aligned across accounts.
struct WindowList: View {
    let account: UsageAccount
    let date: Date
    let compact: Bool

    var body: some View {
        if account.windows.isEmpty || !(account.status == .available || account.status == .limited) {
            Text(accountCaption(account, at: date))
                .font(.system(size: compact ? 10 : 11, weight: .medium))
                .foregroundStyle(account.status == .needsSignIn ? AnyShapeStyle(.orange) : AnyShapeStyle(.secondary))
        } else {
            VStack(alignment: .leading, spacing: compact ? 4 : 8) {
                ForEach(Array(account.windows.enumerated()), id: \.offset) { _, window in
                    HStack(spacing: 7) {
                        Text(compact ? window.shortTitle : window.title)
                            .foregroundStyle(.secondary)
                            .frame(width: compact ? 20 : 44, alignment: .leading)
                        UsageBar(
                            fraction: Double(window.remainingPercent) / 100,
                            tint: UsageLevel(remaining: window.remainingPercent).tint,
                            height: compact ? 5 : 6
                        )
                        Text("\(window.remainingPercent)%")
                            .frame(width: compact ? 30 : 34, alignment: .trailing)
                        Text(window.resetsAt.flatMap { $0 > date ? compactCountdown(until: $0, from: date) : nil } ?? "")
                            .foregroundStyle(.secondary)
                            .frame(width: compact ? 42 : 48, alignment: .trailing)
                    }
                }
            }
            .font(.system(size: compact ? 10 : 11, weight: .medium))
            .monospacedDigit()
            .lineLimit(1)
        }
    }
}

struct StaleFootnote: View {
    let text: String

    var body: some View {
        Label(text, systemImage: "clock")
            .font(.system(size: 10, weight: .medium))
            .foregroundStyle(.secondary)
            .lineLimit(1)
    }
}

/// The title row of the large layouts, noting the data's age only when it is stale.
struct UsageHeader: View {
    let snapshot: UsageSnapshot
    let date: Date

    var body: some View {
        HStack(alignment: .firstTextBaseline) {
            Text("Usage")
                .font(.system(size: 15, weight: .semibold))
            Spacer()
            if let updated = updatedCaption(snapshot, at: date) {
                StaleFootnote(text: updated)
            }
        }
    }
}

struct ProviderSection: Identifiable {
    let provider: UsageProvider
    var accounts: [UsageAccount]
    var id: String { "\(provider.rawValue):\(accounts.first?.name ?? "")" }
}

/// Groups consecutive accounts by provider, keeping the menu's Codex-then-Claude order.
func providerSections(_ accounts: [UsageAccount]) -> [ProviderSection] {
    var sections: [ProviderSection] = []
    for account in accounts {
        if sections.last?.provider == account.provider {
            sections[sections.count - 1].accounts.append(account)
        } else {
            sections.append(ProviderSection(provider: account.provider, accounts: [account]))
        }
    }
    return sections
}

/// Detail lines an account occupies: its name plus one per window.
func detailLines(_ account: UsageAccount) -> Int { 1 + max(1, account.windows.count) }

/// The leading accounts whose details, including provider headings, fit in `lines`.
func accountsFitting(_ accounts: [UsageAccount], lines: Int) -> [UsageAccount] {
    var used = 0
    var fitting: [UsageAccount] = []
    for account in accounts {
        let heading = fitting.last?.provider == account.provider ? 0 : 1
        used += heading + detailLines(account)
        guard used <= lines else { break }
        fitting.append(account)
    }
    return fitting
}

struct MoreAccountsNote: View {
    let count: Int

    var body: some View {
        if count > 0 {
            Text("\(count) more in \(appName)")
                .font(.system(size: 11, weight: .medium))
                .foregroundStyle(.secondary)
                .padding(.top, 6)
        }
    }
}

// MARK: - Small

struct SmallUsageView: View {
    let accounts: [UsageAccount]
    let date: Date

    var body: some View {
        Group {
            switch accounts.count {
            case 1:
                AccountRingCell(account: accounts[0], date: date, diameter: 88)
            case 2:
                HStack(spacing: 12) {
                    ForEach(accounts) { AccountRingCell(account: $0, date: date, diameter: 56, caption: .compact) }
                }
            default:
                // Two rows of two, like the Batteries widget. Larger sizes list every account.
                let shown = Array(accounts.prefix(4))
                VStack(spacing: 10) {
                    ForEach([Array(shown.prefix(2)), Array(shown.dropFirst(2))], id: \.first?.id) { row in
                        HStack(spacing: 16) {
                            ForEach(row) { AccountRingCell(account: $0, date: date, diameter: 46, caption: .none) }
                        }
                    }
                }
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Medium

struct MediumUsageView: View {
    let snapshot: UsageSnapshot
    let date: Date

    var body: some View {
        let accounts = Array(snapshot.accounts.prefix(4))
        VStack(spacing: 0) {
            if accounts.count == 1 {
                HStack(spacing: 20) {
                    AccountRingCell(account: accounts[0], date: date, diameter: 80, caption: .none)
                    WindowList(account: accounts[0], date: date, compact: false)
                }
            } else {
                HStack(spacing: 0) {
                    ForEach(accounts) { account in
                        AccountRingCell(
                            account: account,
                            date: date,
                            diameter: accounts.count > 3 ? 60 : 66,
                            caption: accounts.count > 3 ? .compact : .full
                        )
                        .frame(maxWidth: .infinity)
                    }
                }
            }
            if let updated = updatedCaption(snapshot, at: date) {
                StaleFootnote(text: updated)
                    .padding(.top, 10)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Large

struct LargeUsageView: View {
    let snapshot: UsageSnapshot
    let date: Date

    var body: some View {
        let shown = accountsFitting(snapshot.accounts, lines: 15)
        // Few accounts get roomier rows so the widget does not look empty.
        let roomy = shown.reduce(0) { $0 + detailLines($1) } <= 8
        VStack(alignment: .leading, spacing: 0) {
            UsageHeader(snapshot: snapshot, date: date)
                .padding(.bottom, roomy ? 16 : 10)
            AccountSections(accounts: shown, date: date, roomy: roomy, showsRings: true)
            Spacer(minLength: 0)
            MoreAccountsNote(count: snapshot.accounts.count - shown.count)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }
}

/// Accounts grouped under provider headings.
struct AccountSections: View {
    let accounts: [UsageAccount]
    let date: Date
    let roomy: Bool
    let showsRings: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: roomy ? 20 : 12) {
            ForEach(providerSections(accounts)) { section in
                VStack(alignment: .leading, spacing: roomy ? 14 : 9) {
                    Text(section.provider.title)
                        .font(.system(size: 11, weight: .semibold))
                        .foregroundStyle(.secondary)
                    ForEach(section.accounts) { AccountRow(account: $0, date: date, roomy: roomy, showsRing: showsRings) }
                }
            }
        }
    }
}

struct AccountRow: View {
    let account: UsageAccount
    let date: Date
    let roomy: Bool
    let showsRing: Bool

    var body: some View {
        HStack(alignment: .center, spacing: roomy ? 14 : 12) {
            if showsRing {
                AccountRing(account: account, diameter: roomy ? 48 : 36)
            }
            VStack(alignment: .leading, spacing: roomy ? 6 : 4) {
                Text(account.name)
                    .font(.system(size: roomy ? 13 : 12, weight: .semibold))
                    .lineLimit(1)
                WindowList(account: account, date: date, compact: !roomy)
            }
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(accessibilityDescription(account, at: date))
    }
}

// MARK: - Extra large

/// A ring overview across the top, with every window's details in two columns below.
struct ExtraLargeUsageView: View {
    let snapshot: UsageSnapshot
    let date: Date

    var body: some View {
        let rings = Array(snapshot.accounts.prefix(8))
        let shown = accountsFitting(snapshot.accounts, lines: 18)
        let columns = Self.columns(shown)
        VStack(alignment: .leading, spacing: 0) {
            UsageHeader(snapshot: snapshot, date: date)
            HStack(spacing: 0) {
                ForEach(rings) { account in
                    AccountRingCell(
                        account: account,
                        date: date,
                        diameter: rings.count > 6 ? 54 : 62,
                        caption: rings.count > 6 ? .compact : .full
                    )
                    .frame(maxWidth: .infinity)
                }
            }
            .padding(.top, 14)
            Divider()
                .padding(.vertical, 14)
            HStack(alignment: .top, spacing: 32) {
                ForEach(Array(columns.enumerated()), id: \.offset) { _, column in
                    AccountSections(accounts: column, date: date, roomy: false, showsRings: false)
                        .frame(maxWidth: .infinity, alignment: .topLeading)
                }
            }
            Spacer(minLength: 0)
            MoreAccountsNote(count: snapshot.accounts.count - max(rings.count, shown.count))
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    /// Splits accounts into two columns of similar height, keeping their order. With
    /// exactly two providers, each gets its own column when that fits.
    static func columns(_ accounts: [UsageAccount]) -> [[UsageAccount]] {
        let sections = providerSections(accounts)
        if sections.count == 2,
           sections.allSatisfy({ section in section.accounts.reduce(1) { $0 + detailLines($1) } <= 9 }) {
            return sections.map(\.accounts)
        }
        let total = accounts.reduce(0) { $0 + detailLines($1) }
        var left: [UsageAccount] = []
        var leftLines = 0
        for account in accounts where leftLines < (total + 1) / 2 {
            left.append(account)
            leftLines += detailLines(account)
        }
        return [left, Array(accounts.dropFirst(left.count))].filter { !$0.isEmpty }
    }
}

// MARK: - Empty

struct EmptyUsageView: View {
    let hasSnapshot: Bool

    var body: some View {
        VStack(spacing: 6) {
            Image(systemName: "gauge.with.dots.needle.33percent")
                .font(.system(size: 26, weight: .medium))
                .foregroundStyle(.secondary)
            Text(hasSnapshot ? "No Accounts" : "No Usage Yet")
                .font(.system(size: 13, weight: .semibold))
            Text(hasSnapshot ? "Add an account in Comradex." : emptyHint)
                .font(.system(size: 11))
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}
