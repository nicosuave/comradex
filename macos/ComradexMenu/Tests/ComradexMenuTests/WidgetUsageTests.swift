import XCTest
@testable import ComradexMenu
@testable import ComradexWidgetCore

private func status(_ json: String) throws -> UIStatusSnapshot {
    try JSONDecoder().decode(UIStatusSnapshot.self, from: Data(json.utf8))
}

private let fixture = #"""
{"daemon_running":true,"accounts":[
  {"name":"claude","kind":"claude_home","signed_in":true,"auth_state":"signed_in","pools":["claude"],"usage_percent":11,
   "usage_windows":{"7d":{"used_percent":7,"reset_at_unix":1792454399,"limit_window_seconds":604800},
                    "5h":{"used_percent":11,"reset_at_unix":1791921599,"limit_window_seconds":18000}}},
  {"name":"app","kind":"inbound","signed_in":true,"auth_state":"inbound","pools":["default"]},
  {"name":"sq","kind":"codex_home","signed_in":true,"auth_state":"signed_in","pools":["default"],"usage_percent":25,
   "usage_windows":{"primary":{"used_percent":25,"reset_at_unix":1792504759,"limit_window_seconds":604800},
                    "secondary":{"used_percent":0,"reset_at_unix":1791921539,"limit_window_seconds":0}}},
  {"name":"work","kind":"codex_home","signed_in":true,"auth_state":"signed_in","pools":["default"],"available":false,
   "unavailable_reason":"quota","retry_at_unix":1791925000,
   "usage_windows":{"primary":{"used_percent":100,"reset_at_unix":1791924000,"limit_window_seconds":18000}}},
  {"name":"spare","kind":"codex_home","signed_in":false,"auth_state":"signed_out","pools":[],"usage_percent":40}
],"pools":[{"name":"claude","members":["claude"]},{"name":"default","members":["app","work","sq"]}]}
"""#

final class WidgetUsageTests: XCTestCase {
    func testSnapshotListsUsageAccountsCodexFirstWithoutInboundLogins() throws {
        let snapshot = UsageSnapshot(status: try status(fixture), publishedAt: Date(timeIntervalSince1970: 0))

        XCTAssertEqual(snapshot.accounts.map(\.name), ["work", "sq", "spare", "claude"])
        XCTAssertEqual(snapshot.accounts.map(\.provider), [.codex, .codex, .codex, .claude])
    }

    func testWindowsAreOrderedShortestFirstAndOmitZeroDurationPlaceholders() throws {
        let accounts = UsageSnapshot(status: try status(fixture), publishedAt: Date()).accounts
        let claude = try XCTUnwrap(accounts.first { $0.name == "claude" })
        let sq = try XCTUnwrap(accounts.first { $0.name == "sq" })

        XCTAssertEqual(claude.windows.map(\.title), ["5-hour", "Weekly"])
        XCTAssertEqual(claude.windows.map(\.shortTitle), ["5h", "7d"])
        XCTAssertEqual(claude.remainingPercent, 89)
        XCTAssertEqual(sq.windows.map(\.remainingPercent), [75])
    }

    func testStatesMatchTheMenu() throws {
        let accounts = UsageSnapshot(status: try status(fixture), publishedAt: Date()).accounts
        let work = try XCTUnwrap(accounts.first { $0.name == "work" })
        let spare = try XCTUnwrap(accounts.first { $0.name == "spare" })

        XCTAssertEqual(work.status, .limited)
        XCTAssertEqual(work.remainingPercent, 0)
        // A limited account returns when its exhausted window resets, not at the retry hint.
        XCTAssertEqual(work.resetDate(at: Date(timeIntervalSince1970: 1_791_920_000)), Date(timeIntervalSince1970: 1_791_924_000))
        XCTAssertEqual(spare.status, .needsSignIn)
        // Without windows, the aggregate percentage still yields a meter.
        XCTAssertEqual(spare.windows, [UsageWindow(duration: nil, usedPercent: 40, resetsAt: nil)])

        let renewing = try JSONDecoder().decode(AccountSnapshot.self, from: Data(
            #"{"name":"a","kind":"codex_home","signed_in":true,"auth_state":"signed_in","reauth_required":true}"#.utf8))
        XCTAssertEqual(UsageAccount(account: renewing).status, .needsSignIn)
        let loggingIn = try JSONDecoder().decode(AccountSnapshot.self, from: Data(
            #"{"name":"a","kind":"claude_home","signed_in":false,"auth_state":"login_in_progress"}"#.utf8))
        XCTAssertEqual(UsageAccount(account: loggingIn).status, .signingIn)
        XCTAssertEqual(UsageAccount(account: loggingIn).provider, .claude)
    }

    func testLimitingWindowPrefersTheShorterWindowOnTies() {
        let account = UsageAccount(name: "a", provider: .claude, status: .available, windows: [
            UsageWindow(duration: 18_000, usedPercent: 40, resetsAt: Date(timeIntervalSince1970: 100)),
            UsageWindow(duration: 604_800, usedPercent: 40, resetsAt: Date(timeIntervalSince1970: 200)),
        ])
        XCTAssertEqual(account.limitingWindow?.duration, 18_000)
        XCTAssertEqual(account.resetDate(at: Date(timeIntervalSince1970: 0)), Date(timeIntervalSince1970: 100))
        XCTAssertNil(account.resetDate(at: Date(timeIntervalSince1970: 150)))
    }

    func testCountdownsAndCaptions() {
        let now = Date(timeIntervalSince1970: 0)
        XCTAssertEqual(compactCountdown(until: now.addingTimeInterval(30), from: now), "<1m")
        XCTAssertEqual(compactCountdown(until: now.addingTimeInterval(52 * 60), from: now), "52m")
        XCTAssertEqual(compactCountdown(until: now.addingTimeInterval(2 * 3_600 + 61), from: now), "2h 1m")
        XCTAssertEqual(compactCountdown(until: now.addingTimeInterval(6 * 86_400 + 18 * 3_600), from: now), "6d 18h")

        let available = UsageAccount(name: "a", provider: .codex, status: .available, windows: [
            UsageWindow(duration: 604_800, usedPercent: 25, resetsAt: now.addingTimeInterval(3_600)),
        ])
        XCTAssertEqual(accountCaption(available, at: now), "Resets in 1h 0m")
        XCTAssertEqual(accountCaption(available, at: now, compact: true), "1h 0m")
        var limited = available
        limited.status = .limited
        limited.retryAt = now.addingTimeInterval(3_600)
        XCTAssertEqual(accountCaption(limited, at: now, compact: true), "Back in 1h 0m")
        XCTAssertEqual(limited.remainingPercent, 75)
        let pending = UsageAccount(name: "a", provider: .codex, status: .available, windows: [])
        XCTAssertEqual(accountCaption(pending, at: now), "Usage pending")
        XCTAssertNil(pending.remainingPercent)
    }

    func testDetailLayoutsKeepWholeAccountsWithinTheirLineBudget() {
        func account(_ name: String, _ provider: UsageProvider, windows: Int) -> UsageAccount {
            UsageAccount(name: name, provider: provider, status: .available, windows: Array(
                repeating: UsageWindow(duration: 18_000, usedPercent: 10, resetsAt: nil), count: windows))
        }
        let accounts = [account("a", .codex, windows: 1), account("b", .codex, windows: 2), account("c", .claude, windows: 2)]

        // Heading + 2 lines + 3 lines, then the Claude heading and account would need 4 more.
        XCTAssertEqual(accountsFitting(accounts, lines: 9).map(\.name), ["a", "b"])
        XCTAssertEqual(accountsFitting(accounts, lines: 10).map(\.name), ["a", "b", "c"])
        XCTAssertEqual(ExtraLargeUsageView.columns(accounts).map { $0.map(\.name) }, [["a", "b"], ["c"]])
    }

    func testStoreRoundTripsThroughItsDirectoryAndNoGroupMeansNoFile() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = UsageSnapshotStore(directory: directory)
        let snapshot = UsageSnapshot.sample(at: Date(timeIntervalSince1970: 1_791_921_300))

        try store.save(snapshot)

        XCTAssertEqual(store.load(), snapshot)
        XCTAssertNil(UsageSnapshotStore(bundle: Bundle(for: Self.self)).fileURL)
    }

    @MainActor
    func testPublisherWritesChangesButThrottlesWidgetReloads() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = UsageSnapshotStore(directory: directory)
        var now = Date(timeIntervalSince1970: 1_791_921_000)
        var reloads = 0
        let publisher = WidgetUsagePublisher(store: store, reloadTimelines: { reloads += 1 }, now: { now })
        let base = try status(fixture)

        publisher.publish(base, claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 1, "The first snapshot reloads immediately")
        XCTAssertEqual(store.load()?.accounts.count, 4)

        // Unchanged usage, and reset times that only jitter, neither rewrite nor reload.
        now += 5
        let jittered = try status(fixture.replacingOccurrences(of: "1792504759", with: "1792504790"))
        publisher.publish(jittered, claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 1)
        XCTAssertEqual(store.load()?.publishedAt, Date(timeIntervalSince1970: 1_791_921_000))

        // Usage drift is written at once but waits for the usage reload interval.
        now += 5
        let drifted = try status(fixture.replacingOccurrences(of: #""used_percent":25"#, with: #""used_percent":31"#))
        publisher.publish(drifted, claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 1)
        XCTAssertEqual(store.load()?.accounts.first { $0.name == "sq" }?.remainingPercent, 69)
        now = Date(timeIntervalSince1970: 1_791_921_000 + WidgetUsagePublisher.usageReloadInterval)
        publisher.publish(drifted, claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 2, "A later poll flushes the deferred reload")

        // A state change reloads after the shorter structural interval.
        let signedOut = try status(fixture.replacingOccurrences(of: #""name":"sq","kind":"codex_home","signed_in":true,"auth_state":"signed_in""#,
                                                                with: #""name":"sq","kind":"codex_home","signed_in":false,"auth_state":"signed_out""#))
        now += 1
        publisher.publish(signedOut, claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 2)
        now += WidgetUsagePublisher.structuralReloadInterval
        publisher.publish(signedOut, claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 3)

        // Unchanged usage is rewritten periodically so the widget can tell it is fresh.
        now += WidgetUsagePublisher.heartbeatInterval
        publisher.publish(signedOut, claudeUsage: .fiveHour)
        XCTAssertEqual(store.load()?.publishedAt, now)
        XCTAssertEqual(reloads, 3)
    }

    @MainActor
    func testClaudeUsageChoiceSetsTheWidgetSummaryLikeTheMenu() throws {
        let fiveHour = UsageSnapshot(status: try status(fixture), publishedAt: Date(), claudeUsage: .fiveHour).accounts
        let weekly = UsageSnapshot(status: try status(fixture), publishedAt: Date(), claudeUsage: .weekly).accounts
        func named(_ name: String, _ accounts: [UsageAccount]) throws -> UsageAccount {
            try XCTUnwrap(accounts.first { $0.name == name })
        }

        XCTAssertEqual(try named("claude", fiveHour).summaryWindow?.title, "5-hour")
        XCTAssertEqual(try named("claude", fiveHour).remainingPercent, 89)
        XCTAssertEqual(try named("claude", weekly).summaryWindow?.title, "Weekly")
        XCTAssertEqual(try named("claude", weekly).remainingPercent, 93)
        XCTAssertEqual(
            try named("claude", weekly).resetDate(at: Date(timeIntervalSince1970: 1_791_920_000)),
            Date(timeIntervalSince1970: 1_792_454_399)
        )
        // Codex accounts keep their primary window regardless of the Claude choice.
        XCTAssertEqual(try named("sq", weekly).summaryWindow?.remainingPercent, 75)
        XCTAssertEqual(WidgetUsagePublisher.change(
            from: UsageSnapshot(publishedAt: Date(), accounts: fiveHour),
            to: UsageSnapshot(publishedAt: Date(), accounts: weekly)
        ), .structural)
    }

    func testSnapshotsWithoutASummaryUseTheMostConstrainedWindow() {
        let account = UsageAccount(name: "a", provider: .claude, status: .available, windows: [
            UsageWindow(duration: 18_000, usedPercent: 10, resetsAt: nil),
            UsageWindow(duration: 604_800, usedPercent: 70, resetsAt: nil),
        ])
        XCTAssertEqual(account.remainingPercent, 30)
    }

    @MainActor
    func testChangingClaudeUsageReloadsTheWidgetAtOnce() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        var now = Date(timeIntervalSince1970: 1_791_921_000)
        var reloads = 0
        let publisher = WidgetUsagePublisher(
            store: UsageSnapshotStore(directory: directory), reloadTimelines: { reloads += 1 }, now: { now })
        let snapshot = try status(fixture)

        publisher.publish(snapshot, claudeUsage: .fiveHour)
        now += 1
        publisher.publish(snapshot, claudeUsage: .weekly, userInitiated: true)

        XCTAssertEqual(reloads, 2)
        XCTAssertEqual(UsageSnapshotStore(directory: directory).load()?.accounts.last?.remainingPercent, 93)
    }

    @MainActor
    func testPublisherWithoutAppGroupDoesNothing() throws {
        var reloads = 0
        let publisher = WidgetUsagePublisher(store: UsageSnapshotStore(directory: nil), reloadTimelines: { reloads += 1 })
        publisher.publish(try status(fixture), claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 0)
        XCTAssertNil(publisher.published)
    }
}
