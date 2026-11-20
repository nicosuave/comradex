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
    private let fableLimit = #"""
    {"id":"7d:model:fable","label":"Fable weekly","short_label":"Fable","scope":{"kind":"model","name":"Fable"},
     "window_seconds":604800,"used_percent":%d,"resets_at_unix":1792454399,"affects_routing":false}
    """#

    private func fableAccount(_ used: Int?) throws -> UsageAccount {
        let scoped = used.map { "," + String(format: fableLimit, $0) } ?? ""
        let json = """
        {"accounts":[{"name":"claude","kind":"claude_home","signed_in":true,
          "usage_windows":{"7d":{"used_percent":10,"limit_window_seconds":604800}},
          "usage_limits":[{"id":"7d","label":"Weekly","short_label":"7d","scope":{"kind":"account"},
            "window_seconds":604800,"used_percent":10,"affects_routing":true}\(scoped)]}],"pools":[]}
        """
        return try XCTUnwrap(UsageSnapshot(status: status(json), publishedAt: Date()).accounts.first)
    }

    func testScopedLimitsStaySeparateFromSharedQuota() throws {
        let account = try fableAccount(25)
        XCTAssertEqual(account.scopedWindows.map(\.remainingPercent), [75])
        XCTAssertEqual(account.scopedWindows.first?.title, "Fable weekly")
        XCTAssertEqual(account.scopedWindows.first?.shortTitle, "Fable")
        XCTAssertNotNil(account.scopedWindows.first?.resetsAt)
        XCTAssertEqual(account.windows.map(\.title), ["Weekly"])
        XCTAssertEqual(account.remainingPercent, 90)
        XCTAssertEqual(account.visibleScopedWindow, account.scopedWindows.first)
        XCTAssertEqual(account.visibleWindows.map(\.shortTitle), ["7d", "Fable"])
        XCTAssertEqual(accessibilityDescription(account), "claude, Claude, Weekly 90 percent left, Fable weekly 75 percent left")

        let exhausted = try fableAccount(100)
        XCTAssertEqual(exhausted.scopedWindows.first?.remainingPercent, 0)
        XCTAssertEqual(exhausted.status, .available)
        XCTAssertEqual(exhausted.remainingPercent, 90)
    }

    func testAccountsWithoutScopedLimitsKeepASingleGauge() throws {
        let account = try fableAccount(nil)
        XCTAssertTrue(account.scopedWindows.isEmpty)
        XCTAssertNil(account.visibleScopedWindow)
        XCTAssertEqual(account.remainingPercent, 90)

        var hidden = try fableAccount(25)
        for state in [UsageAccount.Status.needsSignIn, .signingIn, .unavailable] {
            hidden.status = state
            XCTAssertNil(hidden.visibleScopedWindow)
            XCTAssertFalse(accessibilityDescription(hidden).contains("Fable"))
        }
        hidden.status = .limited
        XCTAssertNotNil(hidden.visibleScopedWindow)
    }

    @MainActor
    func testScopedLimitChangesPublishAndRespectWidgetReloadBudget() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = UsageSnapshotStore(directory: directory)
        var now = Date(timeIntervalSince1970: 1_791_921_000)
        var reloads = 0
        let publisher = WidgetUsagePublisher(store: store, reloadTimelines: { reloads += 1 }, now: { now })
        func snapshot(_ percent: Int?) throws -> UIStatusSnapshot {
            let shared = #"""
            {"id":"5h","label":"5-hour","short_label":"5h","scope":{"kind":"account"},"window_seconds":18000,
             "used_percent":11,"resets_at_unix":1791921599,"affects_routing":true},
            {"id":"7d","label":"Weekly","short_label":"7d","scope":{"kind":"account"},"window_seconds":604800,
             "used_percent":7,"resets_at_unix":1792454399,"affects_routing":true}
            """#
            let limits = percent.map { #", "usage_limits":["# + shared + "," + String(format: fableLimit, $0) + "]" } ?? ""
            return try status(fixture.replacingOccurrences(of: #""usage_percent":11"#, with: #""usage_percent":11"# + limits))
        }
        publisher.publish(try snapshot(25), claudeUsage: .fiveHour)
        now += 1
        publisher.publish(try snapshot(100), claudeUsage: .fiveHour)
        XCTAssertEqual(store.load()?.accounts.last?.scopedWindows.first?.remainingPercent, 0)
        XCTAssertEqual(reloads, 1)
        now += WidgetUsagePublisher.usageReloadInterval
        publisher.publish(try snapshot(100), claudeUsage: .fiveHour)
        XCTAssertEqual(reloads, 2)
        now += WidgetUsagePublisher.structuralReloadInterval
        publisher.publish(try snapshot(nil), claudeUsage: .fiveHour)
        XCTAssertEqual(store.load()?.accounts.last?.scopedWindows, [])
        XCTAssertEqual(reloads, 3)
    }

    func testWidgetSnapshotsDecodeAcrossVersions() throws {
        let bare = Data(#"{"publishedAt":0,"accounts":[{"name":"claude","provider":"claude","status":"available","windows":[]}]}"#.utf8)
        XCTAssertEqual(try JSONDecoder().decode(UsageSnapshot.self, from: bare).accounts.first?.scopedWindows, [])

        // Snapshots from older menu apps carry Fable as `fableWindow`.
        let legacy = Data(#"{"publishedAt":0,"accounts":[{"name":"claude","provider":"claude","status":"available","windows":[],"fableWindow":{"duration":604800,"usedPercent":30}}]}"#.utf8)
        let fable = try XCTUnwrap(try JSONDecoder().decode(UsageSnapshot.self, from: legacy).accounts.first?.scopedWindows.first)
        XCTAssertEqual(fable.title, "Fable weekly")
        XCTAssertEqual(fable.remainingPercent, 70)

        // New snapshots still write `fableWindow` for older iPhone apps.
        let written = try JSONSerialization.jsonObject(with: JSONEncoder().encode(UsageSnapshot.sample(at: Date())))
        let accounts = try XCTUnwrap((written as? [String: Any])?["accounts"] as? [[String: Any]])
        XCTAssertNotNil(accounts.last?["fableWindow"])
    }

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
        XCTAssertEqual(accountCaption(available), "")
        XCTAssertEqual(accountCaption(available, compact: true), "")
        var limited = available
        limited.status = .limited
        limited.retryAt = now.addingTimeInterval(3_600)
        XCTAssertEqual(accountCaption(limited, compact: true), "Limit reached")
        XCTAssertEqual(accessibilityDescription(limited), "a, Codex, Weekly 75 percent left, Limit reached")
        XCTAssertEqual(limited.remainingPercent, 75)
        let pending = UsageAccount(name: "a", provider: .codex, status: .available, windows: [])
        XCTAssertEqual(accountCaption(pending), "Usage pending")
        XCTAssertNil(pending.remainingPercent)
    }

    func testMediumCountdownUsesSelectedQuotaAndHidesStaleAuthenticationData() {
        let now = Date(timeIntervalSince1970: 0)
        var account = UsageAccount(name: "claude", provider: .claude, status: .available, windows: [
            UsageWindow(duration: 604_800, usedPercent: 25, resetsAt: now.addingTimeInterval(3_600)),
        ], scopedWindows: [UsageWindow(duration: 604_800, usedPercent: 50, resetsAt: now.addingTimeInterval(7_200))])
        XCTAssertEqual(accountResetCountdown(account, at: now), "1h 0m")
        XCTAssertNil(accountResetCountdown(account, at: now.addingTimeInterval(3_600)))
        account.status = .limited
        account.windows[0].usedPercent = 100
        XCTAssertEqual(accountResetCountdown(account, at: now), "1h 0m")
        account.windows = []
        account.retryAt = now.addingTimeInterval(45 * 60)
        XCTAssertEqual(accountResetCountdown(account, at: now), "45m")
        for status in [UsageAccount.Status.needsSignIn, .signingIn, .unavailable] {
            account.status = status
            XCTAssertNil(accountResetCountdown(account, at: now))
        }
        account.status = .available
        account.retryAt = nil
        XCTAssertNil(accountResetCountdown(account, at: now))
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

        var fableAccounts = (0..<8).map { account("claude-\($0)", .claude, windows: 1) }
        for index in fableAccounts.indices {
            fableAccounts[index].scopedWindows = [UsageWindow(duration: 604_800, usedPercent: 25, resetsAt: nil)]
        }
        // Each account lists its name, its shared limit, and its Fable limit.
        XCTAssertEqual(accountsFitting(fableAccounts, lines: 15, showsRings: true).count, 4)
        XCTAssertEqual(accountsFitting(fableAccounts, lines: 18).count, 5)
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
