import Darwin
import XCTest
@testable import ComradexMenu

final class ComradexMenuTests: XCTestCase {
    func testStatusIconIsAValidTemplateImage() {
        XCTAssertTrue(StatusIcon.image.isValid)
        XCTAssertTrue(StatusIcon.image.isTemplate)
        XCTAssertEqual(StatusIcon.image.size, NSSize(width: 19, height: 13))
    }

    @MainActor
    @available(macOS 14.4, *)
    func testNativeMenuRendersAccountsAndActionableCommands() throws {
        let data = Data(#"""
        {
          "ok": true,
          "status": {
            "daemon_running": true,
            "accounts": [
              {"name":"app","kind":"inbound","signed_in":true,"auth_state":"inbound","pools":["default"]},
              {"name":"sq","kind":"codex_home","signed_in":true,"auth_state":"signed_in","pools":["default"],"usage_percent":81,"usage_updated_at_unix":1788800000,"usage_windows":{"primary":{"used_percent":19,"reset_at_unix":4102444800,"limit_window_seconds":18000},"secondary":{"used_percent":81,"reset_at_unix":4103049600,"limit_window_seconds":604800}}},
              {"name":"bad","kind":"codex_home","signed_in":true,"auth_state":"signed_in","pools":["default"],"available":false,"unavailable_reason":"needs_login","usage_windows":{"primary":{"used_percent":10,"reset_at_unix":4102444800,"limit_window_seconds":18000}}}
            ],
            "pools": [{"name":"default","members":["app","sq","bad"],"preferred":"app","active":"sq"}]
          }
        }
        """#.utf8)
        let snapshot = try ControlSocketClient.decode(
            UIStatusSnapshot.self,
            from: data,
            preferredKeys: ["status"]
        )
        let store = ComradexStore(client: StubClient())
        store.apply(status: snapshot)
        let controller = MenuBarController(store: store)

        controller.rebuildMenu()

        let items = controller.renderedMenu.items
        XCTAssertFalse(controller.renderedMenu.autoenablesItems)
        XCTAssertFalse(items.contains { $0.title.contains("default") || $0.title.contains("Active:") })
        let app = try XCTUnwrap(items.first(where: { $0.title == "app · Requesting client’s login" }))
        let connect = try XCTUnwrap(items.first(where: { $0.title == "Connect existing Codex login…" }))
        XCTAssertEqual(connect.representedObject as? String, "app")
        XCTAssertTrue(connect.isEnabled)
        XCTAssertNotNil(connect.action)
        XCTAssertEqual(items.filter { $0.title == "Connect existing Codex login…" }.count, 1)
        XCTAssertEqual(app.state, .on)
        XCTAssertNil(app.image)
        XCTAssertNil(app.subtitle)
        XCTAssertEqual(app.toolTip, "Preferred · Requesting client’s login")
        let sq = try XCTUnwrap(items.first(where: { $0.title.hasPrefix("sq · 81% left · ") }))
        XCTAssertFalse(sq.title.contains("resets in"))
        XCTAssertFalse(sq.title.contains("19%"))
        XCTAssertNotNil(sq.action)
        XCTAssertEqual(sq.state, .off)
        XCTAssertTrue(sq.image?.accessibilityDescription?.contains("Last used") == true)
        XCTAssertNil(sq.subtitle)
        XCTAssertTrue(sq.toolTip?.contains("resets in") == true)
        let bad = try XCTUnwrap(items.first(where: { $0.title == "bad · Sign-in required" }))
        XCTAssertNil(bad.subtitle)
        XCTAssertNil(bad.image)
        XCTAssertEqual(snapshot.accounts.first(where: { $0.name == "sq" })?.usageUpdatedAtUnix, 1788800000)
        XCTAssertEqual(snapshot.accounts.first(where: { $0.name == "sq" })?.usageWindows["secondary"]?.resetAtUnix, 4103049600)
        XCTAssertEqual(items.first(where: { $0.title == "Refresh" })?.keyEquivalent, "r")
        XCTAssertNotNil(items.first(where: { $0.title == "Refresh" })?.action)
        XCTAssertEqual(items.first(where: { $0.title == "Quit Comradex" })?.keyEquivalent, "q")
        XCTAssertNotNil(items.first(where: { $0.title == "Quit Comradex" })?.action)
    }

    @MainActor
    @available(macOS 14.4, *)
    func testAccountRowsStayCompactAcrossUsageAndAvailabilityStates() throws {
        let cases: [(String, String)] = [
            (#""usage_windows":{"primary":{"used_percent":0,"reset_at_unix":0,"limit_window_seconds":0},"secondary":{"used_percent":31,"limit_window_seconds":604800}}"#, "69% left"),
            (#""usage_windows":{"primary":{"used_percent":32,"limit_window_seconds":604800},"secondary":{"used_percent":0,"limit_window_seconds":0}}"#, "68% left"),
            (#""usage_windows":{"primary":{"used_percent":32},"secondary":{"used_percent":10,"limit_window_seconds":604800}}"#, "68% left"),
            (#""usage_percent":31"#, "69% left"),
            (#""usage_windows":{}"#, "Usage pending"),
            (#""available":false,"unavailable_reason":"quota""#, "0% left"),
            (#""available":false,"unavailable_reason":"temporary_failure""#, "Temporarily unavailable"),
            (#""auth_state":"login_in_progress""#, "Login in progress"),
            (#""auth_state":"signed_out","signed_in":false"#, "Sign-in required"),
            (#""reauth_required":true"#, "Sign-in needed for renewal"),
            (#""available":false,"unavailable_reason":"unknown""#, "Unavailable"),
        ]
        for (fields, detail) in cases {
            let account = try decodeAccount("{\"name\":\"work\",\"kind\":\"codex_home\",\(fields)}")
            let store = ComradexStore(client: StubClient())
            store.apply(status: UIStatusSnapshot(accounts: [account], pools: [
                PoolSnapshot(name: "default", members: ["work"], preferred: "work", active: "work")
            ]))
            let controller = MenuBarController(store: store)
            controller.rebuildMenu()
            let item = try XCTUnwrap(controller.renderedMenu.items.first { $0.title == "work · \(detail)" })
            XCTAssertNil(item.subtitle)
            XCTAssertEqual(item.state, .on)
            XCTAssertFalse(item.toolTip?.contains("0d") == true)
            XCTAssertFalse(item.toolTip?.contains("resets in 0s") == true)
        }
    }

    @MainActor
    func testMainQuotaShowsItsOwnCompactResetCountdown() throws {
        let reset = Int64(Date().timeIntervalSince1970) + 6 * 86_400 + 4 * 3_600 + 120
        let account = try decodeAccount("""
        {"name":"sq","usage_windows":{
          "primary":{"used_percent":32,"limit_window_seconds":604800,"reset_at_unix":\(reset)},
          "secondary":{"used_percent":0,"limit_window_seconds":0,"reset_at_unix":0}
        }}
        """)
        let store = ComradexStore(client: StubClient())
        store.apply(status: UIStatusSnapshot(accounts: [account]))
        let controller = MenuBarController(store: store)
        controller.rebuildMenu()
        XCTAssertTrue(controller.renderedMenu.items.contains {
            $0.title == "sq · 68% left · 6d 4h"
        })
    }

    @MainActor
    func testExhaustedQuotaKeepsPercentageAndResetCountdown() throws {
        let reset = Int64(Date().timeIntervalSince1970) + 5 * 86_400 + 12 * 3_600 + 120
        let cases = [
            "\"usage_windows\":{\"primary\":{\"used_percent\":100,\"reset_at_unix\":\(reset),\"limit_window_seconds\":604800}}",
            "\"usage_windows\":{\"primary\":{\"used_percent\":30,\"limit_window_seconds\":18000},\"secondary\":{\"used_percent\":100,\"reset_at_unix\":\(reset),\"limit_window_seconds\":604800}}",
            "\"retry_at_unix\":\(reset)",
            "\"usage_windows\":{\"primary\":{\"used_percent\":100,\"reset_at_unix\":1}},\"retry_at_unix\":\(reset)",
        ]
        for fields in cases {
            let account = try decodeAccount("{\"name\":\"pm\",\"available\":false,\"unavailable_reason\":\"quota\",\(fields)}")
            let store = ComradexStore(client: StubClient())
            store.apply(status: UIStatusSnapshot(accounts: [account]))
            let controller = MenuBarController(store: store)
            controller.rebuildMenu()
            let item = try XCTUnwrap(controller.renderedMenu.items.first { $0.title == "pm · 0% left · 5d 12h" })
            XCTAssertTrue(item.toolTip?.contains("Rate limited") == true)
        }
    }

    @MainActor
    func testNativeMenuExplainsPoolWithNoAccounts() {
        let store = ComradexStore(client: StubClient())
        store.apply(status: UIStatusSnapshot(
            daemonRunning: true,
            pools: [PoolSnapshot(name: "default", members: [], preferred: nil, active: nil)]
        ))
        let controller = MenuBarController(store: store)

        controller.rebuildMenu()

        XCTAssertTrue(controller.renderedMenu.items.contains { $0.title == "No accounts configured" })
    }

    func testStatusDecodesFromEnvelopeAndIgnoresExtraTrafficFields() throws {
        let data = Data(#"""
        {
          "ok": true,
          "status": {
            "daemon_running": true,
            "codex_routed": false,
            "accounts": [{"name":"work","kind":"codex","signed_in":true,"pools":["default"]}],
            "pools": [{"name":"default","members":["work"],"preferred":null,"active":"work"}],
            "requests": 42
          }
        }
        """#.utf8)
        let value = try ControlSocketClient.decode(
            UIStatusSnapshot.self,
            from: data,
            preferredKeys: ["status", "ui_status", "payload", "result"]
        )
        XCTAssertEqual(value.daemonRunning, true)
        XCTAssertEqual(value.accounts.first?.name, "work")
        XCTAssertEqual(value.pools.first?.active, "work")
    }

    func testLoginActionOnlyAppearsForSignedOutManagedAccounts() throws {
        let signedIn = try decodeAccount(#"{"name":"healthy","kind":"codex_home","signed_in":true,"auth_state":"signed_in"}"#)
        let signedOut = try decodeAccount(#"{"name":"broken","kind":"codex_home","signed_in":false,"auth_state":"signed_out"}"#)
        let inbound = try decodeAccount(#"{"name":"app","kind":"inbound","signed_in":true,"auth_state":"inbound"}"#)
        let inProgress = try decodeAccount(#"{"name":"pending","kind":"codex_home","signed_in":false,"auth_state":"login_in_progress"}"#)
        let unknown = try decodeAccount(#"{"name":"legacy","kind":"codex_home","signed_in":false}"#)

        XCTAssertFalse(signedIn.needsLoginAction)
        XCTAssertTrue(signedOut.needsLoginAction)
        XCTAssertFalse(inbound.needsLoginAction)
        XCTAssertFalse(inProgress.needsLoginAction)
        XCTAssertFalse(unknown.needsLoginAction)
    }

    func testLoginDecodesOnlyAllowlistedDeviceFlowFields() throws {
        let data = Data(#"{"ok":true,"account":"work","session_id":"random","state":"running","verification_uri":"https://auth.openai.com/codex/device","user_code":"ABCD-EFGH","output":"must not be decoded"}"#.utf8)
        let value = try ControlSocketClient.decode(
            LoginSnapshot.self,
            from: data,
            preferredKeys: ["login", "login_status", "payload", "result"]
        )
        XCTAssertEqual(value.state, .running)
        XCTAssertEqual(value.sessionID, "random")
        XCTAssertEqual(value.userCode, "ABCD-EFGH")
        XCTAssertEqual(value.safeVerificationURL.host, "auth.openai.com")
    }

    func testSetPreferredEncodesAccountAndAutomaticAsExplicitNull() throws {
        let preferred = try XCTUnwrap(try JSONSerialization.jsonObject(
            with: UIControlCommand.setPreferred(pool: "default", account: "work").encoded()
        ) as? [String: Any])
        XCTAssertEqual(preferred["command"] as? String, "ui_set_preferred")
        XCTAssertEqual(preferred["pool"] as? String, "default")
        XCTAssertEqual(preferred["account"] as? String, "work")

        let automatic = try XCTUnwrap(try JSONSerialization.jsonObject(
            with: UIControlCommand.setPreferred(pool: "default", account: nil).encoded()
        ) as? [String: Any])
        XCTAssertTrue(automatic["account"] is NSNull)
    }

    func testConnectCommandIncludesOnlyAccount() throws {
        let object = try XCTUnwrap(try JSONSerialization.jsonObject(
            with: UIControlCommand.connectExistingLogin(account: "app").encoded()
        ) as? [String: String])
        XCTAssertEqual(object, ["command": "ui_connect_existing_login", "account": "app"])
    }

    @MainActor
    func testConnectFailureRemainsVisibleWithoutClearingStatus() async {
        let store = ComradexStore(client: FailingClient())
        let original = UIStatusSnapshot(daemonRunning: true)
        store.apply(status: original)
        await store.connectExistingLogin(account: "app")
        XCTAssertNotNil(store.actionErrorMessage)
        XCTAssertNil(store.connectingAccount)
        XCTAssertEqual(store.snapshot, original)
        XCTAssertNil(store.errorMessage)
    }

    @MainActor
    func testConnectWaitsForManagedAccountAfterReload() async throws {
        let client = ConnectingClient()
        let store = ComradexStore(client: client)
        await store.connectExistingLogin(account: "app")
        XCTAssertNil(store.actionErrorMessage)
        XCTAssertNil(store.connectingAccount)
        XCTAssertEqual(store.snapshot?.accounts.first?.kind, "codex_home")
        let calls = await client.calls
        XCTAssertEqual(calls, 3)
    }

    func testSocketOverrideAndDefaultPath() {
        XCTAssertEqual(ControlSocketClient(environment: ["COMRADEX_CONTROL_SOCKET": "/tmp/comradex-test.sock"]).socketPath, "/tmp/comradex-test.sock")
        XCTAssertTrue(ControlSocketClient(environment: [:]).socketPath.hasSuffix("/.config/comradex/state/control.sock"))
    }

    @MainActor
    func testStoreAppliesStatusAndLoginState() {
        let store = ComradexStore(client: StubClient())
        let status = UIStatusSnapshot(daemonRunning: true)
        store.apply(status: status)
        XCTAssertEqual(store.snapshot, status)

        store.apply(login: LoginSnapshot(account: "work", sessionID: "session", state: .running, userCode: "CODE"))
        XCTAssertEqual(store.login?.sessionID, "session")
        XCTAssertEqual(store.login?.userCode, "CODE")
    }

    @MainActor
    func testRefreshFailurePreservesLastGoodStatus() async {
        let store = ComradexStore(client: FailingClient())
        let status = UIStatusSnapshot(daemonRunning: true)
        store.apply(status: status)

        await store.refresh()

        XCTAssertEqual(store.snapshot, status)
        XCTAssertNotNil(store.errorMessage)
    }

    @MainActor
    func testPollingRecoversWithoutMenuInteractionAndStops() async throws {
        let recovered = expectation(description: "automatic retry succeeded")
        recovered.assertForOverFulfill = false
        let client = RecoveringClient(failures: 2, onSuccess: { recovered.fulfill() })
        let store = ComradexStore(client: client)
        let controller = MenuBarController(store: store)
        let cached = UIStatusSnapshot(daemonRunning: false)
        store.apply(status: cached)
        await store.refresh()
        XCTAssertNotNil(store.errorMessage)
        XCTAssertEqual(store.snapshot, cached)
        controller.rebuildMenu()
        XCTAssertTrue(controller.renderedMenu.items.first?.toolTip?.contains("unavailable") == true)
        XCTAssertFalse(controller.renderedMenu.items.contains { $0.title == "Status may be out of date" })

        controller.startPolling(intervalNanoseconds: 10_000_000)
        controller.startPolling(intervalNanoseconds: 10_000_000)
        await fulfillment(of: [recovered], timeout: 2)
        // Allow the response to reach the main actor before inspecting rendered state.
        for _ in 0..<100 where store.errorMessage != nil {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertNil(store.errorMessage)
        XCTAssertEqual(store.snapshot?.daemonRunning, true)
        XCTAssertNotNil(store.lastSuccessfulRefresh)
        if #available(macOS 14.4, *) {
            XCTAssertEqual(controller.renderedMenu.items.first?.subtitle, "Running")
        }
        controller.stopPolling()
        let calls = await client.calls
        try await Task.sleep(nanoseconds: 40_000_000)
        let afterStop = await client.calls
        XCTAssertEqual(afterStop, calls)
    }

    @MainActor
    func testOpeningMenuRefreshesAndDefersStructuralUpdateUntilClose() async throws {
        let refreshed = expectation(description: "opening menu fetched status")
        refreshed.assertForOverFulfill = false
        let client = RecoveringClient(failures: 0, onSuccess: { refreshed.fulfill() })
        let store = ComradexStore(client: client)
        let controller = MenuBarController(store: store)
        controller.menuWillOpen(controller.renderedMenu)
        await fulfillment(of: [refreshed], timeout: 2)
        for _ in 0..<100 where store.snapshot == nil {
            try await Task.sleep(nanoseconds: 1_000_000)
        }
        XCTAssertEqual(store.snapshot?.daemonRunning, true)
        XCTAssertTrue(controller.renderedMenu.items.contains { $0.title == "Connecting…" })
        controller.menuDidClose(controller.renderedMenu)
        if #available(macOS 14.4, *) {
            XCTAssertEqual(controller.renderedMenu.items.first?.subtitle, "Running")
        }
        controller.stopPolling()
    }

    @MainActor
    func testAccountActionFailureDoesNotMarkConnectionStaleOrDisappearOnPoll() async {
        let store = ComradexStore(client: RecoveringClient(failures: 0))
        await store.refresh()
        await store.setPreferred(pool: "default", account: "missing")
        XCTAssertNil(store.errorMessage)
        XCTAssertNotNil(store.actionErrorMessage)
        await store.refresh()
        XCTAssertNil(store.errorMessage)
        XCTAssertNotNil(store.actionErrorMessage)
        let controller = MenuBarController(store: store)
        controller.rebuildMenu()
        XCTAssertTrue(controller.renderedMenu.items.contains { $0.title == "Account change failed" })
        await store.setPreferred(pool: "default", account: "work")
        XCTAssertNil(store.actionErrorMessage)
    }

    @MainActor
    func testConcurrentRefreshesShareOneInFlightRequest() async {
        let entered = expectation(description: "status request entered")
        let client = SuspendedClient(onStatus: { entered.fulfill() })
        let store = ComradexStore(client: client)
        let first = Task { await store.refresh() }
        await fulfillment(of: [entered], timeout: 2)
        await store.refresh()
        let count = await client.calls
        XCTAssertEqual(count, 1)
        await client.complete()
        await first.value
        XCTAssertFalse(store.isRefreshing)
        XCTAssertEqual(store.snapshot?.daemonRunning, true)
    }

    @MainActor
    func testRunningLoginCannotBeReplacedByAnotherAccount() {
        let store = ComradexStore(client: StubClient())
        store.apply(login: LoginSnapshot(account: "work", sessionID: "session", state: .running))

        store.beginLogin(account: "personal")

        XCTAssertEqual(store.login?.account, "work")
        XCTAssertEqual(store.login?.sessionID, "session")
        XCTAssertEqual(store.login?.state, .running)
    }

    func testUntrustedVerificationURIFallsBackToCanonicalOpenAIURL() {
        let login = LoginSnapshot(account: "work", state: .running, verificationURI: "https://example.com/phish")
        XCTAssertEqual(login.safeVerificationURL.absoluteString, "https://auth.openai.com/codex/device")
    }

    func testUnixSocketRoundTripUsesNewlineDelimitedJSON() throws {
        let path = "/tmp/comradex-menu-\(UUID().uuidString.prefix(8)).sock"
        let listener = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        XCTAssertGreaterThanOrEqual(listener, 0)
        defer {
            Darwin.close(listener)
            Darwin.unlink(path)
        }

        var address = sockaddr_un()
        let pathBytes = Array(path.utf8CString)
        let addressLength = socklen_t(MemoryLayout<sa_family_t>.size + pathBytes.count)
        address.sun_len = UInt8(addressLength)
        address.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            pathBytes.withUnsafeBytes { destination.copyBytes(from: $0) }
        }
        let bindResult = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(listener, $0, addressLength)
            }
        }
        XCTAssertEqual(bindResult, 0)
        XCTAssertEqual(Darwin.listen(listener, 1), 0)

        let served = expectation(description: "fake daemon served one request")
        DispatchQueue.global().async {
            let connection = Darwin.accept(listener, nil, nil)
            guard connection >= 0 else { return }
            defer { Darwin.close(connection) }
            var buffer = [UInt8](repeating: 0, count: 1024)
            let count = Darwin.read(connection, &buffer, buffer.count)
            if count > 0, buffer.prefix(count).last == 0x0A {
                let response = Data((#"{"ok":true,"status":{"daemon_running":true,"accounts":[],"pools":[]}}"# + "\n").utf8)
                response.withUnsafeBytes { raw in
                    if let base = raw.baseAddress { _ = Darwin.write(connection, base, raw.count) }
                }
            }
            served.fulfill()
        }

        let response = try ControlSocketClient.send(try UIControlCommand.status.encoded(), to: path)
        let decoded = try ControlSocketClient.decode(
            UIStatusSnapshot.self,
            from: response,
            preferredKeys: ["status"]
        )
        XCTAssertEqual(decoded.daemonRunning, true)
        wait(for: [served], timeout: 2)
    }

    func testUnixSocketPeerCloseReturnsAnErrorWithoutSIGPIPE() throws {
        let path = "/tmp/comradex-menu-close-\(UUID().uuidString.prefix(8)).sock"
        let listener = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        XCTAssertGreaterThanOrEqual(listener, 0)
        defer {
            Darwin.close(listener)
            Darwin.unlink(path)
        }

        var address = sockaddr_un()
        let pathBytes = Array(path.utf8CString)
        let addressLength = socklen_t(MemoryLayout<sa_family_t>.size + pathBytes.count)
        address.sun_len = UInt8(addressLength)
        address.sun_family = sa_family_t(AF_UNIX)
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            pathBytes.withUnsafeBytes { destination.copyBytes(from: $0) }
        }
        let bindResult = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(listener, $0, addressLength)
            }
        }
        XCTAssertEqual(bindResult, 0)
        XCTAssertEqual(Darwin.listen(listener, 1), 0)

        let closed = expectation(description: "fake daemon closed its peer")
        DispatchQueue.global().async {
            let connection = Darwin.accept(listener, nil, nil)
            guard connection >= 0 else { return }
            var reset = linger(l_onoff: 1, l_linger: 0)
            _ = Darwin.setsockopt(connection, SOL_SOCKET, SO_LINGER, &reset, socklen_t(MemoryLayout<linger>.size))
            Darwin.close(connection)
            closed.fulfill()
        }

        XCTAssertThrowsError(try ControlSocketClient.send(Data(repeating: 0x41, count: 64 * 1024), to: path))
        wait(for: [closed], timeout: 2)
    }

    private func decodeAccount(_ json: String) throws -> AccountSnapshot {
        try JSONDecoder().decode(AccountSnapshot.self, from: Data(json.utf8))
    }
}

private struct StubClient: ControlServing {
    func status() async throws -> UIStatusSnapshot { UIStatusSnapshot() }
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? { nil }
    func connectExistingLogin(account: String) async throws { throw ControlSocketError.daemon("unavailable") }
    func startLogin(account: String) async throws -> LoginSnapshot { LoginSnapshot(account: account, state: .running) }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { LoginSnapshot(account: "work", sessionID: sessionID, state: .succeeded) }
}

private struct FailingClient: ControlServing {
    func status() async throws -> UIStatusSnapshot { throw ControlSocketError.daemon("unavailable") }
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? { throw ControlSocketError.daemon("unavailable") }
    func connectExistingLogin(account: String) async throws { throw ControlSocketError.daemon("unavailable") }
    func startLogin(account: String) async throws -> LoginSnapshot { throw ControlSocketError.daemon("unavailable") }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { throw ControlSocketError.daemon("unavailable") }
}

private actor RecoveringClient: ControlServing {
    private(set) var calls = 0
    let failures: Int
    let onSuccess: @Sendable () -> Void
    init(failures: Int = 1, onSuccess: @escaping @Sendable () -> Void = {}) {
        self.failures = failures
        self.onSuccess = onSuccess
    }
    func status() async throws -> UIStatusSnapshot {
        calls += 1
        if calls <= failures { throw ControlSocketError.daemon("unavailable") }
        onSuccess()
        return UIStatusSnapshot(daemonRunning: true)
    }
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? {
        if account == "missing" { throw ControlSocketError.daemon("Unknown account") }
        return nil
    }
    func connectExistingLogin(account: String) async throws { throw ControlSocketError.daemon("unavailable") }
    func startLogin(account: String) async throws -> LoginSnapshot { throw ControlSocketError.emptyResponse }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { throw ControlSocketError.emptyResponse }
}

private actor SuspendedClient: ControlServing {
    private(set) var calls = 0
    let onStatus: @Sendable () -> Void
    private var continuation: CheckedContinuation<UIStatusSnapshot, Never>?
    init(onStatus: @escaping @Sendable () -> Void) { self.onStatus = onStatus }
    func status() async throws -> UIStatusSnapshot {
        calls += 1
        return await withCheckedContinuation {
            continuation = $0
            onStatus()
        }
    }
    func complete() { continuation?.resume(returning: UIStatusSnapshot(daemonRunning: true)); continuation = nil }
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? { nil }
    func connectExistingLogin(account: String) async throws { throw ControlSocketError.daemon("unavailable") }
    func startLogin(account: String) async throws -> LoginSnapshot { throw ControlSocketError.emptyResponse }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { throw ControlSocketError.emptyResponse }
}

private actor ConnectingClient: ControlServing {
    private(set) var calls = 0
    func connectExistingLogin(account: String) async throws {}
    func status() async throws -> UIStatusSnapshot {
        calls += 1
        if calls == 1 { throw ControlSocketError.emptyResponse }
        let kind = calls == 2 ? "inbound" : "codex_home"
        let account = try JSONDecoder().decode(AccountSnapshot.self, from: Data("{\"name\":\"app\",\"kind\":\"\(kind)\"}".utf8))
        return UIStatusSnapshot(daemonRunning: true, accounts: [account])
    }
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? { nil }
    func startLogin(account: String) async throws -> LoginSnapshot { throw ControlSocketError.emptyResponse }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { throw ControlSocketError.emptyResponse }
}
