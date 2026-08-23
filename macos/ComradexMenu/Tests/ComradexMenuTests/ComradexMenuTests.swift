import Darwin
import XCTest
@testable import ComradexMenu

final class ComradexMenuTests: XCTestCase {
    func testStatusIconIsAValidTemplateImage() {
        XCTAssertTrue(StatusIcon.image.isValid)
        XCTAssertTrue(StatusIcon.image.isTemplate)
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
    func startLogin(account: String) async throws -> LoginSnapshot { LoginSnapshot(account: account, state: .running) }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { LoginSnapshot(account: "work", sessionID: sessionID, state: .succeeded) }
}

private struct FailingClient: ControlServing {
    func status() async throws -> UIStatusSnapshot { throw ControlSocketError.daemon("unavailable") }
    func setPreferred(pool: String, account: String?) async throws -> UIStatusSnapshot? { throw ControlSocketError.daemon("unavailable") }
    func startLogin(account: String) async throws -> LoginSnapshot { throw ControlSocketError.daemon("unavailable") }
    func loginStatus(sessionID: String) async throws -> LoginSnapshot { throw ControlSocketError.daemon("unavailable") }
}
