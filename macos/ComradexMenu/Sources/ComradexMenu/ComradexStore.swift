import Foundation
import SwiftUI
import OSLog

@MainActor
final class ComradexStore: ObservableObject {
    @Published private(set) var snapshot: UIStatusSnapshot?
    @Published private(set) var login: LoginSnapshot?
    @Published private(set) var isRefreshing = false
    @Published private(set) var updatingPool: String?
    @Published private(set) var connectingAccount: String?
    @Published private(set) var errorMessage: String?
    @Published private(set) var actionErrorMessage: String?
    @Published private(set) var lastSuccessfulRefresh: Date?
    private let logger = Logger(subsystem: "com.nicosuave.comradex.menu", category: "connection")

    var isLoginRunning: Bool { login?.state == .running }

    private let client: any ControlServing
    private var loginTask: Task<Void, Never>?
    private var statusGeneration: UInt64 = 0

    init(client: any ControlServing = ControlSocketClient()) {
        self.client = client
    }

    deinit { loginTask?.cancel() }

    func refresh() async {
        guard !isRefreshing, updatingPool == nil else { return }
        let generation = statusGeneration
        isRefreshing = true
        defer { isRefreshing = false }
        await readStatus(generation: generation)
    }

    private func readStatus(generation: UInt64) async {
        do {
            let status = try await client.status()
            guard generation == statusGeneration else { return }
            apply(status: status)
        } catch is CancellationError {
            return
        } catch {
            guard generation == statusGeneration else { return }
            if errorMessage != error.localizedDescription {
                logger.error("Status refresh failed: \(error.localizedDescription, privacy: .private)")
            }
            errorMessage = error.localizedDescription
        }
    }

    func setPreferred(pool: String, account: String?) async {
        guard updatingPool == nil else { return }
        updatingPool = pool
        // A poll already in flight can contain the previous preference. Never let
        // it overwrite the authoritative read after the user's selection.
        statusGeneration &+= 1
        defer { updatingPool = nil }
        do {
            if let updated = try await client.setPreferred(pool: pool, account: account) {
                apply(status: updated)
            } else {
                await readStatus(generation: statusGeneration)
            }
            actionErrorMessage = nil
        } catch {
            actionErrorMessage = error.localizedDescription
        }
    }

    func beginLogin(account: String) {
        guard !isLoginRunning else { return }
        loginTask?.cancel()
        // A new attempt must not inherit the previous account's code or session.
        login = LoginSnapshot(account: account, state: .running)
        loginTask = Task { [weak self] in
            guard let self else { return }
            do {
                var current = try await client.startLogin(account: account)
                apply(login: current)
                while current.state == .running && !Task.isCancelled {
                    guard let sessionID = current.sessionID, !sessionID.isEmpty else {
                        throw ControlSocketError.malformedResponse
                    }
                    try await Task.sleep(nanoseconds: 1_000_000_000)
                    current = try await client.loginStatus(sessionID: sessionID)
                    apply(login: current)
                }
                if current.state == .succeeded { await refresh() }
            } catch is CancellationError {
                return
            } catch {
                apply(login: LoginSnapshot(account: account, sessionID: login?.sessionID, state: .failed, error: error.localizedDescription))
            }
        }
    }

    func connectExistingLogin(account: String) async {
        guard connectingAccount == nil, !isLoginRunning, updatingPool == nil else { return }
        connectingAccount = account
        defer { connectingAccount = nil }
        do {
            try await client.connectExistingLogin(account: account)
            // The daemon acknowledges before reloading. Wait until the new account
            // is visible instead of reporting the old inbound snapshot as success.
            for _ in 0..<40 {
                try await Task.sleep(nanoseconds: 250_000_000)
                if let updated = try? await client.status(),
                   let connected = updated.accounts.first(where: { $0.name == account }),
                   !connected.isInbound {
                    apply(status: updated)
                    actionErrorMessage = nil
                    return
                }
            }
            throw ControlSocketError.daemon("Login connection saved, but Comradex has not reconnected yet. Refresh to check its status.")
        } catch is CancellationError {
            return
        } catch {
            actionErrorMessage = error.localizedDescription
        }
    }

    func apply(status: UIStatusSnapshot) {
        if errorMessage != nil { logger.info("Status connection recovered") }
        snapshot = status
        lastSuccessfulRefresh = Date()
        errorMessage = nil
    }

    func apply(login value: LoginSnapshot) {
        let sameAccount = value.account.isEmpty || value.account == login?.account
        let sameSession = value.sessionID == nil || login?.sessionID == nil || value.sessionID == login?.sessionID
        let previous = sameAccount && sameSession ? login : nil
        login = LoginSnapshot(
            account: value.account.isEmpty ? (previous?.account ?? "") : value.account,
            sessionID: value.sessionID ?? previous?.sessionID,
            state: value.state,
            verificationURI: value.verificationURI ?? previous?.verificationURI,
            userCode: value.userCode ?? previous?.userCode,
            error: value.error
        )
    }
}
