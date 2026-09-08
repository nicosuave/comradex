import Foundation
import SwiftUI
import OSLog

@MainActor
final class ComradexStore: ObservableObject {
    @Published private(set) var snapshot: UIStatusSnapshot?
    @Published private(set) var login: LoginSnapshot?
    @Published private(set) var isRefreshing = false
    @Published private(set) var updatingPool: String?
    @Published private(set) var errorMessage: String?
    @Published private(set) var actionErrorMessage: String?
    @Published private(set) var lastSuccessfulRefresh: Date?
    private let logger = Logger(subsystem: "com.nicosuave.comradex.menu", category: "connection")

    var isLoginRunning: Bool { login?.state == .running }

    private let client: any ControlServing
    private var loginTask: Task<Void, Never>?

    init(client: any ControlServing = ControlSocketClient()) {
        self.client = client
    }

    deinit { loginTask?.cancel() }

    func refresh() async {
        guard !isRefreshing else { return }
        isRefreshing = true
        defer { isRefreshing = false }
        do {
            apply(status: try await client.status())
        } catch is CancellationError {
            return
        } catch {
            if errorMessage != error.localizedDescription {
                logger.error("Status refresh failed: \(error.localizedDescription, privacy: .private)")
            }
            errorMessage = error.localizedDescription
        }
    }

    func setPreferred(pool: String, account: String?) async {
        guard updatingPool == nil else { return }
        updatingPool = pool
        defer { updatingPool = nil }
        do {
            if let updated = try await client.setPreferred(pool: pool, account: account) {
                apply(status: updated)
            } else {
                await refresh()
            }
            actionErrorMessage = nil
        } catch {
            actionErrorMessage = error.localizedDescription
        }
    }

    func beginLogin(account: String) {
        guard !isLoginRunning else { return }
        loginTask?.cancel()
        apply(login: LoginSnapshot(account: account, state: .running))
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

    func apply(status: UIStatusSnapshot) {
        if errorMessage != nil { logger.info("Status connection recovered") }
        snapshot = status
        lastSuccessfulRefresh = Date()
        errorMessage = nil
    }

    func apply(login value: LoginSnapshot) {
        login = LoginSnapshot(
            account: value.account.isEmpty ? (login?.account ?? "") : value.account,
            sessionID: value.sessionID ?? login?.sessionID,
            state: value.state,
            verificationURI: value.verificationURI ?? login?.verificationURI,
            userCode: value.userCode ?? login?.userCode,
            error: value.error
        )
    }
}
