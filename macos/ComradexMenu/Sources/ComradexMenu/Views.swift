import SwiftUI

struct MenuContentView: View {
    @EnvironmentObject private var store: ComradexStore
    @Environment(\.openWindow) private var openWindow

    var body: some View {
        VStack(spacing: 0) {
            header
                .padding(.horizontal, 16)
                .padding(.vertical, 12)
            Divider()

            if let snapshot = store.snapshot {
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 0) {
                        if store.errorMessage != nil { staleDataWarning }
                        statusContent(snapshot)
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 8)
                }
                .scrollBounceBehavior(.basedOnSize)
                .frame(maxHeight: 360)
            } else if let error = store.errorMessage {
                errorState(error)
                    .padding(16)
            } else {
                Label("Connecting to Comradex…", systemImage: "arrow.triangle.2.circlepath")
                    .foregroundStyle(.secondary)
                    .padding(16)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }

            Divider()
            commands
                .padding(.horizontal, 10)
                .padding(.vertical, 6)
        }
        .frame(width: 340)
        .task { await store.refreshLoop() }
    }

    private var header: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text("Comradex")
                .font(.title3.weight(.medium))
            Label(connectionLabel, systemImage: "circle.fill")
                .font(.caption)
                .foregroundStyle(connectionColor)
                .labelStyle(CompactStatusLabelStyle())
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    private func statusContent(_ snapshot: UIStatusSnapshot) -> some View {
        if snapshot.pools.isEmpty && snapshot.accounts.isEmpty {
            Label("No pools or accounts configured", systemImage: "tray")
                .font(.callout)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
                .padding(8)
        } else {
            if !snapshot.pools.isEmpty {
                VStack(spacing: 0) {
                    ForEach(snapshot.pools) { pool in
                        poolGroup(pool, snapshot: snapshot)
                    }
                }
            }

            let renderedAccountNames = Set(snapshot.pools.flatMap(\.members))
            let standaloneAccounts = snapshot.accounts.filter { !renderedAccountNames.contains($0.name) }
            if !standaloneAccounts.isEmpty {
                VStack(spacing: 0) {
                    ForEach(standaloneAccounts) { account in
                        accountRow(account, pool: nil, isPreferred: false)
                    }
                }
                .padding(.top, snapshot.pools.isEmpty ? 0 : 8)
            }
        }
    }

    private func poolGroup(_ pool: PoolSnapshot, snapshot: UIStatusSnapshot) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            VStack(alignment: .leading, spacing: 2) {
                Text(pool.name).font(.callout.weight(.medium))
                Text("Pool · Active: \(pool.active ?? "None")")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 8)

            ForEach(pool.members, id: \.self) { member in
                if let account = snapshot.accounts.first(where: { $0.name == member }) {
                    accountRow(account, pool: pool, isPreferred: pool.preferred == member)
                } else {
                    missingAccountRow(member, pool: pool)
                }
            }
        }
        .padding(.bottom, 8)
    }

    private func accountRow(_ account: AccountSnapshot, pool: PoolSnapshot?, isPreferred: Bool) -> some View {
        HStack(spacing: 9) {
            Image(systemName: accountIcon(account))
                .foregroundStyle(accountColor(account))
                .frame(width: 16)
            VStack(alignment: .leading, spacing: 2) {
                Text(account.name).font(.callout.weight(.medium))
                Text(accountSubtitle(account, poolName: pool?.name))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer(minLength: 8)

            if let pool {
                preferenceButton(account: account.name, pool: pool, isPreferred: isPreferred)
            }

            if account.needsLoginAction {
                Button("Re-login") { beginLogin(account.name) }
                    .buttonStyle(.borderless)
                    .disabled(store.isLoginRunning)
                    .accessibilityLabel("Re-login \(account.name)")
            } else if account.authState?.lowercased() == "login_in_progress" {
                ProgressView()
                    .controlSize(.small)
                    .accessibilityLabel("Login in progress for \(account.name)")
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 7)
    }

    private func missingAccountRow(_ name: String, pool: PoolSnapshot) -> some View {
        HStack(spacing: 9) {
            Image(systemName: "questionmark.circle")
                .foregroundStyle(.secondary)
                .frame(width: 16)
            VStack(alignment: .leading, spacing: 2) {
                Text(name).font(.callout.weight(.medium))
                Text("Unavailable · \(pool.name)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 7)
    }

    @ViewBuilder
    private func preferenceButton(account: String, pool: PoolSnapshot, isPreferred: Bool) -> some View {
        if isPreferred {
            Text("Preferred")
                .font(.caption.weight(.medium))
                .foregroundStyle(.tint)
                .accessibilityLabel("\(account) is preferred for \(pool.name)")
        } else {
            Button("Prefer") {
                Task { await store.setPreferred(pool: pool.name, account: account) }
            }
            .buttonStyle(.borderless)
            .disabled(store.updatingPool != nil)
            .accessibilityLabel("Prefer \(account) for \(pool.name)")
        }
    }

    private func beginLogin(_ account: String) {
        openWindow(id: "account-login")
        store.beginLogin(account: account)
    }

    private var commands: some View {
        VStack(spacing: 0) {
            commandButton("Refresh", shortcut: "⌘ R", key: "r", disabled: store.isRefreshing) {
                Task { await store.refresh() }
            }
            commandButton("Quit Comradex", shortcut: "⌘ Q", key: "q") {
                NSApplication.shared.terminate(nil)
            }
        }
    }

    private func commandButton(
        _ title: String,
        shortcut: String,
        key: KeyEquivalent,
        disabled: Bool = false,
        action: @escaping () -> Void
    ) -> some View {
        Button(action: action) {
            HStack {
                Text(title)
                Spacer()
                if title == "Refresh", store.isRefreshing {
                    ProgressView().controlSize(.small)
                } else {
                    Text(shortcut)
                        .font(.caption)
                        .foregroundStyle(.tertiary)
                }
            }
            .contentShape(Rectangle())
            .padding(.horizontal, 8)
            .padding(.vertical, 6)
        }
        .buttonStyle(.plain)
        .keyboardShortcut(key, modifiers: .command)
        .disabled(disabled)
    }

    private var staleDataWarning: some View {
        Label("Status may be out of date", systemImage: "exclamationmark.triangle.fill")
            .font(.caption)
            .foregroundStyle(.orange)
    }

    private func errorState(_ error: String) -> some View {
        let incompatible = error.contains("unknown variant `ui_status`")
            || error.contains("unknown variant \"ui_status\"")
        return VStack(alignment: .leading, spacing: 8) {
            Label(
                incompatible ? "Daemon update required" : "Comradex isn’t available",
                systemImage: incompatible ? "arrow.down.circle" : "exclamationmark.triangle.fill"
            )
            .font(.callout.weight(.semibold))

            Text(incompatible
                ? "The running daemon doesn’t support this menu app yet. Update and restart Comradex, then refresh."
                : "The menu app couldn’t reach the Comradex daemon.")
                .font(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)

        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var connectionLabel: String {
        if let error = store.errorMessage, store.snapshot == nil {
            return error.contains("unknown variant") ? "Update required" : "Unavailable"
        }
        if store.errorMessage != nil { return "Stale" }
        if store.snapshot?.daemonRunning == true { return "Running" }
        return store.isRefreshing ? "Connecting" : "Unavailable"
    }

    private var connectionColor: Color {
        switch connectionLabel {
        case "Running": return .green
        case "Connecting": return .secondary
        case "Stale", "Update required": return .orange
        default: return .red
        }
    }

    private func accountSubtitle(_ account: AccountSnapshot, poolName: String?) -> String {
        let state: String
        switch account.authState?.lowercased() {
        case "login_in_progress": state = "Login in progress"
        case "inbound": state = "Inbound"
        case "signed_in": state = "Signed in"
        case "signed_out": state = "Signed out"
        default: state = account.isSignedIn ? "Signed in" : "Signed out"
        }
        if let poolName { return "\(state) · \(poolName)" }
        guard !account.pools.isEmpty else { return state }
        return "\(state) · \(account.pools.joined(separator: ", "))"
    }

    private func accountIcon(_ account: AccountSnapshot) -> String {
        switch account.authState?.lowercased() {
        case "login_in_progress": return "clock.fill"
        case "inbound": return "arrow.down.circle.fill"
        default: return account.isSignedIn ? "checkmark.circle.fill" : "exclamationmark.circle.fill"
        }
    }

    private func accountColor(_ account: AccountSnapshot) -> Color {
        switch account.authState?.lowercased() {
        case "login_in_progress": return .blue
        case "inbound": return .secondary
        default: return account.isSignedIn ? .green : .orange
        }
    }
}

private struct CompactStatusLabelStyle: LabelStyle {
    func makeBody(configuration: Configuration) -> some View {
        HStack(spacing: 5) {
            configuration.icon
                .font(.system(size: 7))
            configuration.title
        }
    }
}

struct LoginWindowView: View {
    @EnvironmentObject private var store: ComradexStore
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            if let login = store.login {
                HStack {
                    VStack(alignment: .leading, spacing: 3) {
                        Text("Login: \(login.account)")
                            .font(.title3.weight(.semibold))
                        Label(stateLabel(login.state), systemImage: stateIcon(login.state))
                            .foregroundStyle(stateColor(login.state))
                    }
                    Spacer()
                    if login.state == .running { ProgressView() }
                }

                if let code = login.userCode, !code.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("Enter this code")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(code)
                            .font(.system(.title, design: .monospaced, weight: .semibold))
                            .textSelection(.enabled)
                        Link("Open OpenAI device login", destination: login.safeVerificationURL)
                    }
                } else if login.state == .running {
                    Text("Waiting for a device code…")
                        .foregroundStyle(.secondary)
                }

                if let error = login.error, !error.isEmpty {
                    Label(error, systemImage: "exclamationmark.triangle.fill")
                        .foregroundStyle(.red)
                        .textSelection(.enabled)
                }

                HStack {
                    if login.state == .failed {
                        Button("Try Again") { store.beginLogin(account: login.account) }
                    }
                    Spacer()
                    if login.state != .running {
                        Button("Done") { dismiss() }
                    }
                }
            } else {
                ContentUnavailableView("No login in progress", systemImage: "person.crop.circle")
            }
        }
        .padding(20)
        .frame(width: 420)
    }

    private func stateLabel(_ state: LoginState) -> String {
        switch state {
        case .idle, .notStarted: return "Idle"
        case .running: return "Waiting for device authorization"
        case .succeeded: return "Signed in"
        case .failed: return "Login failed"
        }
    }

    private func stateIcon(_ state: LoginState) -> String {
        switch state {
        case .idle, .notStarted: return "circle"
        case .running: return "clock"
        case .succeeded: return "checkmark.circle.fill"
        case .failed: return "xmark.circle.fill"
        }
    }

    private func stateColor(_ state: LoginState) -> Color {
        switch state {
        case .idle, .notStarted: return .secondary
        case .running: return .blue
        case .succeeded: return .green
        case .failed: return .red
        }
    }
}
