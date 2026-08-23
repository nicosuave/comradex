import SwiftUI

struct MenuContentView: View {
    @EnvironmentObject private var store: ComradexStore
    @Environment(\.openWindow) private var openWindow

    var body: some View {
        VStack(spacing: 0) {
            header
                .padding(.horizontal, 12)
                .padding(.vertical, 10)
            Divider()

            if let snapshot = store.snapshot {
                VStack(alignment: .leading, spacing: 12) {
                    if store.errorMessage != nil { staleDataWarning }
                    statusContent(snapshot)
                }
                .padding(12)
            } else if let error = store.errorMessage {
                errorState(error)
                    .padding(16)
            } else {
                Label("Connecting to Comradex…", systemImage: "arrow.triangle.2.circlepath")
                    .foregroundStyle(.secondary)
                    .padding(16)
                    .frame(maxWidth: .infinity, alignment: .leading)
            }
        }
        .frame(width: 340)
        .task { await store.refreshLoop() }
    }

    private var header: some View {
        HStack(spacing: 8) {
            VStack(alignment: .leading, spacing: 2) {
                Text("Comradex")
                    .font(.headline)
                Label(connectionLabel, systemImage: "circle.fill")
                    .font(.caption)
                    .foregroundStyle(connectionColor)
                    .labelStyle(CompactStatusLabelStyle())
            }
            Spacer()

            if store.isRefreshing {
                ProgressView()
                    .controlSize(.small)
            } else {
                Button {
                    Task { await store.refresh() }
                } label: {
                    Image(systemName: "arrow.clockwise")
                }
                .buttonStyle(.borderless)
                .help("Refresh")
                .keyboardShortcut("r")
            }

            Menu {
                Button("Refresh") { Task { await store.refresh() } }
                    .keyboardShortcut("r")
                Divider()
                Button("Quit Comradex") { NSApplication.shared.terminate(nil) }
                    .keyboardShortcut("q")
            } label: {
                Image(systemName: "ellipsis.circle")
            }
            .menuStyle(.borderlessButton)
            .fixedSize()
            .help("More")
        }
    }

    @ViewBuilder
    private func statusContent(_ snapshot: UIStatusSnapshot) -> some View {
        if snapshot.pools.isEmpty && snapshot.accounts.isEmpty {
            Label("No pools or accounts configured", systemImage: "tray")
                .font(.callout)
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            if !snapshot.pools.isEmpty {
                sectionTitle("Pools")
                VStack(spacing: 0) {
                    ForEach(Array(snapshot.pools.enumerated()), id: \.element.id) { index, pool in
                        poolRow(pool)
                        if index < snapshot.pools.count - 1 { Divider() }
                    }
                }
            }

            if !snapshot.pools.isEmpty && !snapshot.accounts.isEmpty { Divider() }

            if !snapshot.accounts.isEmpty {
                sectionTitle("Accounts")
                VStack(spacing: 0) {
                    ForEach(Array(snapshot.accounts.enumerated()), id: \.element.id) { index, account in
                        accountRow(account)
                        if index < snapshot.accounts.count - 1 { Divider() }
                    }
                }
            }
        }
    }

    private func sectionTitle(_ title: String) -> some View {
        Text(title.uppercased())
            .font(.caption2.weight(.semibold))
            .foregroundStyle(.secondary)
    }

    private func poolRow(_ pool: PoolSnapshot) -> some View {
        HStack(spacing: 10) {
            VStack(alignment: .leading, spacing: 2) {
                Text(pool.name)
                    .font(.callout.weight(.medium))
                Text("Active: \(pool.active ?? "None")")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer()
            preferenceMenu(pool)
        }
        .padding(.vertical, 7)
    }

    private func preferenceMenu(_ pool: PoolSnapshot) -> some View {
        Menu(pool.preferred ?? "Automatic") {
            Button {
                Task { await store.setPreferred(pool: pool.name, account: nil) }
            } label: {
                if pool.preferred == nil { Label("Automatic", systemImage: "checkmark") }
                else { Text("Automatic") }
            }
            Divider()
            ForEach(pool.members, id: \.self) { account in
                Button {
                    Task { await store.setPreferred(pool: pool.name, account: account) }
                } label: {
                    if pool.preferred == account { Label(account, systemImage: "checkmark") }
                    else { Text(account) }
                }
            }
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
        .disabled(store.updatingPool != nil)
    }

    private func accountRow(_ account: AccountSnapshot) -> some View {
        HStack(spacing: 9) {
            Image(systemName: accountIcon(account))
                .foregroundStyle(accountColor(account))
                .frame(width: 16)
            VStack(alignment: .leading, spacing: 2) {
                Text(account.name)
                    .font(.callout.weight(.medium))
                Text(accountSubtitle(account))
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer()

            if account.kind.caseInsensitiveCompare("inbound") != .orderedSame {
                Button(account.isSignedIn ? "Re-login" : "Login") {
                    openWindow(id: "account-login")
                    store.beginLogin(account: account.name)
                }
                .buttonStyle(.borderless)
                .disabled(store.isLoginRunning)
            }
        }
        .padding(.vertical, 7)
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

            Button("Try Again") { Task { await store.refresh() } }
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

    private func accountSubtitle(_ account: AccountSnapshot) -> String {
        let state: String
        switch account.authState?.lowercased() {
        case "login_in_progress": state = "Login in progress"
        case "inbound": state = "Inbound"
        case "signed_in": state = "Signed in"
        case "signed_out": state = "Signed out"
        default: state = account.isSignedIn ? "Signed in" : "Signed out"
        }
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
