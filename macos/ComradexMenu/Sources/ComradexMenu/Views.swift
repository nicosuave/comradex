import SwiftUI

struct LoginWindowView: View {
    @EnvironmentObject private var store: ComradexStore
    private let onDone: () -> Void

    init(onDone: @escaping () -> Void = {}) {
        self.onDone = onDone
    }

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
                        Button("Done", action: onDone)
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
