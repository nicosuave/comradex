import ComradexWidgetCore
import SwiftUI
import WidgetKit

@main
struct ComradexPhoneApp: App {
    var body: some Scene {
        WindowGroup {
            ContentView()
        }
    }
}

@MainActor
final class UsageModel: ObservableObject {
    @Published private(set) var snapshot = RemoteUsage.cache.load()
    @Published private(set) var errorMessage: String?
    @Published private(set) var isFetching = false

    func refresh() async {
        guard !isFetching else { return }
        isFetching = true
        defer { isFetching = false }
        do {
            snapshot = try await RemoteUsage.fetch()
            errorMessage = nil
            // The widget keeps its own cache, so it fetches again when reloaded.
            WidgetCenter.shared.reloadTimelines(ofKind: usageWidgetKind)
        } catch {
            errorMessage = error.localizedDescription
        }
    }
}

struct ContentView: View {
    @StateObject private var model = UsageModel()
    @Environment(\.scenePhase) private var scenePhase

    var body: some View {
        NavigationStack {
            List {
                Section {
                    // The same layout as the large widget, sized to its accounts rather than
                    // the widget's fixed height.
                    UsageWidgetView(snapshot: model.snapshot, date: Date(), family: .systemLarge)
                        .fixedSize(horizontal: false, vertical: true)
                        .padding(.vertical, 8)
                } footer: {
                    if let error = model.errorMessage {
                        Label(error, systemImage: "exclamationmark.triangle")
                    } else if let snapshot = model.snapshot {
                        Text("Updated \(snapshot.publishedAt.formatted(date: .omitted, time: .shortened))")
                    }
                }

                Section {
                    Text(RemoteUsage.serverURL.isEmpty ? "Not set" : RemoteUsage.serverURL)
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)
                } header: {
                    Text("Mac")
                } footer: {
                    Text("Comradex Menu shares usage when Share Usage with iPhone is on, through `tailscale serve`. Add the Account Usage widget from the home screen.")
                }
            }
            .navigationTitle("Comradex")
            .refreshable { await model.refresh() }
        }
        .onChange(of: scenePhase, initial: true) { _, phase in
            if phase == .active { Task { await model.refresh() } }
        }
    }
}
