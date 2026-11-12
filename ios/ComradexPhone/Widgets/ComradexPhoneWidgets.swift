import ComradexWidgetCore
import SwiftUI
import WidgetKit

struct UsageEntry: TimelineEntry {
    let date: Date
    let snapshot: UsageSnapshot?
}

struct UsageTimelineProvider: TimelineProvider {
    /// iOS budgets widget reloads; it may refresh less often than this.
    static let refreshInterval: TimeInterval = 15 * 60

    func placeholder(in context: Context) -> UsageEntry {
        UsageEntry(date: Date(), snapshot: .sample(at: Date()))
    }

    func getSnapshot(in context: Context, completion: @escaping (UsageEntry) -> Void) {
        let now = Date()
        let snapshot = RemoteUsage.cache.load() ?? (context.isPreview ? .sample(at: now) : nil)
        completion(UsageEntry(date: now, snapshot: snapshot))
    }

    func getTimeline(in context: Context, completion: @escaping (Timeline<UsageEntry>) -> Void) {
        Task {
            let now = Date()
            // An unreachable Mac falls back to the cached snapshot, which the views label
            // with its age once it is stale.
            let snapshot = (try? await RemoteUsage.fetch()) ?? RemoteUsage.cache.load()
            // Minute entries keep countdowns and stale-data age labels current between fetches.
            let minute = Date(timeIntervalSince1970: (now.timeIntervalSince1970 / 60).rounded(.down) * 60)
            let entries = [UsageEntry(date: now, snapshot: snapshot)] + (1..<30).map {
                UsageEntry(date: minute.addingTimeInterval(TimeInterval($0 * 60)), snapshot: snapshot)
            }
            completion(Timeline(entries: entries, policy: .after(now.addingTimeInterval(Self.refreshInterval))))
        }
    }
}

struct UsageWidgetEntryView: View {
    @Environment(\.widgetFamily) private var family
    let entry: UsageEntry

    var body: some View {
        UsageWidgetView(snapshot: entry.snapshot, date: entry.date, family: family)
            .containerBackground(.background, for: .widget)
    }
}

struct UsageWidget: Widget {
    var body: some WidgetConfiguration {
        StaticConfiguration(kind: usageWidgetKind, provider: UsageTimelineProvider()) { entry in
            UsageWidgetEntryView(entry: entry)
        }
        .configurationDisplayName("Account Usage")
        .description("See how much quota is left on each Comradex account.")
        .supportedFamilies([.systemSmall, .systemMedium, .systemLarge, .systemExtraLarge])
    }
}

@main
struct ComradexPhoneWidgets: WidgetBundle {
    var body: some Widget {
        UsageWidget()
    }
}
