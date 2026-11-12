import ComradexWidgetCore
import SwiftUI
import WidgetKit

struct UsageEntry: TimelineEntry {
    let date: Date
    let snapshot: UsageSnapshot?
}

struct UsageTimelineProvider: TimelineProvider {
    private let store = UsageSnapshotStore()

    func placeholder(in context: Context) -> UsageEntry {
        UsageEntry(date: Date(), snapshot: .sample(at: Date()))
    }

    func getSnapshot(in context: Context, completion: @escaping (UsageEntry) -> Void) {
        let now = Date()
        // The gallery shows the user's own accounts when the menu app has published them.
        let snapshot = store.load() ?? (context.isPreview ? .sample(at: now) : nil)
        completion(UsageEntry(date: now, snapshot: snapshot))
    }

    func getTimeline(in context: Context, completion: @escaping (Timeline<UsageEntry>) -> Void) {
        let now = Date()
        guard let snapshot = store.load() else {
            completion(Timeline(entries: [UsageEntry(date: now, snapshot: nil)], policy: .after(now.addingTimeInterval(15 * 60))))
            return
        }
        // Minute entries keep countdowns and stale-data age labels current. The menu app
        // requests a reload when usage changes; the timeline's end re-reads the file.
        let minute = Date(timeIntervalSince1970: (now.timeIntervalSince1970 / 60).rounded(.down) * 60)
        let entries = [UsageEntry(date: now, snapshot: snapshot)] + (1..<30).map {
            UsageEntry(date: minute.addingTimeInterval(TimeInterval($0 * 60)), snapshot: snapshot)
        }
        completion(Timeline(entries: entries, policy: .atEnd))
    }
}

struct UsageWidgetEntryView: View {
    @Environment(\.widgetFamily) private var family
    let entry: UsageEntry

    var body: some View {
        UsageWidgetView(snapshot: entry.snapshot, date: entry.date, family: family)
            .containerBackground(.background, for: .widget)
            .widgetURL(usageMenuURL())
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
struct ComradexWidgets: WidgetBundle {
    var body: some Widget {
        UsageWidget()
    }
}
