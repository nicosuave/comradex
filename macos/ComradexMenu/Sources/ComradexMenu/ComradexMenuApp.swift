import AppKit
import SwiftUI

enum StatusIcon {
    static let image: NSImage = {
        let base = NSImage(
            systemSymbolName: "point.3.connected.trianglepath.dotted",
            accessibilityDescription: "Comradex"
        ) ?? NSImage(systemSymbolName: "circle.grid.2x2", accessibilityDescription: "Comradex")!
        let image = base.withSymbolConfiguration(.init(pointSize: 15, weight: .medium)) ?? base
        image.isTemplate = true
        return image
    }()
}

@main
struct ComradexMenuApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate

    var body: some Scene {
        Settings { EmptyView() }
    }
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    private let store = ComradexStore()
    private var menuController: MenuBarController?

    func applicationDidFinishLaunching(_ notification: Notification) {
        let controller = MenuBarController(store: store)
        menuController = controller
        controller.start()
    }
}
