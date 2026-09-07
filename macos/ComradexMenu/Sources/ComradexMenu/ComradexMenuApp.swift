import AppKit
import SwiftUI

enum StatusIcon {
    static let image: NSImage = {
        let url = Bundle.main.url(forResource: "comradex-logo", withExtension: "svg")
            ?? Bundle.module.url(forResource: "comradex-logo", withExtension: "svg")!
        let image = NSImage(contentsOf: url)!
        image.size = NSSize(width: 19, height: 13)
        image.accessibilityDescription = "Comradex"
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
