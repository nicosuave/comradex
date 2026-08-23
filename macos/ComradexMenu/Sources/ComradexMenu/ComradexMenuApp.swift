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
    @StateObject private var store = ComradexStore()

    var body: some Scene {
        MenuBarExtra {
            MenuContentView()
                .environmentObject(store)
        } label: {
            Image(nsImage: StatusIcon.image)
                .renderingMode(.template)
                .accessibilityLabel("Comradex")
        }
        .menuBarExtraStyle(.window)

        Window("Account Login", id: "account-login") {
            LoginWindowView()
                .environmentObject(store)
        }
        .defaultSize(width: 440, height: 300)
    }
}
