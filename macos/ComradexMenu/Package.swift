// swift-tools-version: 5.10
import PackageDescription

// Code shared with the widget extension must use only extension-safe APIs.
let applicationExtensionSettings: [SwiftSetting] = [.unsafeFlags(["-application-extension"])]

let package = Package(
    name: "ComradexMenu",
    platforms: [
        .macOS(.v14),
        // The iPhone app in ios/ComradexPhone uses only ComradexWidgetCore.
        .iOS(.v17),
    ],
    products: [
        .executable(name: "ComradexMenu", targets: ["ComradexMenu"]),
        .executable(name: "ComradexWidgets", targets: ["ComradexWidgets"]),
        .library(name: "ComradexWidgetCore", targets: ["ComradexWidgetCore"]),
    ],
    targets: [
        .executableTarget(
            name: "ComradexMenu",
            dependencies: ["ComradexWidgetCore"],
            path: "Sources/ComradexMenu",
            resources: [.copy("Resources/comradex-logo.svg")]
        ),
        .target(
            name: "ComradexWidgetCore",
            path: "Sources/ComradexWidgetCore",
            swiftSettings: applicationExtensionSettings
        ),
        // Packaged as ComradexMenu.app/Contents/PlugIns/ComradexWidgets.appex by
        // Scripts/package_app.sh. Extension processes start in NSExtensionMain.
        .executableTarget(
            name: "ComradexWidgets",
            dependencies: ["ComradexWidgetCore"],
            path: "Sources/ComradexWidgets",
            swiftSettings: [.unsafeFlags(["-parse-as-library", "-application-extension"])],
            linkerSettings: [
                .linkedFramework("WidgetKit"),
                .unsafeFlags(["-Xlinker", "-application_extension", "-Xlinker", "-e", "-Xlinker", "_NSExtensionMain"]),
            ]
        ),
        .testTarget(
            name: "ComradexMenuTests",
            dependencies: ["ComradexMenu", "ComradexWidgetCore"],
            path: "Tests/ComradexMenuTests"
        ),
    ]
)
