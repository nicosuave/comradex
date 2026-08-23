// swift-tools-version: 5.10
import PackageDescription

let package = Package(
    name: "ComradexMenu",
    platforms: [
        .macOS(.v14),
    ],
    products: [
        .executable(name: "ComradexMenu", targets: ["ComradexMenu"]),
    ],
    targets: [
        .executableTarget(
            name: "ComradexMenu",
            path: "Sources/ComradexMenu"
        ),
        .testTarget(
            name: "ComradexMenuTests",
            dependencies: ["ComradexMenu"],
            path: "Tests/ComradexMenuTests"
        ),
    ]
)
