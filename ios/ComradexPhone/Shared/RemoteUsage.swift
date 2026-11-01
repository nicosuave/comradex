import ComradexWidgetCore
import Foundation

/// Fetches the usage snapshot that Comradex Menu serves on the Mac.
///
/// The menu app listens on loopback; `tailscale serve` publishes it to the tailnet over
/// HTTPS. The Mac's URL is set at build time, so the app and widget need no shared app
/// group, which API-key provisioning cannot assign. Each process caches its last good
/// snapshot so the widget still shows something while the Mac is unreachable.
enum RemoteUsage {
    /// The Mac's origin, such as `https://nicbook.example.ts.net:4363`.
    static var serverURL: String {
        (Bundle.main.object(forInfoDictionaryKey: "ComradexUsageServer") as? String ?? "")
            .trimmingCharacters(in: .whitespacesAndNewlines)
    }

    static let cache = UsageSnapshotStore(
        directory: FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask).first
    )

    enum FetchError: LocalizedError {
        case notConfigured
        case invalidURL
        case status(Int)

        var errorDescription: String? {
            switch self {
            case .notConfigured: return "This build has no Mac URL. Reinstall with Scripts/install.sh."
            case .invalidURL: return "The Mac URL must look like https://<mac>.<tailnet>.ts.net:4363."
            case .status(503): return "Comradex Menu has not reached the daemon yet."
            case let .status(code): return "The Mac answered with HTTP \(code)."
            }
        }
    }

    /// `GET <origin>/usage`. A trailing `/usage` in the configured URL is accepted too.
    static func endpoint(for server: String) throws -> URL {
        guard !server.isEmpty else { throw FetchError.notConfigured }
        let trimmed = server.hasSuffix("/") ? String(server.dropLast()) : server
        let withScheme = trimmed.contains("://") ? trimmed : "https://\(trimmed)"
        let path = withScheme.hasSuffix("/usage") ? withScheme : withScheme + "/usage"
        guard let url = URL(string: path), url.host != nil, url.scheme == "https" || url.scheme == "http" else {
            throw FetchError.invalidURL
        }
        return url
    }

    private static let session: URLSession = {
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = 15
        configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
        return URLSession(configuration: configuration)
    }()

    /// Fetches and caches the current snapshot.
    static func fetch() async throws -> UsageSnapshot {
        let (data, response) = try await session.data(from: endpoint(for: serverURL))
        if let http = response as? HTTPURLResponse, http.statusCode != 200 {
            throw FetchError.status(http.statusCode)
        }
        let snapshot = try UsageSnapshotStore.decode(data)
        try? cache.save(snapshot)
        return snapshot
    }
}
