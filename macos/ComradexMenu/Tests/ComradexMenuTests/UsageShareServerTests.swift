import XCTest
@testable import ComradexMenu
@testable import ComradexWidgetCore

private let snapshot = UsageSnapshot(publishedAt: Date(timeIntervalSince1970: 1_800_000_000), accounts: [
    UsageAccount(name: "personal", provider: .codex, status: .available, windows: [
        UsageWindow(duration: 18_000, usedPercent: 38, resetsAt: Date(timeIntervalSince1970: 1_800_003_600)),
    ], summaryWindowIndex: 0),
])

private func response(_ request: String, snapshot: UsageSnapshot? = snapshot) -> (head: String, body: Data) {
    let data = UsageShareServer.response(to: Data(request.utf8), snapshot: snapshot)
    let separator = data.range(of: Data("\r\n\r\n".utf8))!
    return (String(decoding: data[..<separator.lowerBound], as: UTF8.self), Data(data[separator.upperBound...]))
}

final class UsageShareServerTests: XCTestCase {
    func testServesTheWidgetSnapshot() throws {
        let (head, body) = response("GET /usage HTTP/1.1\r\nHost: nicbook\r\n\r\n")

        XCTAssertTrue(head.hasPrefix("HTTP/1.1 200 OK\r\n"))
        XCTAssertTrue(head.contains("Content-Type: application/json"))
        XCTAssertTrue(head.contains("Content-Length: \(body.count)"))
        XCTAssertEqual(try UsageSnapshotStore.decode(body), snapshot)
    }

    func testRejectsOtherRequests() {
        XCTAssertTrue(response("GET / HTTP/1.1\r\n\r\n").head.hasPrefix("HTTP/1.1 404"))
        XCTAssertTrue(response("POST /usage HTTP/1.1\r\n\r\n").head.hasPrefix("HTTP/1.1 405"))
        XCTAssertTrue(response("garbage\r\n\r\n").head.hasPrefix("HTTP/1.1 400"))
        // Before the first daemon read there is nothing to serve.
        XCTAssertTrue(response("GET /usage HTTP/1.1\r\n\r\n", snapshot: nil).head.hasPrefix("HTTP/1.1 503"))
        XCTAssertTrue(response("HEAD /usage HTTP/1.1\r\n\r\n").body.isEmpty)
    }

    @MainActor
    func testListensOnLoopback() async throws {
        let server = UsageShareServer(port: 0) { snapshot }
        let listening = expectation(description: "listening")
        server.onStateChange = { if case .listening = $0 { listening.fulfill() } }
        server.start()
        await fulfillment(of: [listening], timeout: 5)
        defer { server.stop() }
        guard case let .listening(port) = server.state else { return XCTFail("not listening: \(server.state)") }

        let (data, urlResponse) = try await URLSession.shared.data(from: URL(string: "http://127.0.0.1:\(port)/usage")!)

        XCTAssertEqual((urlResponse as? HTTPURLResponse)?.statusCode, 200)
        XCTAssertEqual(try UsageSnapshotStore.decode(data), snapshot)
    }

    /// `tailscale serve` listens on the same port at the tailnet address; loopback must still bind.
    @MainActor
    func testSharesAPortAnotherAddressAlreadyListensOn() async throws {
        let other = Darwin.socket(AF_INET6, SOCK_STREAM, 0)
        defer { Darwin.close(other) }
        var v6Only: Int32 = 1
        setsockopt(other, IPPROTO_IPV6, IPV6_V6ONLY, &v6Only, socklen_t(MemoryLayout<Int32>.size))
        var address = sockaddr_in6()
        address.sin6_len = UInt8(MemoryLayout<sockaddr_in6>.size)
        address.sin6_family = sa_family_t(AF_INET6)
        address.sin6_addr = in6addr_loopback
        var length = socklen_t(MemoryLayout<sockaddr_in6>.size)
        withUnsafeMutablePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { socketAddress in
                XCTAssertEqual(Darwin.bind(other, socketAddress, length), 0)
                XCTAssertEqual(Darwin.listen(other, 1), 0)
                XCTAssertEqual(getsockname(other, socketAddress, &length), 0)
            }
        }
        let port = UInt16(bigEndian: address.sin6_port)

        let server = UsageShareServer(port: port) { snapshot }
        server.start()
        defer { server.stop() }
        XCTAssertEqual(server.state, .listening(port: port))

        let (data, _) = try await URLSession.shared.data(from: URL(string: "http://127.0.0.1:\(port)/usage")!)
        XCTAssertEqual(try UsageSnapshotStore.decode(data), snapshot)
    }
}
