import AppKit
import SwiftUI

private final class PreferredAccountAction: NSObject {
    let pool: String
    let account: String

    init(pool: String, account: String) {
        self.pool = pool
        self.account = account
    }
}

@MainActor
final class MenuBarController: NSObject, NSMenuDelegate {
    private let store: ComradexStore
    private var statusItem: NSStatusItem?
    private let menu = NSMenu()
    private var refreshTask: Task<Void, Never>?
    private var pollingTask: Task<Void, Never>?
    private var loginWindowController: NSWindowController?
    private var isMenuOpen = false
    private var hasDeferredMenuUpdate = false

    var renderedMenu: NSMenu { menu }

    init(store: ComradexStore) {
        self.store = store
        super.init()
        menu.autoenablesItems = false
    }

    deinit {
        refreshTask?.cancel()
        pollingTask?.cancel()
        if let statusItem {
            NSStatusBar.system.removeStatusItem(statusItem)
        }
    }

    func start() {
        let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.squareLength)
        self.statusItem = statusItem
        statusItem.button?.image = StatusIcon.image
        statusItem.button?.toolTip = "Comradex"
        menu.delegate = self
        statusItem.menu = menu
        rebuildMenu()
        startPolling()
    }

    // The controller owns polling so completed requests also update the native menu.
    func startPolling(intervalNanoseconds: UInt64 = 5_000_000_000) {
        guard pollingTask == nil else { return }
        refreshStatus()
        pollingTask = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(nanoseconds: intervalNanoseconds) }
                catch { return }
                guard !Task.isCancelled else { return }
                self?.refreshStatus()
            }
        }
    }

    func stopPolling() {
        pollingTask?.cancel()
        pollingTask = nil
        refreshTask?.cancel()
    }

    func menuWillOpen(_ menu: NSMenu) {
        rebuildMenu()
        isMenuOpen = true
        refreshStatus()
    }

    func menuDidClose(_ menu: NSMenu) {
        isMenuOpen = false
        if hasDeferredMenuUpdate {
            hasDeferredMenuUpdate = false
            rebuildMenu()
        }
        refreshStatus()
    }

    func rebuildMenu() {
        menu.removeAllItems()

        let header = NSMenuItem(title: "Comradex", action: nil, keyEquivalent: "")
        if #available(macOS 14.4, *) {
            header.subtitle = connectionLabel
        } else {
            header.title = "Comradex · \(connectionLabel)"
        }
        header.image = NSImage(systemSymbolName: connectionIcon, accessibilityDescription: connectionLabel)
        header.toolTip = [
            store.lastSuccessfulRefresh.map { "Last updated: \($0.formatted(date: .omitted, time: .standard))" },
            store.errorMessage,
        ].compactMap { $0 }.joined(separator: "\n")
        menu.addItem(header)
        menu.addItem(.separator())

        if let snapshot = store.snapshot {
            addStatus(snapshot)
        } else if let error = store.errorMessage {
            addInformationalItem(
                error.contains("unknown variant") ? "Daemon update required" : "Comradex unavailable",
                icon: "exclamationmark.triangle.fill"
            )
        } else {
            addInformationalItem("Connecting…", icon: "arrow.triangle.2.circlepath")
        }

        if let error = store.actionErrorMessage {
            addInformationalItem("Account change failed", icon: "exclamationmark.triangle.fill")
            menu.items.last?.toolTip = error
        }
        menu.addItem(.separator())
        menu.addItem(actionItem(
            title: store.isRefreshing ? "Refreshing…" : "Refresh",
            icon: "arrow.clockwise",
            action: #selector(refreshSelected(_:)),
            keyEquivalent: "r",
            enabled: !store.isRefreshing
        ))
        menu.addItem(actionItem(
            title: "Quit Comradex",
            action: #selector(quitSelected(_:)),
            keyEquivalent: "q"
        ))
    }

    private func addStatus(_ snapshot: UIStatusSnapshot) {
        if snapshot.pools.isEmpty && snapshot.accounts.isEmpty {
            addInformationalItem("No pools or accounts configured", icon: "tray")
            return
        }

        let showsPoolHeaders = snapshot.pools.count > 1
        for pool in snapshot.pools {
            if showsPoolHeaders {
                menu.addItem(.sectionHeader(title: pool.name))
            }
            for member in pool.members {
                guard let account = snapshot.accounts.first(where: { $0.name == member }) else {
                    addInformationalItem("\(member) · Unavailable", icon: "questionmark.circle")
                    continue
                }
                addAccount(account, pool: pool)
            }
        }

        if snapshot.accounts.isEmpty {
            addInformationalItem("No accounts configured", icon: "tray")
        }

        let renderedAccountNames = Set(snapshot.pools.flatMap(\.members))
        for account in snapshot.accounts where !renderedAccountNames.contains(account.name) {
            addAccount(account, pool: nil)
        }
    }

    private func addAccount(_ account: AccountSnapshot, pool: PoolSnapshot?) {
        let isPreferred = pool?.preferred == account.name
        let isLastUsed = pool?.active == account.name
        let item = NSMenuItem(
            title: account.name,
            action: pool == nil ? nil : #selector(preferredAccountSelected(_:)),
            keyEquivalent: ""
        )
        item.target = self
        let detail = accountDetail(account)
        if isLastUsed && !isPreferred {
            item.image = NSImage(systemSymbolName: "circle.fill", accessibilityDescription: "Last used · \(detail)")
        }
        item.toolTip = [isPreferred ? "Preferred" : nil, accountDetail(account, expanded: true)]
            .compactMap { $0 }.joined(separator: " · ")
        if !detail.isEmpty {
            item.title = "\(account.name) · \(detail)"
        }
        item.state = isPreferred ? .on : .off
        item.isEnabled = pool != nil && store.updatingPool == nil && store.connectingAccount == nil
        if let pool {
            item.representedObject = PreferredAccountAction(pool: pool.name, account: account.name)
        }
        menu.addItem(item)

        if account.isInbound {
            let connect = actionItem(
                title: store.connectingAccount == account.name ? "Connecting Codex login…" : "Connect existing Codex login…",
                icon: "person.crop.circle.badge.checkmark",
                action: #selector(connectExistingLoginSelected(_:)),
                enabled: store.connectingAccount == nil && !store.isLoginRunning && store.updatingPool == nil
            )
            connect.representedObject = account.name
            connect.indentationLevel = 1
            connect.toolTip = "Reuse your local Codex login and show its usage."
            menu.addItem(connect)
        }

        if account.needsLoginAction {
            let login = actionItem(
                title: "Re-login \(account.name)…",
                icon: "person.crop.circle.badge.exclamationmark",
                action: #selector(reloginSelected(_:)),
                enabled: !store.isLoginRunning && store.connectingAccount == nil
            )
            login.representedObject = account.name
            login.indentationLevel = 1
            menu.addItem(login)
        }
    }

    private func addInformationalItem(_ title: String, icon: String) {
        let item = NSMenuItem(title: title, action: nil, keyEquivalent: "")
        item.image = NSImage(systemSymbolName: icon, accessibilityDescription: nil)
        menu.addItem(item)
    }

    private func actionItem(
        title: String,
        icon: String? = nil,
        action: Selector,
        keyEquivalent: String = "",
        enabled: Bool = true
    ) -> NSMenuItem {
        let item = NSMenuItem(title: title, action: action, keyEquivalent: keyEquivalent)
        item.target = self
        item.isEnabled = enabled
        if let icon {
            item.image = NSImage(systemSymbolName: icon, accessibilityDescription: nil)
        }
        return item
    }

    private func refreshStatus() {
        guard refreshTask == nil else { return }
        refreshTask = Task { [weak self] in
            guard let self else { return }
            await store.refresh()
            refreshTask = nil
            if isMenuOpen {
                hasDeferredMenuUpdate = true
            } else {
                rebuildMenu()
            }
        }
    }

    @objc private func refreshSelected(_ sender: NSMenuItem) {
        refreshStatus()
    }

    @objc private func quitSelected(_ sender: NSMenuItem) {
        NSApplication.shared.terminate(nil)
    }

    @objc private func preferredAccountSelected(_ sender: NSMenuItem) {
        guard let selection = sender.representedObject as? PreferredAccountAction else { return }
        Task { [weak self] in
            guard let self else { return }
            await store.setPreferred(pool: selection.pool, account: selection.account)
            rebuildMenu()
        }
    }

    @objc private func reloginSelected(_ sender: NSMenuItem) {
        guard let account = sender.representedObject as? String else { return }
        store.beginLogin(account: account)
        showLoginWindow()
    }

    @objc private func connectExistingLoginSelected(_ sender: NSMenuItem) {
        guard let account = sender.representedObject as? String else { return }
        let alert = NSAlert()
        alert.messageText = "Connect existing Codex login to \(account)?"
        alert.informativeText = "Reuse the local Codex login to show usage and route requests with that account. No tokens are copied and your preferred account stays the same. Comradex will reload, interrupting active requests."
        alert.addButton(withTitle: "Connect Login")
        alert.addButton(withTitle: "Cancel")
        NSApplication.shared.activate(ignoringOtherApps: true)
        guard alert.runModal() == .alertFirstButtonReturn else { return }
        Task { [weak self] in
            guard let self else { return }
            await store.connectExistingLogin(account: account)
            rebuildMenu()
        }
    }

    private func showLoginWindow() {
        if loginWindowController == nil {
            let window = NSWindow(
                contentRect: NSRect(x: 0, y: 0, width: 440, height: 300),
                styleMask: [.titled, .closable],
                backing: .buffered,
                defer: false
            )
            window.title = "Comradex Account Login"
            window.isReleasedWhenClosed = false
            window.contentViewController = NSHostingController(
                rootView: LoginWindowView { [weak window] in window?.close() }
                    .environmentObject(store)
            )
            loginWindowController = NSWindowController(window: window)
        }
        loginWindowController?.window?.center()
        loginWindowController?.showWindow(nil)
        NSApplication.shared.activate(ignoringOtherApps: true)
    }

    private var connectionLabel: String {
        if let error = store.errorMessage, store.snapshot == nil {
            return error.contains("unknown variant") ? "Update required" : "Unavailable"
        }
        if store.errorMessage != nil { return "Reconnecting · showing last update" }
        if store.snapshot?.daemonRunning == true { return "Running" }
        return store.isRefreshing ? "Connecting" : "Unavailable"
    }

    private var connectionIcon: String {
        connectionLabel == "Running" ? "checkmark.circle.fill" : "exclamationmark.circle.fill"
    }

    private func accountState(_ account: AccountSnapshot, expanded: Bool) -> String {
        if !account.available {
            switch account.unavailableReason?.lowercased() {
            case "quota":
                if expanded, let retry = retryDescription(account.retryAtUnix) {
                    return "Rate limited · retry in \(retry)"
                }
                if expanded { return "Rate limited" }
                // Show the exhausted window, even when a different primary window has quota.
                let reset = account.usageWindows.values
                    .filter { ($0.usedPercent ?? 0) >= 100 && $0.limitWindowSeconds != 0 }
                    .compactMap(\.resetAtUnix)
                    .filter { $0 > Int64(Date().timeIntervalSince1970) }
                    .max() ?? account.retryAtUnix
                return retryDescription(reset).map { "0% left · \($0)" } ?? "0% left"
            case "temporary_failure": return "Temporarily unavailable"
            case "login_in_progress": return "Login in progress"
            case "needs_login": return "Sign-in required"
            default: return "Unavailable"
            }
        }
        switch account.authState?.lowercased() {
        case "login_in_progress": return "Login in progress"
        case "inbound": return "Requesting client’s login"
        case "signed_in": return "Signed in"
        case "signed_out": return "Sign-in required"
        default: return account.isSignedIn ? "Signed in" : "Sign-in required"
        }
    }

    private func accountDetail(_ account: AccountSnapshot, expanded: Bool = false) -> String {
        if account.reauthRequired { return "Sign-in needed for renewal" }
        if !account.available
            || account.authState?.lowercased() == "login_in_progress"
            || account.authState?.lowercased() == "signed_out"
        {
            return accountState(account, expanded: expanded)
        }
        if account.isInbound { return "Requesting client’s login" }
        return usageDescription(account, expanded: expanded) ?? "Usage pending"
    }

    private func usageDescription(_ account: AccountSnapshot, expanded: Bool) -> String? {
        let windows = account.usageWindows
            .filter { $0.value.usedPercent != nil && $0.value.limitWindowSeconds != 0 }
            .sorted { left, right in
                let leftOrder = windowOrder(left.key)
                let rightOrder = windowOrder(right.key)
                if leftOrder != rightOrder { return leftOrder < rightOrder }
                return left.key < right.key
            }
        guard let window = windows.first?.value else {
            return account.usagePercent.map { "\(max(0, 100 - $0))% left" }
        }
        var detail = "\(max(0, 100 - window.usedPercent!))% left"
        if let reset = retryDescription(window.resetAtUnix) {
            detail += expanded ? " · resets in \(reset)" : " · \(reset)"
        }
        return detail
    }

    private func windowOrder(_ name: String) -> Int {
        switch name.lowercased() {
        case "primary": return 0
        case "secondary": return 1
        case "tertiary": return 2
        default: return 3
        }
    }

    private func retryDescription(_ retryAtUnix: Int64?) -> String? {
        guard let retryAtUnix else { return nil }
        let now = Int64(Date().timeIntervalSince1970)
        let seconds = retryAtUnix - now
        guard seconds > 0 else { return nil }
        if seconds < 60 { return "\(seconds)s" }
        if seconds < 3_600 { return "\(seconds / 60)m \(seconds % 60)s" }
        if seconds < 86_400 { return "\(seconds / 3_600)h \((seconds % 3_600) / 60)m" }
        return "\(seconds / 86_400)d \((seconds % 86_400) / 3_600)h"
    }
}
