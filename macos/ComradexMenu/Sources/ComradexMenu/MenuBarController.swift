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
        if let statusItem {
            NSStatusBar.system.removeStatusItem(statusItem)
        }
    }

    func start() {
        let statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        self.statusItem = statusItem
        statusItem.button?.image = StatusIcon.image
        statusItem.button?.toolTip = "Comradex"
        menu.delegate = self
        statusItem.menu = menu
        rebuildMenu()
        refreshStatus()
    }

    func menuWillOpen(_ menu: NSMenu) {
        isMenuOpen = true
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
            header.title = "Comradex — \(connectionLabel)"
        }
        header.image = NSImage(systemSymbolName: connectionIcon, accessibilityDescription: connectionLabel)
        menu.addItem(header)
        menu.addItem(.separator())

        if let snapshot = store.snapshot {
            if store.errorMessage != nil {
                addInformationalItem("Status may be out of date", icon: "exclamationmark.triangle.fill")
            }
            addStatus(snapshot)
        } else if let error = store.errorMessage {
            addInformationalItem(
                error.contains("unknown variant") ? "Daemon update required" : "Comradex unavailable",
                icon: "exclamationmark.triangle.fill"
            )
        } else {
            addInformationalItem("Connecting…", icon: "arrow.triangle.2.circlepath")
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
                    addInformationalItem("\(member) — Unavailable", icon: "questionmark.circle")
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
        let detailParts = [isPreferred ? "Preferred" : nil, detail]
            .compactMap { $0 }
        let subtitle = detailParts.joined(separator: " · ")
        if #available(macOS 14.4, *) {
            item.subtitle = subtitle
        } else {
            item.title = "\(account.name) — \(subtitle)"
        }
        item.state = isPreferred ? .on : .off
        item.isEnabled = pool != nil && store.updatingPool == nil
        if let pool {
            item.representedObject = PreferredAccountAction(pool: pool.name, account: account.name)
        }
        menu.addItem(item)

        if account.needsLoginAction {
            let login = actionItem(
                title: "Re-login \(account.name)…",
                icon: "person.crop.circle.badge.exclamationmark",
                action: #selector(reloginSelected(_:)),
                enabled: !store.isLoginRunning
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
        if store.errorMessage != nil { return "Stale" }
        if store.snapshot?.daemonRunning == true { return "Running" }
        return store.isRefreshing ? "Connecting" : "Unavailable"
    }

    private var connectionIcon: String {
        connectionLabel == "Running" ? "checkmark.circle.fill" : "exclamationmark.circle.fill"
    }

    private func accountState(_ account: AccountSnapshot) -> String {
        if !account.available {
            switch account.unavailableReason?.lowercased() {
            case "quota":
                if let retry = retryDescription(account.retryAtUnix) {
                    return "Rate limited · retry in \(retry)"
                }
                return "Rate limited"
            case "temporary_failure": return "Temporarily unavailable"
            case "login_in_progress": return "Login in progress"
            case "needs_login": return "Sign-in required"
            default: return "Unavailable"
            }
        }
        switch account.authState?.lowercased() {
        case "login_in_progress": return "Login in progress"
        case "inbound": return "Codex App account"
        case "signed_in": return "Signed in"
        case "signed_out": return "Sign-in required"
        default: return account.isSignedIn ? "Signed in" : "Sign-in required"
        }
    }

    private func accountDetail(_ account: AccountSnapshot) -> String {
        if account.reauthRequired { return "Sign-in needed for renewal" }
        if !account.available
            || account.authState?.lowercased() == "login_in_progress"
            || account.authState?.lowercased() == "signed_out"
        {
            return accountState(account)
        }
        if account.isInbound { return "Codex App account" }
        return usageDescription(account) ?? "Usage pending"
    }

    private func usageDescription(_ account: AccountSnapshot) -> String? {
        let windows = account.usageWindows
            .filter { $0.value.usedPercent != nil }
            .sorted { left, right in
                let leftDuration = left.value.limitWindowSeconds ?? UInt64.max
                let rightDuration = right.value.limitWindowSeconds ?? UInt64.max
                if leftDuration != rightDuration { return leftDuration < rightDuration }
                return windowOrder(left.key) < windowOrder(right.key)
            }
        if windows.isEmpty {
            return account.usagePercent.map { "\(max(0, 100 - $0))% left" }
        }
        return windows.map { name, window in
            let remaining = max(0, 100 - window.usedPercent!)
            var detail = "\(windowLabel(name, seconds: window.limitWindowSeconds)) \(remaining)% left"
            if let reset = retryDescription(window.resetAtUnix) {
                detail += " (resets in \(reset))"
            }
            return detail
        }.joined(separator: " · ")
    }

    private func windowOrder(_ name: String) -> Int {
        switch name.lowercased() {
        case "primary": return 0
        case "secondary": return 1
        case "tertiary": return 2
        default: return 3
        }
    }

    private func windowLabel(_ name: String, seconds: UInt64?) -> String {
        guard let seconds else { return name.capitalized }
        if seconds.isMultiple(of: 86_400) { return "\(seconds / 86_400)d" }
        if seconds.isMultiple(of: 3_600) { return "\(seconds / 3_600)h" }
        return name.capitalized
    }

    private func retryDescription(_ retryAtUnix: Int64?) -> String? {
        guard let retryAtUnix else { return nil }
        let now = Int64(Date().timeIntervalSince1970)
        let seconds = max(0, retryAtUnix - now)
        if seconds < 60 { return "\(seconds)s" }
        if seconds < 3_600 { return "\(seconds / 60)m \(seconds % 60)s" }
        return "\(seconds / 3_600)h \((seconds % 3_600) / 60)m"
    }
}
