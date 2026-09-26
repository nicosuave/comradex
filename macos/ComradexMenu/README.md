# Comradex Menu

A native macOS 14+ menu-bar companion for viewing Comradex daemon, routing, account, and pool status; changing each account's routing settings; and completing account login when authentication is required.

Each account occupies one line with its usage and reset countdown, including connected `app` accounts: `app · 17% left · 12h 15m`. The primary quota window is shown; zero-duration windows are omitted. Quota-exhausted accounts retain `0% left` and the exhausted window's reset countdown (falling back to the retry deadline). Other authentication or availability errors replace stale usage details. Tooltips explicitly label reset countdowns.

Top-level account rows have no checkmarks or routing labels. The tooltip identifies the account most recently used for an upstream request (`wired`). A single pool has no section header. Multiple pools get section headers named for their provider, Codex before Claude; when a provider has more than one pool, the header adds the pool name, such as `Codex · work`.

Each account opens a submenu with three choices: Automatic — quota-aware selection, Preferred — use first, and Preserved — use last. Exactly one is checked. Preserved saves that account's quota for use outside Comradex by using other available accounts first. Selecting a choice applies immediately, without a confirmation window. Choosing Preferred stops preserving that account; choosing Preserved removes its preference; Automatic clears either setting for that account. Each pool supports one preferred and one preserved account. Existing conversations keep their bindings. Changes persist and apply live without restarting. Failed changes retain the previous displayed state and show an account-change error.

The app starts directly as an AppKit menu-bar application without creating a Settings window.

Sign In… appears within the submenu when authentication or renewal is needed; it does not change routing. During this app's login flow, Continue Sign In… reopens the same login window. The window distinguishes requesting a code from waiting for authorization, shows the selectable code, and provides Copy Code and browser controls. Starting another attempt clears the previous code. Inbound accounts offer Connect existing Codex login… in the submenu.

The app talks directly to the daemon's newline-delimited JSON protocol at `~/.config/comradex/state/control.sock`. It does not invoke the Comradex CLI, read configuration files, expose subprocess output, or handle credentials. Device login polling uses the daemon-issued random session ID and displays only the verification URI, user code, coarse state, and safe error text. Set `COMRADEX_CONTROL_SOCKET` before launching to use another socket path.

Status refreshes automatically every five seconds and when the menu opens. Only one status request runs at a time. While the menu is open, structural updates wait until it closes to avoid moving actions under the pointer. Failed reads preserve the last snapshot and show “Reconnecting”; the header tooltip includes the last successful refresh time and failure detail. Recovery clears the connection warning automatically. Account-change errors remain separate and clear on the next successful account change. Connection failures and recovery are recorded in macOS unified logging, with error details private.

## Build and test

```sh
cd macos/ComradexMenu
swift build
swift test
```

## Package and run

```sh
cd macos/ComradexMenu
Scripts/package_app.sh
Scripts/compile_and_run.sh
```

The package script always emits an `LSUIElement` menu-bar app (`MENU_BAR_APP=1`) and uses ad-hoc signing unless `APP_IDENTITY` is set. Set `ARCHES="arm64 x86_64"` for a universal build.

The running daemon must implement `ui_status`, `ui_set_account_role`, `ui_start_login`, and `ui_login_status` on its existing user-only control socket.

## CLI coexistence

The menu app is a client of the same daemon and state directory used by the Comradex CLI. It does not install another daemon, replace a Homebrew binary, edit a LaunchAgent, or take ownership of the service. An existing current CLI/Homebrew installation therefore remains the daemon authority, and CLI status and account commands continue to work normally alongside the app.

If the socket belongs to an older daemon that predates the UI protocol, the app reports that the daemon must be updated and restarted. It never starts a competing daemon against the same listeners or state directory.

A future self-contained distribution can place a universal `comradex` helper in `ComradexMenu.app/Contents/Helpers`. Its startup policy should remain compatible with CLI installs:

1. Attach to a compatible daemon already listening on the configured control socket.
2. If an existing service is incompatible, ask the user to update it rather than replacing it.
3. Register the bundled per-user LaunchAgent only when no Comradex service exists.
4. Keep the standard config and state paths so the app and CLI remain interchangeable clients.

## CI and releases

The existing `ci` workflow tests the Rust daemon integration on macOS and Linux. The `macOS app` workflow tests the Swift package, creates an ad-hoc-signed universal app, verifies its architectures and signature, and uploads a zipped build artifact for each pull request and main-branch push.

Tagged releases follow the repository's existing local-signing policy. After the Linux workflow creates the GitHub release, `scripts/release_macos_local.sh` builds the universal app, signs it with the maintainer's Developer ID identity, submits it to Apple notarization, staples the ticket, verifies it with Gatekeeper, and uploads the zip plus SHA-256 checksum. Signing and notarization credentials remain on the maintainer's Mac.
