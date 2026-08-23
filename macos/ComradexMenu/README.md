# Comradex Menu

A native macOS 14+ menu-bar companion for viewing Comradex daemon, routing, account, and pool status; choosing a pool's preferred account (or automatic selection); and completing account login or re-login.

The app talks directly to the daemon's newline-delimited JSON protocol at `~/.config/comradex/state/control.sock`. It does not invoke the Comradex CLI, read configuration files, expose subprocess output, or handle credentials. Device login polling uses the daemon-issued random session ID and displays only the verification URI, user code, coarse state, and safe error text. Set `COMRADEX_CONTROL_SOCKET` before launching to use another socket path.

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

The running daemon must implement `ui_status`, `ui_set_preferred`, `ui_start_login`, and `ui_login_status` on its existing user-only control socket.

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
