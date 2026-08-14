# Repository instructions

## macOS releases

- Build, Developer ID-sign, and notarize macOS release artifacts on the maintainer's Mac with `scripts/release_macos_local.sh`.
- Do not export Apple signing certificates or notarization credentials to GitHub Actions secrets unless the maintainer explicitly changes this policy.
- GitHub Actions builds and publishes the Linux artifacts. Wait for that release workflow to create the GitHub release before running the local macOS release script.
- The local script uploads both macOS architectures, verifies the Homebrew tap update workflow, and retries once if GitHub's release-asset CDN is not ready.
