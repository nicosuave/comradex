#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"

version="${1:-$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)}"
tag="v${version}"
notary_profile="${NOTARY_PROFILE:-sidequery-notarization}"
artifacts="$root/target/local-release/$tag"

manifest_version="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)"
if [[ "$version" != "$manifest_version" ]]; then
  echo "Requested $version but Cargo.toml is $manifest_version" >&2
  exit 1
fi
if ! git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  echo "Missing local tag $tag" >&2
  exit 1
fi
if [[ "$(git rev-parse HEAD)" != "$(git rev-parse "$tag^{commit}")" ]]; then
  echo "HEAD must be the commit tagged $tag" >&2
  exit 1
fi
if ! git diff --quiet HEAD -- Cargo.toml Cargo.lock src; then
  echo "Refusing to release with uncommitted source changes" >&2
  exit 1
fi
if ! gh release view "$tag" >/dev/null 2>&1; then
  echo "GitHub release $tag does not exist yet; wait for the Linux release workflow" >&2
  exit 1
fi

identity="${CODESIGN_IDENTITY:-}"
if [[ -z "$identity" ]]; then
  identity="$(security find-identity -v -p codesigning | sed -n 's/.*"\(Developer ID Application:.*\)"/\1/p' | head -n 1)"
fi
if [[ -z "$identity" ]]; then
  echo "No Developer ID Application identity found in the local Keychain" >&2
  exit 1
fi
xcrun notarytool history --keychain-profile "$notary_profile" --output-format json >/dev/null

mkdir -p "$artifacts"

build_package_notarize() {
  local target="$1"
  local arch="$2"
  local executable="$root/target/$target/release/comradex"
  local artifact="comradex-${version}-macos-${arch}.tar.gz"
  local notary_zip="$artifacts/comradex-${version}-macos-${arch}-notarization.zip"

  cargo build --locked --release --target "$target"
  codesign --force --options runtime --timestamp \
    --sign "$identity" \
    --identifier com.nicosuave.comradex \
    "$executable"
  codesign --verify --strict --verbose=2 "$executable"

  ditto -c -k --keepParent "$executable" "$notary_zip"
  xcrun notarytool submit "$notary_zip" \
    --keychain-profile "$notary_profile" \
    --wait

  tar -C "$(dirname "$executable")" -czf "$artifacts/$artifact" comradex
  (cd "$artifacts" && shasum -a 256 "$artifact" > "$artifact.sha256")
  rm -f "$notary_zip"
}

build_package_notarize aarch64-apple-darwin arm64
build_package_notarize x86_64-apple-darwin x86_64

gh release upload "$tag" \
  "$artifacts/comradex-${version}-macos-arm64.tar.gz" \
  "$artifacts/comradex-${version}-macos-arm64.tar.gz.sha256" \
  "$artifacts/comradex-${version}-macos-x86_64.tar.gz" \
  "$artifacts/comradex-${version}-macos-x86_64.tar.gz.sha256" \
  --clobber

update_homebrew() {
  local workflow="update-formula.yml"
  local previous_runs new_run candidate candidate_log
  previous_runs="$(
    gh run list -R nicosuave/homebrew-tap \
      --workflow "$workflow" \
      --event repository_dispatch \
      --limit 100 \
      --json databaseId \
      --jq '.[].databaseId'
  )"
  gh api --method POST repos/nicosuave/homebrew-tap/dispatches \
    -f event_type=update-formula \
    -F "client_payload[formula]=comradex" \
    -F "client_payload[version]=$version" \
    -F "client_payload[repo]=nicosuave/comradex"

  new_run=""
  for _ in {1..60}; do
    while IFS= read -r candidate; do
      [[ -n "$candidate" ]] || continue
      if grep -Fqx "$candidate" <<<"$previous_runs"; then
        continue
      fi
      # repository_dispatch does not return a run ID. Filter to the exact
      # workflow/event, then correlate concurrent dispatches using the payload
      # echoed by this workflow before deciding which run to trust.
      candidate_log="$(
        gh run view "$candidate" -R nicosuave/homebrew-tap --log 2>/dev/null || true
      )"
      if grep -Fq "Updating comradex to $version" <<<"$candidate_log"; then
        new_run="$candidate"
        break
      fi
    done < <(
      gh run list -R nicosuave/homebrew-tap \
        --workflow "$workflow" \
        --event repository_dispatch \
        --limit 20 \
        --json databaseId \
        --jq '.[].databaseId'
    )
    [[ -n "$new_run" ]] && break
    sleep 2
  done
  if [[ -z "$new_run" ]]; then
    echo "Timed out waiting for the Homebrew update workflow" >&2
    return 1
  fi
  gh run watch "$new_run" -R nicosuave/homebrew-tap --exit-status
}

if ! update_homebrew; then
  echo "Homebrew update failed; retrying after GitHub's asset CDN settles" >&2
  sleep 10
  update_homebrew
fi

echo "Published signed and notarized macOS assets for $tag"
