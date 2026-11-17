#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd -P)"
cd "$root"

local_only=false
if [[ "${1:-}" == "--local" ]]; then
  local_only=true
  shift
fi
if [[ "$#" -gt 1 ]]; then
  echo "Usage: $0 [--local] [version]" >&2
  exit 1
fi

version="${1:-$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)}"
tag="v${version}"
notary_profile="${NOTARY_PROFILE:-sidequery-notarization}"
cargo_target_root="${CARGO_TARGET_DIR:-$root/target}"
build_root="$root"
head_sha="$(git rev-parse HEAD)"
if [[ "$local_only" == true ]]; then
  if [[ "$(uname -s)" != "Darwin" ]]; then
    echo "Local signed builds require macOS" >&2
    exit 1
  fi
  tag="local-${version}-${head_sha:0:12}"
fi
artifacts="$root/target/local-release/$tag"

manifest_version="$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)"
if [[ "$version" != "$manifest_version" ]]; then
  echo "Requested $version but Cargo.toml is $manifest_version" >&2
  exit 1
fi
if [[ "$local_only" == false ]] && ! git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
  echo "Missing local tag $tag" >&2
  exit 1
fi
if [[ "$local_only" == false && "$head_sha" != "$(git rev-parse "$tag^{commit}")" ]]; then
  echo "HEAD must be the commit tagged $tag" >&2
  exit 1
fi
if ! git diff --quiet HEAD -- Cargo.toml Cargo.lock src macos scripts tests; then
  echo "Refusing to release with uncommitted macOS source changes" >&2
  exit 1
fi
if [[ "$local_only" == false ]] && ! gh release view "$tag" >/dev/null 2>&1; then
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
if [[ "$local_only" == true ]]; then
  build_root="$(mktemp -d "${TMPDIR:-/tmp}/comradex-release-source.XXXXXX")"
  build_root="$(cd "$build_root" && pwd -P)"
  trap 'rm -rf "$build_root"' EXIT
  case "$build_root/" in
    "$root/"*) echo "Temporary source directory must be outside the checkout" >&2; exit 1 ;;
  esac
  git archive "$head_sha" | tar -x -C "$build_root"
fi

normalize_runtime_dependencies() {
  local executable="$1"
  local dependencies dependency
  dependencies="$(xcrun otool -L "$executable")"
  while IFS= read -r dependency; do
    case "$dependency" in
      /nix/store/*/lib/libiconv.2.dylib)
        xcrun install_name_tool -change "$dependency" /usr/lib/libiconv.2.dylib "$executable"
        ;;
      /System/Library/*|/usr/lib/*) ;;
      *)
        echo "Unsupported runtime dependency: $dependency" >&2
        return 1
        ;;
    esac
  done < <(printf '%s\n' "$dependencies" | awk 'NR > 1 {print $1}')
}

notarize_package() {
  local package="$1"
  local result="${package%.zip}.json"
  xcrun notarytool submit "$package" \
    --keychain-profile "$notary_profile" \
    --wait --output-format json > "$result"
  if [[ "$(plutil -extract status raw -o - "$result")" != "Accepted" ]]; then
    echo "Apple notarization did not accept $package; see $result" >&2
    return 1
  fi
}

build_package_notarize() {
  local target="$1"
  local arch="$2"
  local executable="$cargo_target_root/$target/release/comradex"
  local artifact="comradex-${version}-macos-${arch}.tar.gz"
  local notary_zip="$artifacts/comradex-${version}-macos-${arch}-notarization.zip"
  rm -f "$artifacts/$artifact" "$artifacts/$artifact.sha256"

  (
    cd "$build_root"
    CARGO_TARGET_DIR="$cargo_target_root" cargo build --locked --release \
      --manifest-path "$build_root/Cargo.toml" --target "$target"
  )
  normalize_runtime_dependencies "$executable"
  codesign --force --options runtime --timestamp \
    --sign "$identity" \
    --identifier com.nicosuave.comradex \
    "$executable"
  codesign --verify --strict --verbose=2 "$executable"

  ditto -c -k --keepParent "$executable" "$notary_zip"
  notarize_package "$notary_zip"

  tar -C "$(dirname "$executable")" -czf "$artifacts/$artifact" comradex
  (cd "$artifacts" && shasum -a 256 "$artifact" > "$artifact.sha256")
  rm -f "$notary_zip"
}

if [[ "$local_only" == true ]]; then
  case "$(uname -m)" in
    arm64|aarch64) build_package_notarize aarch64-apple-darwin arm64 ;;
    x86_64) build_package_notarize x86_64-apple-darwin x86_64 ;;
    *) echo "Unsupported local build architecture" >&2; exit 1 ;;
  esac
  echo "Signed and notarized local build: $artifacts"
  exit 0
fi

build_package_notarize aarch64-apple-darwin arm64
build_package_notarize x86_64-apple-darwin x86_64

build_package_notarize_app() {
  local app_root="$root/macos/ComradexMenu"
  local app="$app_root/ComradexMenu.app"
  local artifact="comradex-menu-${version}-macos-universal.zip"
  local notary_zip="$artifacts/comradex-menu-${version}-notarization.zip"
  local build_number
  build_number="$(git rev-list --count HEAD)"

  (
    cd "$app_root"
    MARKETING_VERSION="$version" \
      BUILD_NUMBER="$build_number" \
      ARCHES="arm64 x86_64" \
      APP_IDENTITY="$identity" \
      SIGNING_MODE=developer-id \
      Scripts/package_app.sh release
  )
  codesign --verify --deep --strict --verbose=2 "$app"

  rm -f "$notary_zip"
  ditto --norsrc -c -k --keepParent "$app" "$notary_zip"
  notarize_package "$notary_zip"
  xcrun stapler staple "$app"
  xcrun stapler validate "$app"
  spctl -a -t exec -vv "$app"

  ditto --norsrc -c -k --keepParent "$app" "$artifacts/$artifact"
  (cd "$artifacts" && shasum -a 256 "$artifact" > "$artifact.sha256")
  rm -f "$notary_zip"
}

build_package_notarize_app

gh release upload "$tag" \
  "$artifacts/comradex-${version}-macos-arm64.tar.gz" \
  "$artifacts/comradex-${version}-macos-arm64.tar.gz.sha256" \
  "$artifacts/comradex-${version}-macos-x86_64.tar.gz" \
  "$artifacts/comradex-${version}-macos-x86_64.tar.gz.sha256" \
  "$artifacts/comradex-menu-${version}-macos-universal.zip" \
  "$artifacts/comradex-menu-${version}-macos-universal.zip.sha256" \
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
