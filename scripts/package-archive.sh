#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 || "${1##*/}" != "comradex" || ! -x "$1" ]]; then
  echo "Usage: $0 /path/to/comradex /path/to/archive.tar.gz" >&2
  exit 1
fi

root="$(cd "$(dirname "$0")/.." && pwd)"
tar -czf "$2" -C "$(dirname "$1")" comradex \
  -C "$root" claudex/LICENSE claudex/NOTICE claudex/licenses
