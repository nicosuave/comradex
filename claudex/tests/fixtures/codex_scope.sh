#!/bin/sh
# Genuine Codex stand-in that records the hooks, MCP status and feedback requests it receives.
set -eu
while [ "$1" = -c ]; do shift 2; done
test "$1" = app-server
while IFS= read -r line; do
  id=$(expr "$line" : '.*"id":"\([^"]*\)".*' || true)
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{"userAgent":"scope-fixture"}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"hooks/list"'*)
      printf '%s\n' "$line" >> "$CODEX_HOME/requests"
      printf '{"id":"%s","result":{"data":[]}}\n' "$id"
      ;;
    *'"method":"mcpServerStatus/list"'*)
      printf '%s\n' "$line" >> "$CODEX_HOME/requests"
      printf '{"id":"%s","result":{"data":[],"nextCursor":null}}\n' "$id"
      ;;
    *'"method":"feedback/save"'*)
      printf '%s\n' "$line" >> "$CODEX_HOME/requests"
      path=$(expr "$line" : '.*"savePath":"\([^"]*\)".*')
      : > "$path"
      printf '{"id":"%s","result":{"savedPath":"%s","threadId":"codex-feedback","promptHash":null}}\n' "$id" "$path"
      ;;
    *) exit 22 ;;
  esac
done
