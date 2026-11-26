#!/bin/sh
set -eu
while [ "$1" = -c ]; do printf '%s\n' "$2" >> "$CODEX_HOME/overrides"; shift 2; done
test "$1" = app-server
test "$2" = --stdio
printf '%s' "$$" > "$CODEX_HOME/pid"
if [ -e "$CODEX_HOME/fail-start" ]; then printf 'SENSITIVE_STDERR_SENTINEL\n' >&2; exit 19; fi
while IFS= read -r line; do
  id=$(expr "$line" : '.*"id":"\([^"]*\)".*' || true)
  case "$line" in
    *'"method":"initialize"'*)
      printf '%s\n' "$line" > "$CODEX_HOME/initialize.json"
      printf '{"id":"%s","result":{"userAgent":"genuine-fixture"}}\n' "$id"
      ;;
    *'"method":"initialized"'*) : > "$CODEX_HOME/initialized" ;;
    *'"method":"echo"'*) printf '{"id":"%s","result":%s}\n' "$id" "$line" ;;
    *'"method":"callback"'*)
      callback="$id"
      printf '{"method":"mcpServer/startupStatus/updated","params":{"name":"fixture","status":"ready"}}\n'
      printf '{"id":700,"method":"mcpServer/elicitation/request","params":{"threadId":"sidecar-thread","serverName":"fixture","message":"Choose"}}\n'
      ;;
    *'"id":700'*) printf '{"id":"%s","result":%s}\n' "$callback" "$line" ;;
    *'"method":"hold"'*) held="$id"; : > "$CODEX_HOME/held" ;;
    *'"method":"release"'*)
      printf '{"id":"%s","result":"released"}\n' "$held"
      printf '{"id":"%s","result":"release-ack"}\n' "$id"
      ;;
    *'"method":"rpc-error"'*) printf '{"id":"%s","error":{"code":-32077,"message":"fixture refused","data":{"reason":"policy"}}}\n' "$id" ;;
    *'"method":"malformed"'*) printf 'SENSITIVE_INVALID_FRAME\n' ;;
    *'"method":"exit"'*) printf 'SENSITIVE_STDERR_SENTINEL\n' >&2; exit 12 ;;
    *'"method":"flood"'*)
      count=0
      while [ "$count" -lt 256 ]; do printf '{"method":"fixture/event","params":{}}\n'; count=$((count+1)); done
      ;;
    *) exit 22 ;;
  esac
done
