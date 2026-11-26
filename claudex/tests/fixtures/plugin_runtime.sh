#!/bin/sh
set -eu
disabled=0
while [ "$1" = -c ]; do
  printf '%s\n' "$2" >> "$CODEX_HOME/overrides"
  if [ "$2" = 'plugins={"sample@fixture" = { enabled = false }}' ]; then disabled=1; fi
  shift 2
done
test "$1" = app-server
while IFS= read -r line; do
  id=$(expr "$line" : '.*"id":"\([^"]*\)".*' || true)
  case "$line" in
    *'"method":"initialize"'*) printf '{"id":"%s","result":{}}\n' "$id" ;;
    *'"method":"initialized"'*) ;;
    *'"method":"plugin/installed"'*) printf '{"id":"%s","result":{"marketplaces":[{"name":"fixture","path":"/fixture/marketplace.json","plugins":[{"id":"sample@fixture","name":"sample"}]},{"name":"remote","path":null,"plugins":[{"id":"sites@remote","remotePluginId":"remote-sites","name":"sites"}]}]}}\n' "$id" ;;
    *'"method":"plugin/read"'*)
      printf '%s\n' "$line" >> "$CODEX_HOME/reads"
      case "$line" in
        *'"pluginName":"remote-sites"'*) printf '{"id":"%s","result":{"plugin":{"skills":[{"name":"sites-building","path":"/sites/SKILL.md"}],"apps":[{"id":"sites-connector"}],"mcpServers":[]}}}\n' "$id" ;;
        *) printf '{"id":"%s","result":{"plugin":{"skills":[{"name":"sample:fixture-skill","path":"/fixture/SKILL.md"}],"apps":[],"mcpServers":["fixture_server"]}}}\n' "$id" ;;
      esac ;;
    *'"method":"thread/start"'*) printf '{"id":"%s","result":{"thread":{"id":"real-sidecar-id"}}}\n' "$id" ;;
    *'"method":"skills/list"'*)
      if [ "$disabled" = 1 ]; then enabled=false; else enabled=true; fi
      description='Build sites with the full plugin guidance preserved.'
      if [ -f "$CODEX_HOME/description" ]; then description=$(cat "$CODEX_HOME/description"); fi
      printf '{"id":"%s","result":{"data":[{"skills":[{"name":"fixture-skill","pluginId":"sample@fixture","description":"Read fixture instructions","enabled":%s,"path":"/fixture/SKILL.md"},{"name":"sites:sites-building","pluginId":"sites@remote","description":"%s","enabled":true,"path":"/sites/SKILL.md"},{"name":"other:retained","pluginId":"other@remote","enabled":true,"path":"/other/SKILL.md"},{"name":"standalone","description":"Claude owns this skill","enabled":true,"path":"/standalone/SKILL.md"}]}]}}\n' "$id" "$enabled" "$description"
      ;;
    *'"method":"mcpServerStatus/list"'*)
      if [ "$disabled" = 1 ]; then printf '{"id":"%s","result":{"data":[],"nextCursor":null}}\n' "$id"
      else
        case "$line" in
          *'"cursor":"page2"'*) printf '{"id":"%s","result":{"data":[],"nextCursor":null}}\n' "$id" ;;
          *) printf '{"id":"%s","result":{"data":[{"name":"fixture_server","tools":{"qualified-key":{"name":"token","description":"Echo a nonce","inputSchema":{"type":"object","properties":{"nonce":{"type":"string"}},"required":["nonce"],"additionalProperties":false}}}},{"name":"codex_apps","tools":{"sites":{"name":"sites.list_sites","inputSchema":{"type":"object"},"_meta":{"connector_id":"sites-connector","connector_description":"Sites guidance exactly once","ui":{"resourceUri":"ui://sites","visibility":["model","app"]}}},"sites2":{"name":"sites.read_site","inputSchema":{"type":"object"},"_meta":{"connector_id":"sites-connector","connector_description":"Sites guidance exactly once"}},"app-only":{"name":"app_only","inputSchema":{"type":"object"},"_meta":{"ui":{"visibility":["app"]}}},"other":{"name":"other.tool","inputSchema":{"type":"object"},"_meta":{"connector_id":"other-connector"}}}},{"name":"codex_app","tools":{"app_tool":{"name":"app_tool","inputSchema":{"type":"object"}}}}],"nextCursor":"page2"}}\n' "$id" ;;
        esac
      fi
      ;;
    *'"method":"mcpServer/tool/call"'*)
      printf '%s\n' "$line" >> "$CODEX_HOME/calls"
      printf '{"id":"%s","result":{"content":[{"type":"text","text":"Executed fixture"}],"structuredContent":%s}}\n' "$id" "$line"
      ;;
    *) exit 2 ;;
  esac
done
