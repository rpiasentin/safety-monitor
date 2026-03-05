#!/usr/bin/env bash
set -euo pipefail

# Post-deploy guard for CT104:
# - normalizes runtime-writable file ownership (config.yaml)
# - restarts service
# - runs required health/smoke checks
#
# Usage:
#   ./tools/ct104_post_deploy_guard.sh
#
# Optional overrides:
#   SM_CT104_HOST=192.168.2.105
#   SM_CT104_USER=root
#   SM_CT104_KEY=/abs/path/to/key

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SM_CT104_HOST="${SM_CT104_HOST:-192.168.2.105}"
SM_CT104_USER="${SM_CT104_USER:-root}"

_default_key_1="/Users/rpias/dev/vscode-dev-env/.notes_access/ssh/ct104_root_ed25519"
_default_key_2="$HOME/dev/vscode-dev-env/.notes_access/ssh/ct104_root_ed25519"
_default_key_3="$HOME/.ssh/ct104_root_ed25519"

if [[ -n "${SM_CT104_KEY:-}" ]]; then
  CT104_KEY="$SM_CT104_KEY"
elif [[ -f "$_default_key_1" ]]; then
  CT104_KEY="$_default_key_1"
elif [[ -f "$_default_key_2" ]]; then
  CT104_KEY="$_default_key_2"
else
  CT104_KEY="$_default_key_3"
fi

if [[ ! -f "$CT104_KEY" ]]; then
  echo "ERROR: CT104 key not found at $CT104_KEY" >&2
  exit 1
fi

chmod 600 "$CT104_KEY" || true

ssh -i "$CT104_KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=12 \
  "${SM_CT104_USER}@${SM_CT104_HOST}" \
  'set -euo pipefail
   cd /opt/safety-monitor/app
   chown safetymon:safetymon config.yaml
   systemctl restart safety-monitor
   systemctl is-active safety-monitor
   echo "HEAD=$(git rev-parse --short HEAD)"
   echo "CONFIG_OWNER=$(stat -c "%U:%G" config.yaml)"
   curl -fsS http://127.0.0.1:8000/api/status | python3 -m json.tool >/dev/null
   echo "API_STATUS=ok"
   curl -fsS http://127.0.0.1:8000/api/system/health | python3 -m json.tool | head -n 20
   echo "UI_SMOKE:"
   curl -fsS http://127.0.0.1:8000/ | grep -n "All temperatures\|Container Health\|Reboot Container"'
