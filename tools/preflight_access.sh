#!/usr/bin/env bash
set -euo pipefail

# Safety Monitor zero-friction preflight:
# - verifies local git/remote health
# - verifies GitHub reachability/auth for origin
# - verifies CT104 SSH + service + local API health
#
# Usage:
#   ./tools/preflight_access.sh
#
# Optional environment overrides:
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

EXPECTED_REMOTE_HTTPS="https://github.com/rpiasentin/safety-monitor.git"
EXPECTED_REMOTE_SSH="git@github.com:rpiasentin/safety-monitor.git"

PASS_COUNT=0
FAIL_COUNT=0

pass() {
  printf 'PASS: %s\n' "$1"
  PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
  printf 'FAIL: %s\n' "$1"
  FAIL_COUNT=$((FAIL_COUNT + 1))
}

section() {
  printf '\n=== %s ===\n' "$1"
}

run_with_timeout() {
  local seconds="$1"
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$seconds" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then
    gtimeout "$seconds" "$@"
  else
    "$@"
  fi
}

require_cmd() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    pass "command '$cmd' found"
  else
    fail "command '$cmd' missing"
  fi
}

section "Tooling"
require_cmd git
require_cmd ssh
require_cmd curl
require_cmd python3

section "Repository"
if [[ -d .git ]]; then
  pass "git repo detected at $ROOT_DIR"
else
  fail "not a git repo: $ROOT_DIR"
fi

BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
if [[ "$BRANCH" == "main" ]]; then
  pass "on branch main"
else
  fail "expected branch main, got '$BRANCH'"
fi

if git diff --quiet && git diff --cached --quiet; then
  pass "working tree clean"
else
  fail "working tree dirty (commit/stash before handoff)"
fi

REMOTE_URL="$(git remote get-url origin 2>/dev/null || true)"
if [[ "$REMOTE_URL" == "$EXPECTED_REMOTE_HTTPS" || "$REMOTE_URL" == "$EXPECTED_REMOTE_SSH" ]]; then
  pass "origin remote is Safety Monitor repo ($REMOTE_URL)"
else
  fail "origin remote unexpected: '$REMOTE_URL'"
fi

section "GitHub Access"
if run_with_timeout 20 git ls-remote --heads origin main >/dev/null 2>&1; then
  pass "origin/main reachable via git ls-remote"
else
  fail "cannot reach origin/main (GitHub auth/network issue)"
fi

if command -v gh >/dev/null 2>&1; then
  if run_with_timeout 15 gh auth status -h github.com >/dev/null 2>&1; then
    pass "gh auth status is valid for github.com"
  else
    fail "gh installed but not authenticated for github.com"
  fi
else
  fail "gh CLI missing (optional but recommended for agent workflows)"
fi

section "CT104 Access"
if [[ -f "$CT104_KEY" ]]; then
  pass "CT104 SSH key exists at $CT104_KEY"
else
  fail "CT104 SSH key not found at $CT104_KEY"
fi

if [[ -f "$CT104_KEY" ]]; then
  chmod 600 "$CT104_KEY" || true
  if run_with_timeout 20 ssh -i "$CT104_KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8 \
      "${SM_CT104_USER}@${SM_CT104_HOST}" 'exit 0' >/dev/null 2>&1; then
    pass "SSH batch login to CT104 works"
  else
    fail "SSH batch login to CT104 failed"
  fi
fi

section "CT104 Runtime"
if [[ -f "$CT104_KEY" ]]; then
  REMOTE_CHECK_OUTPUT="$(
    run_with_timeout 25 ssh -i "$CT104_KEY" -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=8 \
      "${SM_CT104_USER}@${SM_CT104_HOST}" \
      'set -euo pipefail
       cd /opt/safety-monitor/app
       echo "remote_head=$(git rev-parse --short HEAD)"
       echo "service=$(systemctl is-active safety-monitor)"
       curl -fsS http://127.0.0.1:8000/api/status >/dev/null
       echo "api_status=ok"'
  )" || true

  if [[ "$REMOTE_CHECK_OUTPUT" == *"remote_head="* ]]; then
    pass "CT104 app repo reachable"
    printf '%s\n' "$REMOTE_CHECK_OUTPUT"
  else
    fail "CT104 app repo/runtime check failed"
  fi
fi

section "Summary"
printf 'Passed: %d\n' "$PASS_COUNT"
printf 'Failed: %d\n' "$FAIL_COUNT"

if (( FAIL_COUNT > 0 )); then
  cat <<'TXT'
Preflight failed.
Recommended fixes:
1) GitHub auth:
   gh auth login -h github.com -p https --web
   gh auth setup-git
2) CT104 key login test:
   ssh -i /Users/rpias/dev/vscode-dev-env/.notes_access/ssh/ct104_root_ed25519 -o IdentitiesOnly=yes root@192.168.2.105
3) Verify repo remote:
   git remote set-url origin https://github.com/rpiasentin/safety-monitor.git
TXT
  exit 1
fi

echo "Preflight passed."
