# Safety Monitor

Live Tailscale-accessible dashboard for 4 properties — solar, battery, temperature, and device health.

## Architecture

```
main.py            FastAPI web app (uvicorn, port 8000)
scheduler.py       APScheduler — polls all sources every 15 min
aggregator.py      Per-property collector runner + merger
db.py              SQLite store (data/safety_monitor.db)
alerts.py          Pushover alerts: low SOC, low temp, offline
formatters.py      Display helpers

collectors/
  eg4.py           EG4 inverter via raw TCP SolarmanV5 banner (port 8000)
  victron.py       Victron Venus OS via MQTT (port 1883)
  ha_api.py        Home Assistant REST API — temps, Tesla, Hubitat via HACS
  hubitat.py       Hubitat cloud API, lock/smoke state extraction, lock commands

app/templates/
  dashboard.html   Tailwind dark dashboard, auto-refreshes 60s
  system_decisions.html  Critical decision/event trace page
```

## Properties

| ID | Name | Solar | HA | Notes |
|----|------|-------|----|-------|
| fm | Forgetmenot | EG4 + Victron | ✓ | 192.168.2.x |
| hc | High Country | — | ✓ | Tesla + Hubitat via HACS |
| lr | Lariat | — | ✓ | Hubitat only, no solar |
| rd | Redwood | — | — | Hubitat cloud API |

## Quick install on CT104

```bash
# From Proxmox host:
pct enter 104

# Inside CT104:
curl -fsSL https://raw.githubusercontent.com/rpiasentin/safety-monitor/main/deploy/install.sh | bash
```

Then edit `/opt/safety-monitor/.env` with your credentials.

## Manual run

```bash
cd /opt/safety-monitor/app
python3 main.py
# Dashboard at http://<tailscale-ip>:8000
```

## Stepwise testing (run each, confirm output before next)

```bash
# 1. EG4 — should print voltage ~53V, SOC ~57%
python3 -c "from collectors.eg4 import EG4Client; import json; print(json.dumps(EG4Client().get_status(), indent=2))"

# 2. Victron — should print SOC ~64%, voltage ~53V
python3 -c "from collectors.victron import VictronClient; import json; print(json.dumps(VictronClient().get_status(), indent=2))"

# 3. HA API (requires HA_LONG_LIVED_TOKEN in .env)
python3 -c "
from collectors.ha_api import HACollector
import json
c = HACollector('fm', {'location_id': 'fm', 'primary_temp_sensor': 'sensor.fm_main_temp'})
print(json.dumps(c.collect(), indent=2, default=str))
"

# 4. Full collection run (writes to SQLite)
python3 -c "import scheduler, yaml; cfg=yaml.safe_load(open('config.yaml')); scheduler.start(cfg); import time; time.sleep(5); scheduler.stop()"

# 5. Check DB
python3 -c "import db, json; print(json.dumps(db.get_latest_merged_all(), indent=2, default=str))"

# 6. Full app (leave running, open browser)
python3 main.py
```

## Environment variables (.env)

See `.env.example` for full list. Minimum required:
- `HA_LONG_LIVED_TOKEN` — for fm, hc, lr properties
- `VICTRON_PORTAL_ID=c0619ab88ee0` — confirmed Feb 2026
- `PUSHOVER_USER_KEY` + `PUSHOVER_API_TOKEN` — for alerts

## Alert thresholds (config.yaml)

| Alert | Threshold | Pushover |
|-------|-----------|---------|
| Temperature | < 40°F (critical < 32°F) | ✓ |
| Battery SOC | < 20% (critical < 10%) | ✓ |
| Device battery | < 20% | ✓ |
| Source offline | No data > 30 min | ✓ |

Cooldowns prevent repeat Pushover spam: 60 min (temp), 120 min (battery/offline).

## Backlog Delivery (March 2026)

Completed:
- Mobile-first UI refresh with larger thumb-safe controls and clearer property cards
- Feed freshness boxes for Hubitat, Home Assistant, EG4, and Victron on each property
- Property lock status panel with lock/unlock controls (`all` + per-lock actions)
- Property smoke/CO status panel for each location card
- Hubitat device auto-pruning when devices are removed upstream
- Dedicated critical decision log page at `/decisions`
- Persistent `system_events` decision trail in SQLite (operator/system actions)
- Time-bounded stale Tesla/Powerwall fallback to keep HC energy cards visible during transient HA outages, with decision-log events

Operational details:
- Lock control endpoints:
  - `POST /api/property/{property_id}/locks/all/{lock|unlock}`
  - `POST /api/property/{property_id}/locks/{device_id}/{lock|unlock}`
- Decision log API endpoint:
  - `GET /api/system/decisions`
- Reboot action can run under non-root service via scoped sudoers policy:
  - `/etc/sudoers.d/safety-monitor-reboot` (installed by `deploy/install.sh`)
- Device pruning is conservative: if Hubitat returns an empty/unusable payload, prune is skipped to avoid accidental mass removal.

## Update deployed app

```bash
# From Proxmox host (pve-forget):
pct exec 104 -- bash -c 'cd /opt/safety-monitor/app && git pull && systemctl restart safety-monitor'
```

## Cowork session preflight (zero-friction)

Run this first in every new thread/session:

```bash
cd /Users/rpias/dev/safety-monitor
make preflight
```

What it verifies:
1. Local repo health (branch, clean tree, correct origin)
2. GitHub access (`git ls-remote`, `gh auth status` if `gh` is installed)
3. CT104 SSH access with key auth
4. CT104 runtime health (`safety-monitor` service + `/api/status`)

### One-time Codex approvals (for new agents/threads)

Approve these command prefixes once to avoid repeated permission prompts:

1. `ssh -i /Users/rpias/dev/vscode-dev-env/.notes_access/ssh/ct104_root_ed25519`
2. `git -C /Users/rpias/dev/safety-monitor`
3. `gh auth`
4. `gh repo`

### Canonical remotes

Expected `origin` for this repo:

```bash
https://github.com/rpiasentin/safety-monitor.git
```

### Deployment rule

Source of truth is `origin/main`.
Standard flow:

1. edit in workspace
2. commit + push to `origin/main`
3. deploy to CT104 from git
