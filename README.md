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
  hubitat.py       Hubitat cloud API — Redwood (no local HA)

app/templates/
  dashboard.html   Tailwind dark dashboard, auto-refreshes 60s
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

## Update deployed app

```bash
cd /opt/safety-monitor/app
git pull
pip3 install --break-system-packages -r requirements.txt
systemctl restart safety-monitor
```
