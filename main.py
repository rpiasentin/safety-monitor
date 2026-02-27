"""
Safety Monitor — FastAPI web application
=========================================
Entry point. Starts the APScheduler collection engine and serves the
live dashboard at http://<tailscale-ip>:8000/

Run:
  python3 main.py
  or: uvicorn main:app --host 0.0.0.0 --port 8000

Systemd service: see Safety_Monitor_Deployment_Guide.docx
"""

import json
import logging
import os
from contextlib import asynccontextmanager

import uvicorn
import yaml
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import db
import formatters
import scheduler

# ── Logging ───────────────────────────────────────────────────────────────────

os.makedirs("logs", exist_ok=True)
os.makedirs("data", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-20s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler("logs/safety_monitor.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(cfg_path) as f:
        return yaml.safe_load(f)


CONFIG = load_config()


# ── Auth ──────────────────────────────────────────────────────────────────────
# Set MONITOR_API_KEY env var to require an X-API-Key header on write endpoints.
# If the env var is not set, write endpoints remain open (safe on Tailscale-only
# deployments where no external network access exists).

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_API_KEY = os.getenv("MONITOR_API_KEY", "")


async def _require_write_auth(key: str = Security(_API_KEY_HEADER)) -> None:
    """Dependency: enforce API key on mutating endpoints if MONITOR_API_KEY is set."""
    if _API_KEY and key != _API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing X-API-Key header")


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Safety Monitor starting up...")
    scheduler.start(CONFIG)
    yield
    logger.info("Safety Monitor shutting down...")
    scheduler.stop()


app = FastAPI(
    title=CONFIG.get("web", {}).get("title", "Safety Monitor"),
    lifespan=lifespan,
)

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "app/templates"))

# Register Jinja2 globals for formatting helpers
templates.env.globals.update({
    "fmt_temp":         formatters.fmt_temp,
    "fmt_power":        formatters.fmt_power,
    "fmt_voltage":      formatters.fmt_voltage,
    "fmt_pct":          formatters.fmt_pct,
    "temp_status":      formatters.temp_status,
    "battery_status":   formatters.battery_status,
    "soc_color":        formatters.soc_color,
    "temp_color":       formatters.temp_color,
    "battery_color":    formatters.battery_color,
    "ago":              formatters.ago,
    "activity_status":  formatters.activity_status,
})


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main live dashboard — auto-refreshes every 60 s."""
    props  = CONFIG.get("properties", [])
    latest = db.get_latest_merged_all()
    alerts = db.get_recent_alerts(hours=24)

    # Merge raw_json extra fields (pv_eg4, pv_victron_1/2, battery_charging_power,
    # load_power, etc.) into each row so the template can access them directly.
    for row in latest.values():
        raw_str = row.get("raw_json")
        if raw_str:
            try:
                raw = json.loads(raw_str)
                for k, v in raw.items():
                    if v is not None and row.get(k) is None:
                        row[k] = v
            except Exception:
                pass

    # Build per-property context
    global_temp_cfg = CONFIG.get("alerts", {}).get("temperature", {})
    cards = []
    for p in props:
        pid  = p["id"]
        row  = latest.get(pid, {})
        devs = db.get_hubitat_devices(pid)
        pcfg = p.get("alerts", {})
        # Find current primary_temp_sensor from collector configs
        primary_sensor = next(
            (c.get("primary_temp_sensor", "") for c in p.get("collectors", [])
             if c.get("primary_temp_sensor")),
            ""
        )

        # Alert counts for this property
        prop_alerts = [a for a in alerts if a["property_id"] == pid]

        # Temp status
        pt = row.get("primary_temp")
        ts = formatters.temp_status(pt)

        cards.append({
            "id":             pid,
            "name":           p.get("name", pid),
            "enabled":        p.get("enabled", True),
            "reading":        row,
            "devices":        devs,
            "alert_count":    len(prop_alerts),
            "recent_alerts":  prop_alerts[:3],
            "primary_temp":   pt,
            "temp_status":    ts,
            "temp_color":     formatters.temp_color(ts),
            "soc_color":      formatters.soc_color(row.get("soc")),
            # Alert threshold config (for settings panel)
            "alerts_cfg": {
                "indoor_temp_warning":  pcfg.get("indoor_temp_warning",
                                            global_temp_cfg.get("threshold_fahrenheit", 40)),
                "indoor_temp_critical": pcfg.get("indoor_temp_critical",
                                            global_temp_cfg.get("critical_fahrenheit", 32)),
                "outdoor_temp_warning":  pcfg.get("outdoor_temp_warning", 15),
                "outdoor_temp_critical": pcfg.get("outdoor_temp_critical", 0),
                "outdoor_sensors":    pcfg.get("outdoor_sensors", []),
                "exclude_sensors":    pcfg.get("exclude_sensors", []),
                "primary_temp_sensor": primary_sensor,
            },
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "cards":   cards,
        "alerts":  alerts[:10],
        "config":  CONFIG,
    })


@app.get("/devices/{property_id}", response_class=HTMLResponse)
async def device_activity(request: Request, property_id: str):
    """Per-property device activity view — last seen timestamps for all Hubitat devices."""
    # Find property config
    props = CONFIG.get("properties", [])
    prop  = next((p for p in props if p["id"] == property_id), None)
    if not prop:
        return HTMLResponse(f"<h1>Property '{property_id}' not found</h1>", status_code=404)

    # Resolve thresholds: per-property overrides global defaults
    global_da  = CONFIG.get("device_activity", {})
    prop_da    = prop.get("device_activity", {})
    warn_mins  = prop_da.get("warning_minutes",
                              global_da.get("warning_minutes", 120))
    crit_mins  = prop_da.get("critical_minutes",
                              global_da.get("critical_minutes", 1440))

    devices = db.get_hubitat_devices_activity(property_id)

    # Annotate each device with effective activity timestamp/status.
    # If Hubitat does not expose per-device activity time, use collected_at
    # (last seen in API payload) so active devices are not shown as "never".
    for dev in devices:
        effective_ts = dev.get("last_activity") or dev.get("collected_at")
        dev["activity_display_ts"] = effective_ts
        dev["activity_source"] = "activity" if dev.get("last_activity") else (
            "seen" if dev.get("collected_at") else "none"
        )
        dev["activity_status"] = formatters.activity_status(
            effective_ts, warn_mins, crit_mins)

    # Summary counts
    counts = {"good": 0, "warning": 0, "critical": 0, "unknown": 0}
    for dev in devices:
        counts[dev["activity_status"]] = counts.get(dev["activity_status"], 0) + 1

    return templates.TemplateResponse("device_activity.html", {
        "request":       request,
        "prop":          prop,
        "devices":       devices,
        "warn_mins":     warn_mins,
        "crit_mins":     crit_mins,
        "counts":        counts,
        "config":        CONFIG,
    })


@app.get("/api/status")
async def api_status():
    """JSON snapshot of all properties — useful for external scripts / health checks."""
    return JSONResponse(content={
        "properties": db.get_latest_readings_all(),
        "alerts":     db.get_recent_alerts(hours=24),
    })


@app.get("/api/property/{property_id}")
async def api_property(property_id: str):
    reading  = db.get_latest_reading(property_id)
    history  = db.get_readings_history(property_id, hours=24)
    devices  = db.get_hubitat_devices(property_id)
    alerts   = db.get_recent_alerts(hours=48)
    p_alerts = [a for a in alerts if a["property_id"] == property_id]
    return JSONResponse(content={
        "latest":  reading,
        "history": history,
        "devices": devices,
        "alerts":  p_alerts,
    })


@app.get("/api/history/{property_id}")
async def api_history(property_id: str, hours: int = 24):
    return JSONResponse(content=db.get_readings_history(property_id, hours))


@app.get("/api/alerts")
async def api_alerts(hours: int = 48):
    return JSONResponse(content=db.get_recent_alerts(hours))


@app.get("/api/config/thresholds")
async def get_thresholds():
    """Return per-property alert threshold config."""
    global_temp = CONFIG.get("alerts", {}).get("temperature", {})
    result = {}
    for p in CONFIG.get("properties", []):
        pid  = p["id"]
        pcfg = p.get("alerts", {})
        result[pid] = {
            "name":                  p.get("name", pid),
            "indoor_temp_warning":   pcfg.get("indoor_temp_warning",
                                         global_temp.get("threshold_fahrenheit", 40)),
            "indoor_temp_critical":  pcfg.get("indoor_temp_critical",
                                         global_temp.get("critical_fahrenheit", 32)),
            "outdoor_temp_warning":  pcfg.get("outdoor_temp_warning", 15),
            "outdoor_temp_critical": pcfg.get("outdoor_temp_critical", 0),
            "outdoor_sensors":       pcfg.get("outdoor_sensors", []),
            "exclude_sensors":       pcfg.get("exclude_sensors", []),
        }
    return JSONResponse(content=result)


@app.post("/api/config/thresholds/{pid}")
async def update_thresholds(pid: str, request: Request,
                             _auth=Depends(_require_write_auth)):
    """Update per-property alert thresholds and persist to config.yaml."""
    # Find the property
    props = CONFIG.get("properties", [])
    prop  = next((p for p in props if p["id"] == pid), None)
    if not prop:
        return JSONResponse(status_code=404, content={"error": f"Property '{pid}' not found"})

    body = await request.json()

    if "alerts" not in prop:
        prop["alerts"] = {}
    pcfg = prop["alerts"]

    # Numeric thresholds
    for key in ("indoor_temp_warning", "indoor_temp_critical",
                "outdoor_temp_warning", "outdoor_temp_critical"):
        if key in body and body[key] is not None:
            pcfg[key] = float(body[key])

    # Sensor lists (accept comma-separated string or list)
    for key in ("outdoor_sensors", "exclude_sensors"):
        if key in body:
            val = body[key]
            if isinstance(val, str):
                val = [s.strip() for s in val.split(",") if s.strip()]
            pcfg[key] = val

    # Primary display sensor — stored in the collector config block
    new_primary = body.get("primary_temp_sensor", "").strip()
    if new_primary:
        for coll in prop.get("collectors", []):
            if coll.get("type") in ("hubitat_cloud", "ha_api"):
                coll["primary_temp_sensor"] = new_primary
                break
        # Patch the running collector immediately (no restart needed)
        scheduler.update_primary_temp_sensor(pid, new_primary)

    # Persist to config.yaml
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(cfg_path, "w") as f:
        yaml.dump(CONFIG, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    # Update scheduler in-memory alert state (no restart needed)
    scheduler.update_property_alert_cfg(pid, pcfg)

    logger.info("Thresholds updated for [%s]: alerts=%s primary_sensor=%s", pid, pcfg, new_primary)
    return JSONResponse(content={"status": "ok", "pid": pid, "alerts": pcfg})


@app.get("/api/property/{property_id}/sensors")
async def api_sensors(property_id: str):
    """Return sorted list of temperature sensor names known for a property."""
    row = db.get_latest_reading(property_id, source="merged")
    sensors = []
    if row:
        raw_str = row.get("raw_json")
        if raw_str:
            try:
                raw = json.loads(raw_str)
                all_temps = raw.get("all_temps") or {}
                sensors = sorted(all_temps.keys())
            except Exception:
                pass
    return JSONResponse(content={"sensors": sensors})


@app.post("/api/collect/now")
async def trigger_collection(_auth=Depends(_require_write_auth)):
    """Manually trigger an immediate collection run (useful for testing)."""
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, scheduler.collect_all)
    return JSONResponse(content={"status": "triggered"})


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    web_cfg = CONFIG.get("web", {})
    uvicorn.run(
        "main:app",
        host=web_cfg.get("host", "0.0.0.0"),
        port=web_cfg.get("port", 8000),
        reload=False,
        log_level="info",
    )
