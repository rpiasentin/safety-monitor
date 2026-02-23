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
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
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
    "fmt_temp":       formatters.fmt_temp,
    "fmt_power":      formatters.fmt_power,
    "fmt_voltage":    formatters.fmt_voltage,
    "fmt_pct":        formatters.fmt_pct,
    "temp_status":    formatters.temp_status,
    "battery_status": formatters.battery_status,
    "soc_color":      formatters.soc_color,
    "temp_color":     formatters.temp_color,
    "battery_color":  formatters.battery_color,
    "ago":            formatters.ago,
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
    cards = []
    for p in props:
        pid  = p["id"]
        row  = latest.get(pid, {})
        devs = db.get_hubitat_devices(pid)

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
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "cards":   cards,
        "alerts":  alerts[:10],
        "config":  CONFIG,
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


@app.post("/api/collect/now")
async def trigger_collection():
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
