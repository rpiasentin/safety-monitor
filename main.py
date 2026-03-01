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
import shutil
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

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


# ── Container health helpers ───────────────────────────────────────────────────

def _status_rank(status: str) -> int:
    return {"good": 0, "warning": 1, "critical": 2, "unknown": 3}.get(status, 3)


def _worst_status(statuses: list[str]) -> str:
    if not statuses:
        return "unknown"
    ranked = sorted(statuses, key=_status_rank, reverse=True)
    return ranked[0]


def _reboot_command() -> list[str] | None:
    """Return a usable reboot command for this host, or None if unavailable."""
    candidates = [
        ["/usr/sbin/reboot"],
        ["/sbin/reboot"],
        ["reboot"],
        ["systemctl", "reboot"],
    ]
    for cmd in candidates:
        exe = cmd[0]
        if exe.startswith("/"):
            if os.path.exists(exe):
                return cmd
        else:
            found = shutil.which(exe)
            if found:
                return [found] + cmd[1:]
    return None


def _collect_container_health(config: dict) -> dict:
    """
    Gather container host KPIs with an emphasis on disk availability.

    Config (optional):
      system.health.disk_paths: ["/", "/opt/safety-monitor"]
      system.health.disk_warning_free_percent: 20
      system.health.disk_critical_free_percent: 10
      system.health.memory_warning_free_percent: 10
      system.health.memory_critical_free_percent: 5
    """
    sys_cfg = (config or {}).get("system", {}) if isinstance(config, dict) else {}
    health_cfg = sys_cfg.get("health", {}) if isinstance(sys_cfg, dict) else {}

    warn_free_disk_pct = float(health_cfg.get("disk_warning_free_percent", 20))
    crit_free_disk_pct = float(health_cfg.get("disk_critical_free_percent", 10))
    warn_free_mem_pct = float(health_cfg.get("memory_warning_free_percent", 10))
    crit_free_mem_pct = float(health_cfg.get("memory_critical_free_percent", 5))

    db_dir = os.path.dirname(os.path.abspath(db.DB_PATH)) if db.DB_PATH else "/"
    raw_paths = health_cfg.get("disk_paths", ["/", db_dir])
    if not isinstance(raw_paths, list) or not raw_paths:
        raw_paths = ["/", db_dir]

    disks = []
    seen_devices = set()
    for path in raw_paths:
        try:
            abs_path = os.path.abspath(path)
            st = os.stat(abs_path)
            # Avoid duplicate entries when multiple paths are on same filesystem.
            if st.st_dev in seen_devices:
                continue
            seen_devices.add(st.st_dev)

            usage = shutil.disk_usage(abs_path)
            total = float(usage.total)
            free = float(usage.free)
            used = float(usage.used)
            free_pct = (free / total * 100.0) if total > 0 else 0.0
            if free_pct <= crit_free_disk_pct:
                status = "critical"
            elif free_pct <= warn_free_disk_pct:
                status = "warning"
            else:
                status = "good"
            disks.append({
                "path": abs_path,
                "status": status,
                "free_pct": round(free_pct, 1),
                "used_pct": round(100.0 - free_pct, 1),
                "free_gb": round(free / (1024 ** 3), 2),
                "used_gb": round(used / (1024 ** 3), 2),
                "total_gb": round(total / (1024 ** 3), 2),
            })
        except Exception as e:
            disks.append({
                "path": str(path),
                "status": "unknown",
                "error": str(e),
            })

    mem = {
        "status": "unknown",
        "free_pct": None,
        "free_gb": None,
        "used_gb": None,
        "total_gb": None,
    }
    try:
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                meminfo[k.strip()] = v.strip()
        total_kb = float(meminfo.get("MemTotal", "0 kB").split()[0])
        avail_kb = float(meminfo.get("MemAvailable", "0 kB").split()[0])
        used_kb = max(total_kb - avail_kb, 0.0)
        free_pct = (avail_kb / total_kb * 100.0) if total_kb > 0 else 0.0
        if free_pct <= crit_free_mem_pct:
            mem_status = "critical"
        elif free_pct <= warn_free_mem_pct:
            mem_status = "warning"
        else:
            mem_status = "good"
        mem = {
            "status": mem_status,
            "free_pct": round(free_pct, 1),
            "free_gb": round(avail_kb / (1024 ** 2), 2),
            "used_gb": round(used_kb / (1024 ** 2), 2),
            "total_gb": round(total_kb / (1024 ** 2), 2),
        }
    except Exception as e:
        mem["error"] = str(e)

    uptime_seconds = None
    try:
        with open("/proc/uptime") as f:
            uptime_seconds = int(float(f.read().split()[0]))
    except Exception:
        pass

    load_1m = None
    try:
        load_1m = round(os.getloadavg()[0], 2)
    except Exception:
        pass

    overall_status = _worst_status(
        [d.get("status", "unknown") for d in disks] + [mem.get("status", "unknown")]
    )
    reboot_cmd = _reboot_command()
    can_reboot = bool(reboot_cmd and os.geteuid() == 0)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_status": overall_status,
        "disk_warning_free_percent": warn_free_disk_pct,
        "disk_critical_free_percent": crit_free_disk_pct,
        "disks": disks,
        "memory": mem,
        "uptime_seconds": uptime_seconds,
        "load_1m": load_1m,
        "can_reboot": can_reboot,
    }


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
    alerts = db.get_dashboard_alerts(hours=24, recent_limit=20)
    container_health = _collect_container_health(CONFIG)

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
    global_alerts_cfg = CONFIG.get("alerts", {})
    global_temp_cfg = global_alerts_cfg.get("temperature", {})
    global_battery_cfg = global_alerts_cfg.get("battery", {})
    global_offline_cfg = global_alerts_cfg.get("offline", {})
    global_water_cfg = global_alerts_cfg.get("water", {})
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
                "battery_low_threshold_percent": pcfg.get(
                    "battery_low_threshold_percent",
                    global_battery_cfg.get("low_threshold_percent", 20),
                ),
                "battery_critical_threshold_percent": pcfg.get(
                    "battery_critical_threshold_percent",
                    global_battery_cfg.get("critical_threshold_percent", 10),
                ),
                "battery_cooldown_minutes": pcfg.get(
                    "battery_cooldown_minutes",
                    global_battery_cfg.get("cooldown_minutes", 120),
                ),
                "battery_pushover_enabled": pcfg.get(
                    "battery_pushover_enabled",
                    global_battery_cfg.get("pushover_enabled", True),
                ),
                "battery_exclude_devices": pcfg.get(
                    "battery_exclude_devices",
                    global_battery_cfg.get("exclude_devices", []),
                ),
                "offline_timeout_minutes": pcfg.get(
                    "offline_timeout_minutes",
                    global_offline_cfg.get("timeout_minutes", 30),
                ),
                "offline_cooldown_minutes": pcfg.get(
                    "offline_cooldown_minutes",
                    global_offline_cfg.get("cooldown_minutes", 120),
                ),
                "offline_pushover_enabled": pcfg.get(
                    "offline_pushover_enabled",
                    global_offline_cfg.get("pushover_enabled", True),
                ),
                "water_pushover_enabled": pcfg.get(
                    "water_pushover_enabled",
                    global_water_cfg.get("pushover_enabled", True),
                ),
                "water_exclude_sensors": pcfg.get(
                    "water_exclude_sensors",
                    global_water_cfg.get("exclude_sensors", []),
                ),
            },
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "cards":   cards,
        "alerts":  alerts,
        "container_health": container_health,
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
        "alerts":     db.get_dashboard_alerts(hours=24, recent_limit=50),
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


@app.get("/api/system/health")
async def api_system_health():
    """Container health KPIs with disk, memory, load, and uptime snapshots."""
    return JSONResponse(content=_collect_container_health(CONFIG))


@app.post("/api/system/reboot")
async def api_system_reboot(_auth=Depends(_require_write_auth)):
    """
    Schedule a host/container reboot shortly after responding.
    This keeps the UI call deterministic before connectivity drops.
    """
    cmd = _reboot_command()
    if not cmd:
        return JSONResponse(status_code=500, content={"error": "No reboot command available on host"})
    if os.geteuid() != 0:
        return JSONResponse(status_code=500, content={"error": "Process is not running as root"})

    delay_seconds = 2

    def _reboot_later():
        time.sleep(delay_seconds)
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            logger.exception("Container reboot command failed: %s", cmd)

    threading.Thread(target=_reboot_later, daemon=True).start()
    logger.warning("Container reboot requested via API, executing in %ss", delay_seconds)
    return JSONResponse(content={"status": "reboot_scheduled", "delay_seconds": delay_seconds})


@app.get("/api/config/thresholds")
async def get_thresholds():
    """Return per-property alert threshold config."""
    global_alerts = CONFIG.get("alerts", {})
    global_temp = global_alerts.get("temperature", {})
    global_battery = global_alerts.get("battery", {})
    global_offline = global_alerts.get("offline", {})
    global_water = global_alerts.get("water", {})
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
            "battery_low_threshold_percent": pcfg.get(
                "battery_low_threshold_percent",
                global_battery.get("low_threshold_percent", 20),
            ),
            "battery_critical_threshold_percent": pcfg.get(
                "battery_critical_threshold_percent",
                global_battery.get("critical_threshold_percent", 10),
            ),
            "battery_cooldown_minutes": pcfg.get(
                "battery_cooldown_minutes",
                global_battery.get("cooldown_minutes", 120),
            ),
            "battery_pushover_enabled": pcfg.get(
                "battery_pushover_enabled",
                global_battery.get("pushover_enabled", True),
            ),
            "battery_exclude_devices": pcfg.get(
                "battery_exclude_devices",
                global_battery.get("exclude_devices", []),
            ),
            "offline_timeout_minutes": pcfg.get(
                "offline_timeout_minutes",
                global_offline.get("timeout_minutes", 30),
            ),
            "offline_cooldown_minutes": pcfg.get(
                "offline_cooldown_minutes",
                global_offline.get("cooldown_minutes", 120),
            ),
            "offline_pushover_enabled": pcfg.get(
                "offline_pushover_enabled",
                global_offline.get("pushover_enabled", True),
            ),
            "water_pushover_enabled": pcfg.get(
                "water_pushover_enabled",
                global_water.get("pushover_enabled", True),
            ),
            "water_exclude_sensors": pcfg.get(
                "water_exclude_sensors",
                global_water.get("exclude_sensors", []),
            ),
        }
    return JSONResponse(content=result)


@app.post("/api/config/thresholds/{pid}")
async def update_thresholds(pid: str, request: Request,
                             _auth=Depends(_require_write_auth)):
    """Update per-property alert thresholds and persist to config.yaml."""
    import asyncio

    def _as_bool(val):
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "on"}
        return False

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
                "outdoor_temp_warning", "outdoor_temp_critical",
                "battery_low_threshold_percent",
                "battery_critical_threshold_percent",
                "battery_cooldown_minutes",
                "offline_timeout_minutes",
                "offline_cooldown_minutes"):
        if key in body and body[key] is not None:
            pcfg[key] = float(body[key])

    # Sensor lists (accept comma-separated string or list)
    for key in ("outdoor_sensors", "exclude_sensors",
                "battery_exclude_devices", "water_exclude_sensors"):
        if key in body:
            val = body[key]
            if isinstance(val, str):
                val = [s.strip() for s in val.split(",") if s.strip()]
            pcfg[key] = val

    # Per-alert push toggles
    for key in ("battery_pushover_enabled",
                "offline_pushover_enabled",
                "water_pushover_enabled"):
        if key in body and body[key] is not None:
            pcfg[key] = _as_bool(body[key])

    # Primary display sensor — stored in the collector config block.
    # If changed, trigger an immediate background collection so the
    # dashboard reflects the new sensor without waiting for the interval.
    primary_changed = False
    new_primary = body.get("primary_temp_sensor", "").strip()
    if new_primary:
        for coll in prop.get("collectors", []):
            if coll.get("type") in ("hubitat_cloud", "ha_api"):
                old_primary = str(coll.get("primary_temp_sensor", "")).strip()
                primary_changed = (old_primary != new_primary)
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
    if primary_changed:
        asyncio.get_running_loop().run_in_executor(None, scheduler.collect_all)
        logger.info("[%s] primary sensor changed; immediate collection requested", pid)

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


@app.post("/api/alerts/{alert_id}/clear")
async def clear_alert(alert_id: int, _auth=Depends(_require_write_auth)):
    """Manually clear a latched water alert so it disappears from the dashboard."""
    alert = db.get_alert(alert_id)
    if not alert:
        return JSONResponse(status_code=404, content={"error": f"Alert {alert_id} not found"})
    if alert.get("alert_type") != "water":
        return JSONResponse(status_code=400, content={"error": "Only water alerts can be manually cleared"})

    changed = db.resolve_alert(alert_id)
    if not changed:
        return JSONResponse(content={"status": "already_cleared", "alert_id": alert_id})

    logger.info("Water alert cleared manually: id=%s pid=%s sensor=%s",
                alert_id, alert.get("property_id"), alert.get("sensor_id"))
    return JSONResponse(content={"status": "cleared", "alert_id": alert_id})


_CLEARABLE_ALERT_CATEGORIES = {"temperature", "battery", "offline", "water", "all"}


@app.post("/api/alerts/clear/{pid}/{category}")
async def clear_alerts_by_category(pid: str, category: str, _auth=Depends(_require_write_auth)):
    """Clear unresolved alerts for one property and one category (or all)."""
    category = (category or "").strip().lower()
    if category not in _CLEARABLE_ALERT_CATEGORIES:
        allowed = ", ".join(sorted(_CLEARABLE_ALERT_CATEGORIES))
        return JSONResponse(
            status_code=400,
            content={"error": f"Invalid category '{category}'. Allowed: {allowed}"},
        )

    known_pids = {p.get("id") for p in CONFIG.get("properties", []) if p.get("id")}
    if pid not in known_pids:
        return JSONResponse(status_code=404, content={"error": f"Property '{pid}' not found"})

    alert_type = None if category == "all" else category
    changed = db.resolve_alerts(property_id=pid, alert_type=alert_type)
    logger.info("Alerts cleared manually: pid=%s category=%s count=%s", pid, category, changed)
    return JSONResponse(content={
        "status": "cleared",
        "property_id": pid,
        "category": category,
        "cleared": changed,
    })


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
