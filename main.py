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
import csv
import io
import logging
import math
import os
import shutil
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import uvicorn
import yaml
from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import db
import formatters
import scheduler
import water_service
from collectors.hubitat import HubitatCloudClient

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
    """
    Return a usable reboot command for this host, or None if unavailable.

    Preference order:
      1) direct reboot commands when running as root
      2) passwordless sudo for reboot commands when running non-root
    """
    candidates = [
        ["/usr/sbin/reboot"],
        ["/sbin/reboot"],
        ["reboot"],
        ["systemctl", "reboot"],
    ]
    sudo_bin = shutil.which("sudo")
    for cmd in candidates:
        exe = cmd[0]
        if exe.startswith("/"):
            if not os.path.exists(exe):
                continue
            resolved = cmd
        else:
            found = shutil.which(exe)
            if found:
                resolved = [found] + cmd[1:]
            else:
                continue

        # Root can run the reboot command directly.
        if os.geteuid() == 0:
            return resolved

        # Non-root path: allow only if sudoers permits this specific command.
        if not sudo_bin:
            continue
        try:
            probe = subprocess.run(
                [sudo_bin, "-n", "-l", "--", *resolved],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
                timeout=2,
            )
            if probe.returncode == 0:
                return [sudo_bin, "-n", "--", *resolved]
        except Exception:
            continue
    return None


def _parse_timestamp_utc(raw_ts: str | None) -> datetime | None:
    """Parse mixed timestamp formats and normalize to UTC."""
    if not raw_ts:
        return None
    try:
        ts = datetime.fromisoformat(str(raw_ts).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    except Exception:
        return None


def _collector_feed_health(label: str, collected_at: str | None) -> dict:
    """
    Build feed health payload with traffic-light status:
      green  <= 5 minutes
      yellow <= 10 minutes
      red    > 10 minutes or missing/not parseable
    """
    ts = _parse_timestamp_utc(collected_at)
    if not ts:
        return {
            "label": label,
            "status": "critical",
            "last_activity": "not responding",
            "collected_at": None,
        }

    age_seconds = max(0, int((datetime.now(timezone.utc) - ts).total_seconds()))
    age_minutes = age_seconds / 60.0
    if age_minutes <= 5:
        status = "good"
    elif age_minutes <= 10:
        status = "warning"
    else:
        status = "critical"

    if age_seconds < 60:
        last_activity = "just now"
    elif age_seconds < 3600:
        last_activity = f"{age_seconds // 60}m ago"
    elif age_seconds < 86400:
        last_activity = f"{age_seconds // 3600}h ago"
    else:
        last_activity = f"{age_seconds // 86400}d ago"

    return {
        "label": label,
        "status": status,
        "last_activity": last_activity,
        "collected_at": ts.isoformat(),
    }


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
    can_reboot = bool(reboot_cmd)
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


def _record_system_event(event_type: str,
                         message: str,
                         level: str = "info",
                         property_id: str | None = None,
                         actor: str = "api",
                         details: dict | None = None) -> None:
    """Best-effort persistence for critical system decisions/actions."""
    try:
        db.insert_system_event(
            event_type=event_type,
            message=message,
            level=level,
            property_id=property_id,
            actor=actor,
            details=details,
        )
    except Exception:
        logger.debug("Failed to persist system event: %s", event_type, exc_info=True)


def _parse_raw_payload(row: dict | None) -> dict:
    if not row:
        return {}
    raw = row.get("raw_json")
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _maker_device_activity(last_activity: str | None,
                           collected_at: str | None) -> dict:
    """Return traffic-light activity status using 5/10 minute thresholds."""
    ts = _parse_timestamp_utc(last_activity) or _parse_timestamp_utc(collected_at)
    if not ts:
        return {
            "status": "critical",
            "last_activity": "not responding",
            "activity_at": None,
        }

    age_seconds = max(0, int((datetime.now(timezone.utc) - ts).total_seconds()))
    age_minutes = age_seconds / 60.0
    if age_minutes <= 5:
        status = "good"
    elif age_minutes <= 10:
        status = "warning"
    else:
        status = "critical"

    if age_seconds < 60:
        last_activity_text = "just now"
    elif age_seconds < 3600:
        last_activity_text = f"{age_seconds // 60}m ago"
    elif age_seconds < 86400:
        last_activity_text = f"{age_seconds // 3600}h ago"
    else:
        last_activity_text = f"{age_seconds // 86400}d ago"

    return {
        "status": status,
        "last_activity": last_activity_text,
        "activity_at": ts.isoformat(),
    }


def _maker_device_roles(property_id: str) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """
    Build best-effort role maps from latest Hubitat collector payload:
      by device_id and by friendly_name(lowercase).
    """
    source_row = db.get_latest_reading(property_id, source="hubitat_cloud")
    payload = _parse_raw_payload(source_row)
    roles_by_id: dict[str, set[str]] = {}
    roles_by_name: dict[str, set[str]] = {}

    def _add(role: str, rows: list[dict]) -> None:
        for row in (rows or []):
            did = str(row.get("entity_id") or "").strip()
            name = str(row.get("friendly_name") or "").strip()
            if did:
                roles_by_id.setdefault(did, set()).add(role)
            if name:
                roles_by_name.setdefault(name.lower(), set()).add(role)

    # Temperature list is keyed by friendly label in the Maker payload.
    for sensor_name in (payload.get("temperatures") or {}).keys():
        name = str(sensor_name or "").strip()
        if name:
            roles_by_name.setdefault(name.lower(), set()).add("temperature")

    _add("battery", list(payload.get("battery_devices") or []))
    _add("water", list(payload.get("water_sensors") or []))
    _add("smoke", list(payload.get("smoke_devices") or []))
    _add("lock", list(payload.get("lock_devices") or []))
    _add("valve", list(payload.get("valve_devices") or []))
    return roles_by_id, roles_by_name


def _get_property_cfg(property_id: str) -> dict | None:
    return next(
        (p for p in CONFIG.get("properties", []) if p.get("id") == property_id),
        None,
    )


def _hubitat_client_for_property(property_id: str) -> HubitatCloudClient | None:
    prop = _get_property_cfg(property_id)
    if not prop:
        return None
    coll_cfg = next(
        (c for c in prop.get("collectors", []) if c.get("type") == "hubitat_cloud"),
        None,
    )
    if not coll_cfg or not coll_cfg.get("endpoint"):
        return None

    token = (
        coll_cfg.get("token")
        or os.getenv(f"HUBITAT_{property_id.upper()}_TOKEN")
        or os.getenv("HUBITAT_CLOUD_TOKEN", "")
    )
    return HubitatCloudClient(coll_cfg["endpoint"], api_token=token)


def _schedule_collection_refresh(retries: int = 3,
                                 wait_seconds: int = 5,
                                 always_run: bool = False,
                                 initial_delay: int = 0) -> None:
    """Background refresh helper used after control-plane actions."""
    retries = max(1, int(retries))
    wait_seconds = max(1, int(wait_seconds))
    initial_delay = max(0, int(initial_delay))

    def _refresh_with_retry():
        if initial_delay:
            time.sleep(initial_delay)

        if always_run:
            for i in range(retries):
                scheduler.collect_all()
                if i < (retries - 1):
                    time.sleep(wait_seconds)
            return

        for _ in range(retries):
            if scheduler.collect_all():
                return
            time.sleep(wait_seconds)

    threading.Thread(target=_refresh_with_retry, daemon=True).start()


def _expected_lock_state(action: str) -> str:
    return "locked" if str(action or "").strip().lower() == "lock" else "unlocked"


def _expected_valve_state(action: str) -> str:
    return "open" if str(action or "").strip().lower() == "open" else "closed"


def _property_alert_cfg(property_id: str) -> dict:
    prop = _get_property_cfg(property_id) or {}
    return prop.get("alerts", {}) or {}


def _valve_service_meta(property_cfg: dict | None,
                        valve_id: str,
                        raw_state: str | None) -> dict:
    return water_service.valve_service_state(raw_state, property_cfg, valve_id)


def _valve_service_action_label(action: str) -> str:
    cmd = str(action or "").strip().lower()
    if cmd == "on":
        return "Turn Water On"
    if cmd == "off":
        return "Turn Water Off"
    return cmd.replace("_", " ").title() if cmd else "Valve Action"


def _raw_valve_command_service_state(property_cfg: dict | None,
                                     valve_id: str,
                                     raw_command: str) -> str:
    return _valve_service_meta(property_cfg, valve_id, raw_command).get("service_state") or ""


def _valve_event_label(event_type: str,
                       valve_id: str,
                       property_cfg: dict | None = None) -> str:
    kind = str(event_type or "").strip().lower()
    if kind == "water_shutoff_closed":
        service_state = _raw_valve_command_service_state(property_cfg, valve_id, "closed")
        return "water on" if service_state == "on" else "water off"
    if kind == "water_shutoff_opened":
        service_state = _raw_valve_command_service_state(property_cfg, valve_id, "open")
        return "water on" if service_state == "on" else "water off"
    if kind == "water_incident_opened":
        return "incident opened"
    if kind == "water_incident_acknowledged":
        return "acknowledged"
    if kind == "water_incident_resolved":
        return "water on"
    if kind == "valve_command_state_unchanged":
        return "status not changing"
    return ""


def _verify_lock_transition(client: HubitatCloudClient,
                            expected_states: dict[str, str],
                            name_hints: dict[str, str] | None = None,
                            polls: int = 4,
                            wait_seconds: int = 3) -> dict:
    """
    Poll Hubitat lock states after a command and report devices that did not
    transition to the expected state.
    """
    pending: dict[str, dict] = {}
    for device_id, expected in (expected_states or {}).items():
        did = str(device_id).strip()
        if not did:
            continue
        pending[did] = {
            "device_id": did,
            "expected_state": str(expected or "").strip().lower(),
            "observed_state": "unknown",
            "friendly_name": (name_hints or {}).get(did) or did,
        }

    polls = max(1, int(polls))
    wait_seconds = max(1, int(wait_seconds))

    for attempt in range(polls):
        if attempt > 0:
            time.sleep(wait_seconds)

        try:
            current = client.get_lock_devices()
        except Exception as exc:
            logger.warning("Lock transition verification poll failed: %s", exc)
            continue

        by_id = {str(x.get("entity_id")): x for x in current}
        for did in list(pending.keys()):
            row = pending[did]
            cur = by_id.get(did, {})
            observed = str(cur.get("state") or "unknown").strip().lower()
            row["observed_state"] = observed
            if cur.get("friendly_name"):
                row["friendly_name"] = cur.get("friendly_name")
            if observed == row["expected_state"]:
                del pending[did]

        if not pending:
            break

    unresolved = sorted(pending.values(), key=lambda x: str(x.get("friendly_name") or x.get("device_id")))
    return {
        "ok": len(unresolved) == 0,
        "unresolved": unresolved,
    }


def _record_lock_state_unchanged(property_id: str,
                                 action: str,
                                 unresolved: list[dict]) -> list[dict]:
    warnings = []
    for row in (unresolved or []):
        did = str(row.get("device_id") or "").strip()
        if not did:
            continue
        name = row.get("friendly_name") or did
        expected = str(row.get("expected_state") or "").strip().lower()
        observed = str(row.get("observed_state") or "").strip().lower() or "unknown"
        msg = f"Command sent but device status not changing: {name} stayed {observed}"
        details = {
            "device_id": did,
            "friendly_name": name,
            "action": str(action or "").strip().lower(),
            "expected_state": expected,
            "observed_state": observed,
            "message": "Command sent but device status not changing",
        }
        _record_system_event(
            event_type="lock_command_state_unchanged",
            level="warning",
            property_id=property_id,
            actor="api",
            message=msg,
            details=details,
        )
        warnings.append(details)
    return warnings


def _verify_valve_transition(client: HubitatCloudClient,
                             expected_states: dict[str, str],
                             name_hints: dict[str, str] | None = None,
                             polls: int = 4,
                             wait_seconds: int = 3) -> dict:
    """
    Poll Hubitat valve states after a command and report devices that did not
    transition to the expected state.
    """
    pending: dict[str, dict] = {}
    for device_id, expected in (expected_states or {}).items():
        did = str(device_id).strip()
        if not did:
            continue
        pending[did] = {
            "device_id": did,
            "expected_state": str(expected or "").strip().lower(),
            "observed_state": "unknown",
            "friendly_name": (name_hints or {}).get(did) or did,
        }

    polls = max(1, int(polls))
    wait_seconds = max(1, int(wait_seconds))

    for attempt in range(polls):
        if attempt > 0:
            time.sleep(wait_seconds)

        try:
            current = client.get_valve_devices()
        except Exception as exc:
            logger.warning("Valve transition verification poll failed: %s", exc)
            continue

        by_id = {str(x.get("entity_id")): x for x in current}
        for did in list(pending.keys()):
            row = pending[did]
            cur = by_id.get(did, {})
            observed = str(cur.get("state") or "unknown").strip().lower()
            row["observed_state"] = observed
            if cur.get("friendly_name"):
                row["friendly_name"] = cur.get("friendly_name")
            if observed == row["expected_state"]:
                del pending[did]

        if not pending:
            break

    unresolved = sorted(pending.values(), key=lambda x: str(x.get("friendly_name") or x.get("device_id")))
    return {
        "ok": len(unresolved) == 0,
        "unresolved": unresolved,
    }


def _record_valve_state_unchanged(property_id: str,
                                  action: str,
                                  unresolved: list[dict]) -> list[dict]:
    warnings = []
    for row in (unresolved or []):
        did = str(row.get("device_id") or "").strip()
        if not did:
            continue
        name = row.get("friendly_name") or did
        expected = str(row.get("expected_state") or "").strip().lower()
        observed = str(row.get("observed_state") or "").strip().lower() or "unknown"
        msg = f"Command sent but valve status not changing: {name} stayed {observed}"
        details = {
            "device_id": did,
            "friendly_name": name,
            "action": str(action or "").strip().lower(),
            "expected_state": expected,
            "observed_state": observed,
            "message": "Command sent but valve status not changing",
        }
        _record_system_event(
            event_type="valve_command_state_unchanged",
            level="warning",
            property_id=property_id,
            actor="api",
            message=msg,
            details=details,
        )
        warnings.append(details)
    return warnings


def _recent_lock_warning_map(limit: int = 300) -> dict[str, dict[str, dict]]:
    """
    Build latest warning lookup: property_id -> device_id -> warning details.
    """
    rows = db.get_system_events(limit=limit, event_type="lock_command_state_unchanged")
    events = _decode_system_event_rows(rows)
    out: dict[str, dict[str, dict]] = {}
    for ev in events:
        pid = str(ev.get("property_id") or "").strip()
        details = ev.get("details") or {}
        did = str(details.get("device_id") or "").strip()
        if not pid or not did:
            continue
        out.setdefault(pid, {})
        if did in out[pid]:
            continue
        out[pid][did] = {
            "created_at": ev.get("created_at"),
            "expected_state": str(details.get("expected_state") or "").strip().lower(),
            "message": str(details.get("message") or ev.get("message") or "").strip(),
        }
    return out


def _recent_valve_warning_map(limit: int = 300) -> dict[str, dict[str, dict]]:
    rows = db.get_system_events(limit=limit, event_type="valve_command_state_unchanged")
    events = _decode_system_event_rows(rows)
    out: dict[str, dict[str, dict]] = {}
    for ev in events:
        pid = str(ev.get("property_id") or "").strip()
        details = ev.get("details") or {}
        did = str(details.get("device_id") or "").strip()
        if not pid or not did:
            continue
        out.setdefault(pid, {})
        if did in out[pid]:
            continue
        out[pid][did] = {
            "created_at": ev.get("created_at"),
            "expected_state": str(details.get("expected_state") or "").strip().lower(),
            "message": str(details.get("message") or ev.get("message") or "").strip(),
        }
    return out


def _active_alert_map(alert_type: str) -> dict[str, dict[str, dict]]:
    out: dict[str, dict[str, dict]] = {}
    for row in db.get_active_alerts(alert_type):
        pid = str(row.get("property_id") or "").strip()
        sensor_id = str(row.get("sensor_id") or "").strip()
        if not pid or not sensor_id:
            continue
        out.setdefault(pid, {})
        if sensor_id in out[pid]:
            continue
        out[pid][sensor_id] = dict(row)
    return out


def _recent_device_event_map(event_types: set[str],
                             detail_key: str,
                             limit: int = 400) -> dict[str, dict[str, dict]]:
    rows = db.get_system_events(limit=limit)
    events = _decode_system_event_rows(rows)
    out: dict[str, dict[str, dict]] = {}
    wanted = {str(x or "").strip().lower() for x in (event_types or set())}
    for ev in events:
        event_type = str(ev.get("event_type") or "").strip().lower()
        if event_type not in wanted:
            continue
        pid = str(ev.get("property_id") or "").strip()
        details = ev.get("details") or {}
        did = str(details.get(detail_key) or "").strip()
        if not pid or not did:
            continue
        out.setdefault(pid, {})
        if did in out[pid]:
            continue
        out[pid][did] = {
            "created_at": ev.get("created_at"),
            "event_type": event_type,
            "message": str(ev.get("message") or "").strip(),
        }
    return out


def _decorate_lock_devices(lock_devices: list[dict]) -> tuple[list[dict], dict]:
    decorated = []
    counts = {"locked": 0, "unlocked": 0, "other": 0}
    for lock in (lock_devices or []):
        row = dict(lock)
        state = str(row.get("state") or "unknown").strip().lower()
        if state == "locked":
            status = "good"
            state_label = "Locked"
            counts["locked"] += 1
        elif state == "unlocked":
            status = "critical"
            state_label = "Unlocked"
            counts["unlocked"] += 1
        elif state in {"locking", "unlocking"}:
            status = "warning"
            state_label = state.title()
            counts["other"] += 1
        else:
            status = "unknown"
            state_label = state.replace("_", " ").title() if state else "Unknown"
            counts["other"] += 1
        row["status"] = status
        row["state_label"] = state_label
        decorated.append(row)
    return decorated, counts


def _decorate_water_devices(water_devices: list[dict],
                            collected_at: str | None = None,
                            recent_event_map: dict[str, dict] | None = None) -> tuple[list[dict], dict]:
    decorated = []
    counts = {"wet": 0, "dry": 0, "other": 0}
    recent_event_map = recent_event_map or {}
    for sensor in (water_devices or []):
        row = dict(sensor)
        state = str(row.get("state") or "unknown").strip().lower()
        if state == "wet":
            status = "critical"
            state_label = "Wet"
            counts["wet"] += 1
        elif state == "dry":
            status = "good"
            state_label = "Dry"
            counts["dry"] += 1
        else:
            status = "unknown"
            state_label = state.replace("_", " ").title() if state else "Unknown"
            counts["other"] += 1
        row["status"] = status
        row["state_label"] = state_label
        row["activity"] = _maker_device_activity(row.get("last_activity"), collected_at)
        event = recent_event_map.get(str(row.get("entity_id") or "").strip())
        row["recent_event"] = event
        row["recent_event_age"] = formatters.ago(event.get("created_at")) if event else ""
        decorated.append(row)
    decorated.sort(
        key=lambda row: (
            0 if str(row.get("state") or "").strip().lower() == "wet" else 1,
            str(row.get("friendly_name") or row.get("entity_id") or "").lower(),
        )
    )
    return decorated, counts


def _decorate_valve_devices(valve_devices: list[dict],
                            property_cfg: dict | None = None,
                            collected_at: str | None = None,
                            recent_event_map: dict[str, dict] | None = None,
                            valve_state_map: dict[str, dict] | None = None,
                            active_alert_map: dict[str, dict] | None = None) -> tuple[list[dict], dict]:
    decorated = []
    counts = {"on": 0, "off": 0, "other": 0}
    recent_event_map = recent_event_map or {}
    valve_state_map = valve_state_map or {}
    active_alert_map = active_alert_map or {}
    for valve in (valve_devices or []):
        row = dict(valve)
        valve_id = str(row.get("entity_id") or "").strip()
        state = str(row.get("state") or "unknown").strip().lower()
        service_meta = _valve_service_meta(property_cfg, valve_id, state)
        row["status"] = service_meta.get("status") or "unknown"
        row["state_label"] = service_meta.get("state_label") or "Unknown"
        row["service_state"] = service_meta.get("service_state") or "unknown"
        row["water_on_raw_state"] = service_meta.get("water_on_raw_state")
        row["water_off_raw_state"] = service_meta.get("water_off_raw_state")
        row["can_turn_water_on"] = bool(service_meta.get("can_turn_water_on"))
        row["can_turn_water_off"] = bool(service_meta.get("can_turn_water_off"))
        if row["service_state"] == "on":
            counts["on"] += 1
        elif row["service_state"] == "off":
            counts["off"] += 1
        else:
            counts["other"] += 1
        row["activity"] = _maker_device_activity(row.get("last_activity"), collected_at)
        event = recent_event_map.get(valve_id)
        row["recent_event"] = event
        row["recent_event_age"] = formatters.ago(event.get("created_at")) if event else ""
        row["recent_event_label"] = _valve_event_label(
            event.get("event_type") if event else "",
            valve_id,
            property_cfg,
        )
        valve_state = valve_state_map.get(valve_id) or {}
        active_alert = active_alert_map.get(valve_id)
        row["incident_active"] = bool(active_alert)
        row["incident_acknowledged"] = bool(int(valve_state.get("acked_until_open") or 0))
        row["expected_water_off"] = bool(int(valve_state.get("expected_closed") or 0))
        row["trigger_sensor_name"] = str(valve_state.get("trigger_sensor_name") or "").strip()
        row["trigger_sensor_id"] = str(valve_state.get("trigger_sensor_id") or "").strip()
        row["last_water_off_at"] = valve_state.get("last_closed_at")
        row["incident_age"] = formatters.ago(valve_state.get("last_closed_at")) if valve_state.get("last_closed_at") else ""
        row["can_ack_incident"] = bool(
            row["service_state"] == "off" and active_alert and not row["incident_acknowledged"]
        )
        decorated.append(row)
    decorated.sort(
        key=lambda row: (
            0 if row.get("service_state") == "off" else 1 if row.get("service_state") == "transition_off" else 2,
            str(row.get("friendly_name") or row.get("entity_id") or "").lower(),
        )
    )
    return decorated, counts


def _decorate_smoke_devices(smoke_devices: list[dict],
                            smoke_state_map: dict[str, dict] | None = None) -> tuple[list[dict], dict]:
    decorated = []
    counts = {"critical": 0, "warning": 0, "good": 0, "unknown": 0}
    smoke_state_map = smoke_state_map or {}
    now = datetime.now(timezone.utc)
    for sensor in (smoke_devices or []):
        row = dict(sensor)
        status = str(row.get("status") or "unknown").strip().lower()
        if status not in counts:
            status = "unknown"
        counts[status] += 1

        state = str(row.get("state") or "unknown").strip().lower()
        if state == "alarm":
            state_label = "ALARM"
        elif state == "clear":
            state_label = "Clear"
        elif state == "test":
            state_label = "Test"
        else:
            state_label = state.replace("_", " ").title() if state else "Unknown"

        row["status"] = status
        row["state_label"] = state_label

        sensor_id = str(row.get("entity_id") or "").strip()
        state_row = smoke_state_map.get(sensor_id) or {}
        acked = bool(int(state_row.get("acked_until_clear") or 0))
        muted_until_raw = state_row.get("muted_until")
        muted_until_dt = _parse_timestamp_utc(muted_until_raw)
        muted_active = bool(muted_until_dt and muted_until_dt > now)
        mute_remaining_minutes = None
        if muted_active and muted_until_dt:
            mute_remaining_minutes = max(1, int((muted_until_dt - now).total_seconds() // 60))
        first_alarm_dt = _parse_timestamp_utc(state_row.get("first_alarm_at"))
        sustained_minutes = None
        if status == "critical" and first_alarm_dt:
            sustained_minutes = max(0, int((now - first_alarm_dt).total_seconds() // 60))

        row["acked_until_clear"] = acked
        row["muted_active"] = muted_active
        row["muted_until"] = muted_until_raw
        row["mute_remaining_minutes"] = mute_remaining_minutes
        row["sustained_minutes"] = sustained_minutes
        row["can_ack"] = bool(status == "critical" and not acked)
        row["can_mute"] = bool(status in {"critical", "warning"})
        row["can_unmute"] = bool(muted_active)
        decorated.append(row)

    return decorated, counts


def _smoke_sensor_name(property_id: str, sensor_id: str) -> str:
    sid = str(sensor_id or "").strip()
    if not sid:
        return ""
    source_row = db.get_latest_reading(property_id, source="hubitat_cloud")
    payload = _parse_raw_payload(source_row)
    for sensor in (payload.get("smoke_devices") or []):
        if str(sensor.get("entity_id") or "").strip() == sid:
            return str(sensor.get("friendly_name") or sid)
    row = db.get_smoke_sensor_state(property_id, sid)
    if row:
        return str(row.get("friendly_name") or sid)
    return sid


def _shutoff_valve_name(property_id: str, valve_id: str) -> str:
    did = str(valve_id or "").strip()
    if not did:
        return ""
    source_row = db.get_latest_reading(property_id, source="hubitat_cloud")
    payload = _parse_raw_payload(source_row)
    for valve in (payload.get("valve_devices") or []):
        if str(valve.get("entity_id") or "").strip() == did:
            return str(valve.get("friendly_name") or did)
    row = db.get_shutoff_valve_state(property_id, did)
    if row:
        return str(row.get("friendly_name") or did)
    return did


def _set_valve_expected_service_off(property_id: str,
                                    valve_id: str,
                                    friendly_name: str = "",
                                    expected_service_off: bool = True) -> None:
    did = str(valve_id or "").strip()
    if not did:
        return
    row = db.get_shutoff_valve_state(property_id, did) or {}
    property_cfg = _property_alert_cfg(property_id)
    last_service_off_at = row.get("last_closed_at")
    if expected_service_off and not last_service_off_at:
        last_service_off_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    last_state = row.get("last_state") or "unknown"
    if expected_service_off:
        last_state = water_service.water_action_to_raw_command("off", property_cfg, did) or last_state
    db.upsert_shutoff_valve_state(
        property_id=property_id,
        valve_id=did,
        friendly_name=friendly_name or row.get("friendly_name") or did,
        last_state=last_state,
        last_closed_at=last_service_off_at,
        acked_until_open=False if expected_service_off else bool(int(row.get("acked_until_open") or 0)),
        expected_closed=expected_service_off,
        trigger_sensor_id=None if expected_service_off else row.get("trigger_sensor_id"),
        trigger_sensor_name=None if expected_service_off else row.get("trigger_sensor_name"),
    )


def _ack_valve_incident(property_id: str,
                        valve_id: str,
                        actor: str = "api") -> dict:
    did = str(valve_id or "").strip()
    name = _shutoff_valve_name(property_id, did) or did
    row = db.get_shutoff_valve_state(property_id, did) or {}
    acked = bool(int(row.get("acked_until_open") or 0))
    cleared = db.resolve_alerts_for_sensor(property_id, "water_shutoff", did)
    if not row and not cleared:
        return {
            "status": "no_incident",
            "property_id": property_id,
            "device_id": did,
            "friendly_name": name,
            "cleared_alerts": 0,
        }

    if not acked:
        db.set_shutoff_valve_ack(
            property_id,
            did,
            acked_until_open=True,
            friendly_name=name,
        )
        _record_system_event(
            event_type="water_incident_acknowledged",
            level="info",
            property_id=property_id,
            actor=actor,
            message=f"Water incident acknowledged: {name}",
            details={
                "device_id": did,
                "valve_id": did,
                "friendly_name": name,
                "cleared_alerts": int(cleared),
                "trigger_sensor_id": row.get("trigger_sensor_id"),
                "trigger_sensor_name": row.get("trigger_sensor_name"),
            },
        )

    return {
        "status": "acknowledged" if not acked else "already_acknowledged",
        "property_id": property_id,
        "device_id": did,
        "friendly_name": name,
        "cleared_alerts": int(cleared),
    }


def _resolve_valve_incident(property_id: str,
                            valve_id: str,
                            friendly_name: str = "",
                            actor: str = "api",
                            resolution: str = "water_turned_on") -> dict:
    did = str(valve_id or "").strip()
    row = db.get_shutoff_valve_state(property_id, did) or {}
    property_cfg = _property_alert_cfg(property_id)
    name = friendly_name or row.get("friendly_name") or _shutoff_valve_name(property_id, did) or did
    acked = bool(int(row.get("acked_until_open") or 0))
    expected_service_off = bool(int(row.get("expected_closed") or 0))
    trigger_sensor_id = row.get("trigger_sensor_id")
    trigger_sensor_name = row.get("trigger_sensor_name")
    last_service_state = _valve_service_meta(
        property_cfg,
        did,
        str(row.get("last_state") or "").strip().lower(),
    ).get("service_state")
    cleared = db.resolve_alerts_for_sensor(property_id, "water_shutoff", did)
    should_log = bool(
        cleared
        or acked
        or (
            last_service_state == "off"
            and not expected_service_off
        )
        or trigger_sensor_id
        or trigger_sensor_name
    )
    if should_log:
        _record_system_event(
            event_type="water_incident_resolved",
            level="info",
            property_id=property_id,
            actor=actor,
            message=f"Water incident resolved: {name} water turned on",
            details={
                "device_id": did,
                "valve_id": did,
                "friendly_name": name,
                "resolved_alerts": int(cleared),
                "trigger_sensor_id": trigger_sensor_id,
                "trigger_sensor_name": trigger_sensor_name,
                "acknowledged": acked,
                "resolution": resolution,
            },
        )

    db.upsert_shutoff_valve_state(
        property_id=property_id,
        valve_id=did,
        friendly_name=name,
        last_state=(
            water_service.water_action_to_raw_command("on", property_cfg, did)
            or str(row.get("last_state") or "unknown").strip().lower()
            or "unknown"
        ),
        last_closed_at=None,
        acked_until_open=False,
        expected_closed=False,
        trigger_sensor_id=None,
        trigger_sensor_name=None,
    )
    return {
        "status": "resolved" if should_log or cleared else "no_incident",
        "property_id": property_id,
        "device_id": did,
        "friendly_name": name,
        "resolved_alerts": int(cleared),
    }


def _ack_active_valve_incidents(property_id: str,
                                actor: str = "api") -> dict:
    active_alerts = _active_alert_map("water_shutoff").get(property_id) or {}
    state_map = db.get_shutoff_valve_state_map(property_id)
    property_cfg = _property_alert_cfg(property_id)
    valve_ids = set(active_alerts.keys())
    for did, row in (state_map or {}).items():
        if (
            _valve_service_meta(
                property_cfg,
                did,
                str(row.get("last_state") or "").strip().lower(),
            ).get("service_state") == "off"
            and not bool(int(row.get("expected_closed") or 0))
        ):
            valve_ids.add(did)

    acked = 0
    cleared = 0
    for did in sorted(valve_ids):
        result = _ack_valve_incident(property_id, did, actor=actor)
        if result.get("status") in {"acknowledged", "already_acknowledged"}:
            acked += 1
        cleared += int(result.get("cleared_alerts") or 0)
    return {
        "acknowledged": acked,
        "cleared_alerts": cleared,
    }


def _decode_system_event_rows(rows: list[dict]) -> list[dict]:
    out = []
    for row in rows:
        item = dict(row)
        raw_details = item.get("details_json")
        details = None
        if raw_details:
            try:
                details = json.loads(raw_details)
            except Exception:
                details = {"raw": str(raw_details)}
        item["details"] = details
        out.append(item)
    return out


# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Safety Monitor starting up...")
    scheduler.start(CONFIG)
    _record_system_event(
        event_type="service_started",
        level="info",
        actor="service",
        message="Safety Monitor service started",
    )
    yield
    _record_system_event(
        event_type="service_stopping",
        level="warning",
        actor="service",
        message="Safety Monitor service shutting down",
    )
    logger.info("Safety Monitor shutting down...")
    scheduler.stop()


app = FastAPI(
    title=CONFIG.get("web", {}).get("title", "Safety Monitor"),
    lifespan=lifespan,
)

BASE_DIR = os.path.dirname(__file__)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "app/templates"))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "app/static")), name="static")


def _static_asset_version(rel_path: str) -> str:
    asset_path = os.path.join(BASE_DIR, "app/static", rel_path)
    try:
        return str(int(os.path.getmtime(asset_path)))
    except Exception:
        return "0"


def _apply_no_cache(response: Response) -> Response:
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.middleware("http")
async def disable_cache_for_live_views(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path or "/"
    if path == "/" or path.startswith("/devices/") or path.startswith("/temperatures/") or path == "/decisions" or path.startswith("/api/"):
        return _apply_no_cache(response)
    return response

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
    global_smoke_cfg = global_alerts_cfg.get("smoke", {})
    lock_warning_map = _recent_lock_warning_map(limit=500)
    valve_warning_map = _recent_valve_warning_map(limit=500)
    valve_incident_alert_map = _active_alert_map("water_shutoff")
    water_event_map = _recent_device_event_map(
        {"water_sensor_wet", "water_sensor_cleared"},
        "sensor_id",
        limit=500,
    )
    valve_event_map = _recent_device_event_map(
        {
            "water_shutoff_closed",
            "water_shutoff_opened",
            "water_incident_opened",
            "water_incident_acknowledged",
            "water_incident_resolved",
            "valve_command_state_unchanged",
        },
        "device_id",
        limit=500,
    )
    cards = []
    for p in props:
        pid  = p["id"]
        row  = latest.get(pid, {})
        devs = db.get_hubitat_devices(pid)
        pcfg = p.get("alerts", {})
        collector_types = {
            str(c.get("type", "")).strip()
            for c in p.get("collectors", [])
            if c.get("type")
        }
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
        feed_health = []
        source_rows: dict[str, dict | None] = {}
        for source_type, label in (
            ("hubitat_cloud", "Hubitat API response"),
            ("ha_api", "Home Assistant API response"),
            ("eg4", "EG4 API response"),
            ("victron", "Victron API response"),
        ):
            if source_type not in collector_types:
                continue
            source_row = db.get_latest_reading(pid, source=source_type)
            source_rows[source_type] = source_row
            feed_health.append(_collector_feed_health(
                label, source_row.get("collected_at") if source_row else None
            ))

        hub_payload = _parse_raw_payload(source_rows.get("hubitat_cloud"))
        hub_collected_at = source_rows.get("hubitat_cloud", {}).get("collected_at") if source_rows.get("hubitat_cloud") else None
        valve_state_map = db.get_shutoff_valve_state_map(pid)
        lock_devices, lock_counts = _decorate_lock_devices(
            list(hub_payload.get("lock_devices") or [])
        )
        valve_devices, valve_counts = _decorate_valve_devices(
            list(hub_payload.get("valve_devices") or []),
            property_cfg=pcfg,
            collected_at=hub_collected_at,
            recent_event_map=valve_event_map.get(pid) or {},
            valve_state_map=valve_state_map,
            active_alert_map=valve_incident_alert_map.get(pid) or {},
        )
        water_devices, water_counts = _decorate_water_devices(
            list(hub_payload.get("water_sensors") or []),
            collected_at=hub_collected_at,
            recent_event_map=water_event_map.get(pid) or {},
        )
        prop_lock_warnings = lock_warning_map.get(pid) or {}
        prop_valve_warnings = valve_warning_map.get(pid) or {}
        for lock in lock_devices:
            did = str(lock.get("entity_id") or "").strip()
            warning = prop_lock_warnings.get(did)
            if not warning:
                lock["command_warning"] = None
                continue
            expected = str(warning.get("expected_state") or "").strip().lower()
            observed = str(lock.get("state") or "").strip().lower()
            # Hide stale warning once lock converges to expected state.
            lock["command_warning"] = warning if (not expected or observed != expected) else None
        for valve in valve_devices:
            did = str(valve.get("entity_id") or "").strip()
            warning = prop_valve_warnings.get(did)
            if not warning:
                valve["command_warning"] = None
                continue
            expected = str(warning.get("expected_state") or "").strip().lower()
            observed = str(valve.get("state") or "").strip().lower()
            valve["command_warning"] = warning if (not expected or observed != expected) else None
        smoke_state_map = db.get_smoke_sensor_state_map(pid)
        smoke_devices, smoke_counts = _decorate_smoke_devices(
            list(hub_payload.get("smoke_devices") or []),
            smoke_state_map=smoke_state_map,
        )
        source_warnings = row.get("source_warnings") if isinstance(row.get("source_warnings"), list) else []

        cards.append({
            "id":             pid,
            "name":           p.get("name", pid),
            "enabled":        p.get("enabled", True),
            "has_hubitat":    "hubitat_cloud" in collector_types,
            "reading":        row,
            "devices":        devs,
            "feed_health":    feed_health,
            "source_warnings": source_warnings,
            "lock_devices":   lock_devices,
            "lock_counts":    lock_counts,
            "valve_devices":  valve_devices,
            "valve_counts":   valve_counts,
            "smoke_devices":  smoke_devices,
            "smoke_counts":   smoke_counts,
            "water_devices":  water_devices,
            "water_counts":   water_counts,
            "smoke_sustain_minutes": int(pcfg.get("smoke_sustain_minutes",
                                      global_smoke_cfg.get("sustain_minutes", 3))),
            "smoke_mute_default_minutes": int(pcfg.get("smoke_mute_default_minutes",
                                           global_smoke_cfg.get("mute_default_minutes", 60))),
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
                "temperature_cooldown_minutes": pcfg.get(
                    "temperature_cooldown_minutes",
                    global_temp_cfg.get("cooldown_minutes", 60),
                ),
                "temperature_pushover_enabled": pcfg.get(
                    "temperature_pushover_enabled",
                    global_temp_cfg.get("pushover_enabled", True),
                ),
                "temp_graph_hours":    pcfg.get("temp_graph_hours",
                                            global_temp_cfg.get("graph_hours", 24)),
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
                "smoke_sustain_minutes": pcfg.get(
                    "smoke_sustain_minutes",
                    global_smoke_cfg.get("sustain_minutes", 3),
                ),
                "smoke_cooldown_minutes": pcfg.get(
                    "smoke_cooldown_minutes",
                    global_smoke_cfg.get("cooldown_minutes", 60),
                ),
                "smoke_mute_default_minutes": pcfg.get(
                    "smoke_mute_default_minutes",
                    global_smoke_cfg.get("mute_default_minutes", 60),
                ),
                "smoke_pushover_enabled": pcfg.get(
                    "smoke_pushover_enabled",
                    global_smoke_cfg.get("pushover_enabled", True),
                ),
                "suppress_maker_device_alerts": pcfg.get(
                    "suppress_maker_device_alerts",
                    False,
                ),
                "suppress_maker_devices": pcfg.get(
                    "suppress_maker_devices",
                    [],
                ),
            },
        })

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "cards":   cards,
        "alerts":  alerts,
        "container_health": container_health,
        "config":  CONFIG,
        "static_version": _static_asset_version("css/monitor-ui.css"),
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
        "static_version": _static_asset_version("css/monitor-ui.css"),
    })


@app.get("/temperatures/{property_id}", response_class=HTMLResponse)
async def all_temperatures(request: Request, property_id: str, sensor: str = ""):
    """Per-property temperature view with current values and recent trend graph."""
    props = CONFIG.get("properties", [])
    prop = next((p for p in props if p["id"] == property_id), None)
    if not prop:
        return HTMLResponse(f"<h1>Property '{property_id}' not found</h1>", status_code=404)

    row = db.get_latest_reading(property_id, source="merged") or {}
    all_temps: dict[str, float] = {}
    if row:
        raw_str = row.get("raw_json")
        if raw_str:
            try:
                raw = json.loads(raw_str)
                all_temps = dict(raw.get("all_temps") or {})
            except Exception:
                all_temps = {}

    pcfg = prop.get("alerts", {})
    global_temp = (CONFIG.get("alerts", {}) or {}).get("temperature", {})
    try:
        graph_hours = int(float(pcfg.get("temp_graph_hours", global_temp.get("graph_hours", 24))))
    except Exception:
        graph_hours = 24
    graph_hours = max(1, min(graph_hours, 24 * 30))

    outdoors = {str(s).strip().lower() for s in (pcfg.get("outdoor_sensors") or [])}
    excludes = {str(s).strip().lower() for s in (pcfg.get("exclude_sensors") or [])}
    sensors = []
    for name in sorted(all_temps.keys(), key=lambda s: s.lower()):
        key = str(name).strip().lower()
        cls = "excluded" if key in excludes else ("outdoor" if key in outdoors else "indoor")
        sensors.append({
            "name": name,
            "value": all_temps.get(name),
            "classification": cls,
        })

    selected_sensor = ""
    if sensor and sensor in all_temps:
        selected_sensor = sensor
    elif sensors:
        selected_sensor = sensors[0]["name"]

    graph_series = []
    graph_labels = []
    graph_values = []
    if selected_sensor:
        graph_series = db.get_temperature_history(property_id, selected_sensor, hours=graph_hours)
        graph_labels = [str(p.get("collected_at")) for p in graph_series]
        graph_values = [p.get("temperature_f") for p in graph_series]

    return templates.TemplateResponse("all_temperatures.html", {
        "request": request,
        "prop": prop,
        "config": CONFIG,
        "static_version": _static_asset_version("css/monitor-ui.css"),
        "latest_collected_at": row.get("collected_at") if row else None,
        "graph_hours": graph_hours,
        "sensors": sensors,
        "selected_sensor": selected_sensor,
        "graph_series": graph_series,
        "graph_labels": graph_labels,
        "graph_values": graph_values,
    })


@app.get("/decisions", response_class=HTMLResponse)
async def system_decisions(request: Request,
                           level: str = "",
                           property_id: str = "",
                           event_type: str = "",
                           limit: int = 250,
                           cursor: int = 0):
    """Dedicated page for critical system decisions/operator actions."""
    def _query(params: dict) -> str:
        q = {k: v for k, v in params.items() if v not in (None, "", 0)}
        return urlencode(q)

    level_filter = level.strip().lower() or None
    property_filter = property_id.strip() or None
    type_filter = event_type.strip().lower() or None
    try:
        lim = max(1, min(int(limit), 1000))
    except Exception:
        lim = 250
    cursor_id = int(cursor) if int(cursor or 0) > 0 else None

    rows, next_cursor = db.get_system_events_page(
        limit=lim,
        cursor=cursor_id,
        level=level_filter,
        property_id=property_filter,
        event_type=type_filter,
    )
    events = _decode_system_event_rows(rows)
    base_params = {
        "level": level_filter or "",
        "property_id": property_filter or "",
        "event_type": type_filter or "",
        "limit": lim,
    }
    newer_url = "/decisions"
    newer_q = _query(base_params)
    if newer_q:
        newer_url = f"/decisions?{newer_q}"

    older_url = None
    if next_cursor:
        older_q = _query({**base_params, "cursor": int(next_cursor)})
        older_url = f"/decisions?{older_q}" if older_q else "/decisions"

    export_limit = max(1000, lim)
    csv_q = _query({**base_params, "cursor": cursor_id or "", "limit": export_limit, "format": "csv"})
    json_q = _query({**base_params, "cursor": cursor_id or "", "limit": export_limit, "format": "json"})

    return templates.TemplateResponse("system_decisions.html", {
        "request": request,
        "config": CONFIG,
        "static_version": _static_asset_version("css/monitor-ui.css"),
        "events": events,
        "filters": {
            "level": level_filter or "",
            "property_id": property_filter or "",
            "event_type": type_filter or "",
            "limit": lim,
            "cursor": int(cursor_id or 0),
        },
        "page": {
            "next_cursor": next_cursor,
            "count": len(events),
            "older_url": older_url,
            "newer_url": newer_url,
        },
        "exports": {
            "csv_url": f"/api/system/decisions/export?{csv_q}" if csv_q else "/api/system/decisions/export?format=csv",
            "json_url": f"/api/system/decisions/export?{json_q}" if json_q else "/api/system/decisions/export?format=json",
        },
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


@app.get("/api/property/{property_id}/maker-devices")
async def api_property_maker_devices(property_id: str):
    """
    List Maker API (Hubitat) devices known for a property, including
    per-device activity status and inferred alert roles.
    """
    prop = _get_property_cfg(property_id)
    if not prop:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    pcfg = prop.get("alerts", {})
    suppressed = {
        str(v).strip().lower()
        for v in (pcfg.get("suppress_maker_devices") or [])
        if str(v).strip()
    }
    roles_by_id, roles_by_name = _maker_device_roles(property_id)

    devices = []
    for row in db.get_hubitat_devices(property_id):
        device_id = str(row.get("entity_id") or "").strip()
        friendly_name = str(row.get("friendly_name") or device_id or "").strip()
        role_set = set(roles_by_id.get(device_id) or [])
        role_set.update(roles_by_name.get(friendly_name.lower()) or set())
        activity = _maker_device_activity(
            row.get("last_activity"),
            row.get("collected_at"),
        )
        devices.append({
            "entity_id": device_id,
            "friendly_name": friendly_name,
            "device_type": str(row.get("device_type") or "").strip(),
            "battery_pct": row.get("battery_pct"),
            "last_activity": row.get("last_activity"),
            "collected_at": row.get("collected_at"),
            "activity_status": activity.get("status"),
            "activity_display": activity.get("last_activity"),
            "roles": sorted(role_set),
            "suppressed": (
                device_id.lower() in suppressed
                or friendly_name.lower() in suppressed
            ),
        })

    devices.sort(key=lambda d: (str(d.get("friendly_name") or "").lower(), str(d.get("entity_id") or "")))
    return JSONResponse(content={"property_id": property_id, "devices": devices})


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


@app.get("/api/system/decisions")
async def api_system_decisions(limit: int = 200,
                               level: str = "",
                               property_id: str = "",
                               event_type: str = "",
                               cursor: int = 0):
    cursor_id = int(cursor) if int(cursor or 0) > 0 else None
    rows, next_cursor = db.get_system_events_page(
        limit=limit,
        cursor=cursor_id,
        level=level.strip().lower() or None,
        property_id=property_id.strip() or None,
        event_type=event_type.strip().lower() or None,
    )
    events = _decode_system_event_rows(rows)
    resp = JSONResponse(content=events)
    if next_cursor:
        resp.headers["X-Next-Cursor"] = str(next_cursor)
    return resp


@app.get("/api/system/decisions/export")
async def api_system_decisions_export(format: str = "csv",
                                      limit: int = 5000,
                                      level: str = "",
                                      property_id: str = "",
                                      event_type: str = "",
                                      cursor: int = 0):
    """Export decision log rows for incident review (CSV or JSON)."""
    fmt = str(format or "csv").strip().lower()
    if fmt not in {"csv", "json"}:
        return JSONResponse(status_code=400, content={"error": "format must be 'csv' or 'json'"})

    try:
        lim = max(1, min(int(limit), 10000))
    except Exception:
        lim = 5000
    cursor_id = int(cursor) if int(cursor or 0) > 0 else None
    rows, next_cursor = db.get_system_events_page(
        limit=lim,
        cursor=cursor_id,
        level=level.strip().lower() or None,
        property_id=property_id.strip() or None,
        event_type=event_type.strip().lower() or None,
    )
    events = _decode_system_event_rows(rows)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    if fmt == "json":
        payload = {
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "count": len(events),
            "next_cursor": next_cursor,
            "filters": {
                "level": level.strip().lower() or "",
                "property_id": property_id.strip() or "",
                "event_type": event_type.strip().lower() or "",
                "cursor": int(cursor_id or 0),
                "limit": lim,
            },
            "events": events,
        }
        return JSONResponse(
            content=payload,
            headers={"Content-Disposition": f'attachment; filename="decisions_{stamp}.json"'},
        )

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "created_at", "level", "event_type", "property_id", "actor", "message", "details_json"])
    for ev in events:
        writer.writerow([
            ev.get("id"),
            ev.get("created_at"),
            ev.get("level"),
            ev.get("event_type"),
            ev.get("property_id"),
            ev.get("actor"),
            ev.get("message"),
            json.dumps(ev.get("details") or {}, ensure_ascii=True, sort_keys=True),
        ])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="decisions_{stamp}.csv"'},
    )


@app.post("/api/property/{property_id}/locks/all/{action}")
async def api_lock_all(property_id: str, action: str,
                       _auth=Depends(_require_write_auth)):
    """Lock/unlock all locks for a property via Hubitat Maker API."""
    cmd = str(action or "").strip().lower()
    if cmd not in {"lock", "unlock"}:
        return JSONResponse(status_code=400, content={"error": "Action must be 'lock' or 'unlock'"})

    prop = _get_property_cfg(property_id)
    if not prop:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    client = _hubitat_client_for_property(property_id)
    if not client:
        return JSONResponse(status_code=400, content={"error": "Property has no Hubitat collector configured"})

    try:
        locks = client.get_lock_devices()
        result = client.command_locks(cmd, locks)
    except Exception as exc:
        _record_system_event(
            event_type="lock_command_all_failed",
            level="error",
            property_id=property_id,
            actor="api",
            message=f"{cmd.title()} all locks failed",
            details={"error": str(exc)},
        )
        return JSONResponse(status_code=502, content={"error": str(exc)})

    _record_system_event(
        event_type="lock_command_all",
        level="warning" if cmd == "unlock" else "info",
        property_id=property_id,
        actor="api",
        message=f"{cmd.title()} all locks requested",
        details={
            "attempted": result.get("attempted"),
            "succeeded": result.get("succeeded"),
            "failed": result.get("failed"),
        },
    )

    attempted_ids = []
    name_hints: dict[str, str] = {}
    for row in (result.get("results") or []):
        did = str(row.get("device_id") or "").strip()
        if not did:
            continue
        attempted_ids.append(did)
        name_hints[did] = str(row.get("friendly_name") or did)

    warnings: list[dict] = []
    if attempted_ids:
        expected = _expected_lock_state(cmd)
        verify = _verify_lock_transition(
            client,
            expected_states={did: expected for did in attempted_ids},
            name_hints=name_hints,
            polls=4,
            wait_seconds=3,
        )
        if not verify.get("ok"):
            warnings = _record_lock_state_unchanged(
                property_id=property_id,
                action=cmd,
                unresolved=verify.get("unresolved") or [],
            )

    # Lock hardware/cloud states can settle a few seconds after command ACK.
    # Run a short rolling refresh window so dashboard lock pills converge fast.
    _schedule_collection_refresh(
        retries=4,
        wait_seconds=8,
        always_run=True,
        initial_delay=2,
    )
    return JSONResponse(content={
        "status": "ok",
        "property_id": property_id,
        "action": cmd,
        "result": result,
        "warning": ({
            "message": f"Command sent but device status not changing for {len(warnings)} lock(s)",
            "count": len(warnings),
            "devices": warnings,
        } if warnings else None),
    })


@app.post("/api/property/{property_id}/locks/{device_id}/{action}")
async def api_lock_device(property_id: str, device_id: str, action: str,
                          _auth=Depends(_require_write_auth)):
    """Lock/unlock one lock device for a property."""
    cmd = str(action or "").strip().lower()
    if cmd not in {"lock", "unlock"}:
        return JSONResponse(status_code=400, content={"error": "Action must be 'lock' or 'unlock'"})

    prop = _get_property_cfg(property_id)
    if not prop:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    client = _hubitat_client_for_property(property_id)
    if not client:
        return JSONResponse(status_code=400, content={"error": "Property has no Hubitat collector configured"})

    lock_name = device_id
    try:
        known_locks = client.get_lock_devices()
        for lock in known_locks:
            if str(lock.get("entity_id")) == str(device_id):
                lock_name = lock.get("friendly_name") or device_id
                break
        result = client.command_device(device_id, cmd)
    except Exception as exc:
        _record_system_event(
            event_type="lock_command_failed",
            level="error",
            property_id=property_id,
            actor="api",
            message=f"{cmd.title()} lock failed: {lock_name}",
            details={"device_id": str(device_id), "error": str(exc)},
        )
        return JSONResponse(status_code=502, content={"error": str(exc)})

    _record_system_event(
        event_type="lock_command",
        level="warning" if cmd == "unlock" else "info",
        property_id=property_id,
        actor="api",
        message=f"{cmd.title()} lock requested: {lock_name}",
        details={"device_id": str(device_id), "friendly_name": lock_name},
    )

    warnings: list[dict] = []
    verify = _verify_lock_transition(
        client,
        expected_states={str(device_id): _expected_lock_state(cmd)},
        name_hints={str(device_id): str(lock_name)},
        polls=4,
        wait_seconds=3,
    )
    if not verify.get("ok"):
        warnings = _record_lock_state_unchanged(
            property_id=property_id,
            action=cmd,
            unresolved=verify.get("unresolved") or [],
        )

    _schedule_collection_refresh(
        retries=4,
        wait_seconds=8,
        always_run=True,
        initial_delay=2,
    )
    return JSONResponse(content={
        "status": "ok",
        "property_id": property_id,
        "device_id": str(device_id),
        "action": cmd,
        "result": result,
        "warning": (warnings[0] if warnings else None),
    })


@app.post("/api/property/{property_id}/valves/all/{action}")
async def api_valve_all(property_id: str, action: str,
                        _auth=Depends(_require_write_auth)):
    """Turn water on/off for all shutoff valves in a property."""
    cmd = str(action or "").strip().lower()
    if cmd not in {"on", "off", "open", "close"}:
        return JSONResponse(status_code=400, content={"error": "Action must be 'on', 'off', 'open', or 'close'"})

    prop = _get_property_cfg(property_id)
    if not prop:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})
    property_cfg = prop.get("alerts", {}) or {}

    client = _hubitat_client_for_property(property_id)
    if not client:
        return JSONResponse(status_code=400, content={"error": "Property has no Hubitat collector configured"})

    try:
        valves = client.get_valve_devices()
        if cmd in {"on", "off"}:
            results = []
            attempted = 0
            succeeded = 0
            for valve in valves:
                did = str(valve.get("entity_id") or "").strip()
                if not did:
                    continue
                raw_cmd = water_service.water_action_to_raw_command(cmd, property_cfg, did)
                if raw_cmd not in {"open", "close"}:
                    continue
                attempted += 1
                try:
                    res = client.command_device(did, raw_cmd)
                    res["friendly_name"] = valve.get("friendly_name") or did
                    res["requested_action"] = cmd
                    results.append(res)
                    succeeded += 1
                except Exception as exc:
                    results.append({
                        "device_id": did,
                        "friendly_name": valve.get("friendly_name") or did,
                        "command": raw_cmd,
                        "requested_action": cmd,
                        "ok": False,
                        "error": str(exc),
                    })
            result = {
                "attempted": attempted,
                "succeeded": succeeded,
                "failed": attempted - succeeded,
                "results": results,
            }
        else:
            result = client.command_valves(cmd, valves)
    except Exception as exc:
        _record_system_event(
            event_type="valve_command_all_failed",
            level="error",
            property_id=property_id,
            actor="api",
            message=f"{_valve_service_action_label(cmd)} failed for all shutoff valves",
            details={"error": str(exc)},
        )
        return JSONResponse(status_code=502, content={"error": str(exc)})

    _record_system_event(
        event_type="valve_command_all",
        level="warning" if cmd in {"off", "close"} else "info",
        property_id=property_id,
        actor="api",
        message=f"{_valve_service_action_label(cmd)} requested for all shutoff valves",
        details={
            "requested_action": cmd,
            "attempted": result.get("attempted"),
            "succeeded": result.get("succeeded"),
            "failed": result.get("failed"),
        },
    )

    attempted_ids = []
    name_hints: dict[str, str] = {}
    expected_states: dict[str, str] = {}
    requested_service_actions: dict[str, str] = {}
    for row in (result.get("results") or []):
        did = str(row.get("device_id") or "").strip()
        if not did:
            continue
        attempted_ids.append(did)
        name_hints[did] = str(row.get("friendly_name") or did)
        raw_cmd = str(row.get("command") or "").strip().lower()
        if raw_cmd in {"open", "close"}:
            expected_states[did] = _expected_valve_state(raw_cmd)
        if cmd in {"on", "off"}:
            requested_service_actions[did] = cmd
        elif raw_cmd in {"open", "close"}:
            service_action = _raw_valve_command_service_state(property_cfg, did, raw_cmd)
            if service_action in {"on", "off"}:
                requested_service_actions[did] = service_action

    warnings: list[dict] = []
    confirmed_ids: list[str] = []
    if attempted_ids:
        verify = _verify_valve_transition(
            client,
            expected_states=expected_states,
            name_hints=name_hints,
            polls=4,
            wait_seconds=3,
        )
        if not verify.get("ok"):
            warnings = _record_valve_state_unchanged(
                property_id=property_id,
                action=cmd,
                unresolved=verify.get("unresolved") or [],
            )
        unresolved_ids = {
            str(row.get("device_id") or "").strip()
            for row in (verify.get("unresolved") or [])
            if str(row.get("device_id") or "").strip()
        }
        confirmed_ids = [did for did in attempted_ids if did not in unresolved_ids]

    for did in confirmed_ids:
        service_action = requested_service_actions.get(did)
        if service_action == "off":
            _set_valve_expected_service_off(
                property_id,
                did,
                friendly_name=name_hints.get(did, did),
                expected_service_off=True,
            )
        elif service_action == "on":
            _resolve_valve_incident(
                property_id,
                did,
                friendly_name=name_hints.get(did, did),
                actor="api",
                resolution="water_turned_on_via_api",
            )

    _schedule_collection_refresh(
        retries=4,
        wait_seconds=8,
        always_run=True,
        initial_delay=2,
    )
    return JSONResponse(content={
        "status": "ok",
        "property_id": property_id,
        "action": cmd,
        "result": result,
        "warning": ({
            "message": f"Command sent but valve status not changing for {len(warnings)} valve(s)",
            "count": len(warnings),
            "devices": warnings,
        } if warnings else None),
    })


@app.post("/api/property/{property_id}/valves/{device_id}/{action}")
async def api_valve_device(property_id: str, device_id: str, action: str,
                           _auth=Depends(_require_write_auth)):
    """Turn water on/off for one shutoff valve device."""
    cmd = str(action or "").strip().lower()
    if cmd not in {"on", "off", "open", "close"}:
        return JSONResponse(status_code=400, content={"error": "Action must be 'on', 'off', 'open', or 'close'"})

    prop = _get_property_cfg(property_id)
    if not prop:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})
    property_cfg = prop.get("alerts", {}) or {}

    client = _hubitat_client_for_property(property_id)
    if not client:
        return JSONResponse(status_code=400, content={"error": "Property has no Hubitat collector configured"})

    valve_name = device_id
    try:
        known_valves = client.get_valve_devices()
        for valve in known_valves:
            if str(valve.get("entity_id")) == str(device_id):
                valve_name = valve.get("friendly_name") or device_id
                break
        raw_cmd = water_service.water_action_to_raw_command(cmd, property_cfg, str(device_id))
        if raw_cmd not in {"open", "close"}:
            return JSONResponse(status_code=400, content={"error": f"Unsupported valve action '{cmd}'"})
        result = client.command_device(device_id, raw_cmd)
    except Exception as exc:
        _record_system_event(
            event_type="valve_command_failed",
            level="error",
            property_id=property_id,
            actor="api",
            message=f"{_valve_service_action_label(cmd)} failed: {valve_name}",
            details={"device_id": str(device_id), "requested_action": cmd, "error": str(exc)},
        )
        return JSONResponse(status_code=502, content={"error": str(exc)})

    _record_system_event(
        event_type="valve_command",
        level="warning" if cmd in {"off", "close"} else "info",
        property_id=property_id,
        actor="api",
        message=f"{_valve_service_action_label(cmd)} requested: {valve_name}",
        details={"device_id": str(device_id), "friendly_name": valve_name, "requested_action": cmd},
    )

    warnings: list[dict] = []
    expected_raw = water_service.water_action_to_raw_command(cmd, property_cfg, str(device_id))
    verify = _verify_valve_transition(
        client,
        expected_states={str(device_id): _expected_valve_state(expected_raw)},
        name_hints={str(device_id): str(valve_name)},
        polls=4,
        wait_seconds=3,
    )
    if not verify.get("ok"):
        warnings = _record_valve_state_unchanged(
            property_id=property_id,
            action=cmd,
            unresolved=verify.get("unresolved") or [],
        )

    unresolved_ids = {
        str(row.get("device_id") or "").strip()
        for row in (verify.get("unresolved") or [])
        if str(row.get("device_id") or "").strip()
    }
    if str(device_id) not in unresolved_ids:
        service_action = cmd if cmd in {"on", "off"} else _raw_valve_command_service_state(
            property_cfg,
            str(device_id),
            expected_raw,
        )
        if service_action == "off":
            _set_valve_expected_service_off(
                property_id,
                str(device_id),
                friendly_name=valve_name,
                expected_service_off=True,
            )
        elif service_action == "on":
            _resolve_valve_incident(
                property_id,
                str(device_id),
                friendly_name=valve_name,
                actor="api",
                resolution="water_turned_on_via_api",
            )

    _schedule_collection_refresh(
        retries=4,
        wait_seconds=8,
        always_run=True,
        initial_delay=2,
    )
    return JSONResponse(content={
        "status": "ok",
        "property_id": property_id,
        "device_id": str(device_id),
        "action": cmd,
        "raw_action": expected_raw,
        "result": result,
        "warning": (warnings[0] if warnings else None),
    })


@app.post("/api/property/{property_id}/valves/{device_id}/ack")
async def api_ack_valve_incident(property_id: str, device_id: str,
                                 _auth=Depends(_require_write_auth)):
    """Acknowledge an active shutoff-valve safety incident until water turns on."""
    known_pids = {p.get("id") for p in CONFIG.get("properties", []) if p.get("id")}
    if property_id not in known_pids:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    did = str(device_id or "").strip()
    if not did:
        return JSONResponse(status_code=400, content={"error": "device_id is required"})

    result = _ack_valve_incident(property_id, did, actor="api")
    _schedule_collection_refresh(
        retries=2,
        wait_seconds=5,
        always_run=True,
        initial_delay=0,
    )
    return JSONResponse(content=result)


@app.post("/api/system/reboot")
async def api_system_reboot(_auth=Depends(_require_write_auth)):
    """
    Schedule a host/container reboot shortly after responding.
    This keeps the UI call deterministic before connectivity drops.
    """
    cmd = _reboot_command()
    if not cmd:
        return JSONResponse(
            status_code=500,
            content={"error": "No permitted reboot command available on host"},
        )

    delay_seconds = 2

    def _reboot_later():
        time.sleep(delay_seconds)
        try:
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            logger.exception("Container reboot command failed: %s", cmd)

    threading.Thread(target=_reboot_later, daemon=True).start()
    _record_system_event(
        event_type="system_reboot_requested",
        level="warning",
        actor="api",
        message="Container reboot requested from dashboard",
        details={"delay_seconds": delay_seconds},
    )
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
    global_smoke = global_alerts.get("smoke", {})
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
            "temperature_cooldown_minutes": pcfg.get(
                "temperature_cooldown_minutes",
                global_temp.get("cooldown_minutes", 60),
            ),
            "temperature_pushover_enabled": pcfg.get(
                "temperature_pushover_enabled",
                global_temp.get("pushover_enabled", True),
            ),
            "temp_graph_hours":      pcfg.get("temp_graph_hours",
                                         global_temp.get("graph_hours", 24)),
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
            "smoke_sustain_minutes": pcfg.get(
                "smoke_sustain_minutes",
                global_smoke.get("sustain_minutes", 3),
            ),
            "smoke_cooldown_minutes": pcfg.get(
                "smoke_cooldown_minutes",
                global_smoke.get("cooldown_minutes", 60),
            ),
            "smoke_mute_default_minutes": pcfg.get(
                "smoke_mute_default_minutes",
                global_smoke.get("mute_default_minutes", 60),
            ),
            "smoke_pushover_enabled": pcfg.get(
                "smoke_pushover_enabled",
                global_smoke.get("pushover_enabled", True),
            ),
            "suppress_maker_device_alerts": pcfg.get(
                "suppress_maker_device_alerts",
                False,
            ),
            "suppress_maker_devices": pcfg.get(
                "suppress_maker_devices",
                [],
            ),
        }
    return JSONResponse(content=result)


@app.post("/api/config/thresholds/{pid}")
async def update_thresholds(pid: str, request: Request,
                             _auth=Depends(_require_write_auth)):
    """Update per-property alert thresholds and persist to config.yaml."""
    def _as_bool(val):
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        if isinstance(val, str):
            return val.strip().lower() in {"1", "true", "yes", "on"}
        return False

    def _as_finite_float(val):
        try:
            num = float(val)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(num):
            return None
        return num

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
    minute_keys = {
        "temperature_cooldown_minutes",
        "battery_cooldown_minutes",
        "offline_timeout_minutes",
        "offline_cooldown_minutes",
        "smoke_sustain_minutes",
        "smoke_cooldown_minutes",
        "smoke_mute_default_minutes",
    }
    for key in ("indoor_temp_warning", "indoor_temp_critical",
                "temperature_cooldown_minutes",
                "outdoor_temp_warning", "outdoor_temp_critical",
                "battery_low_threshold_percent",
                "battery_critical_threshold_percent",
                "battery_cooldown_minutes",
                "offline_timeout_minutes",
                "offline_cooldown_minutes",
                "smoke_sustain_minutes",
                "smoke_cooldown_minutes",
                "smoke_mute_default_minutes"):
        if key in body and body[key] is not None:
            parsed = _as_finite_float(body[key])
            if parsed is None:
                continue
            if key in minute_keys:
                pcfg[key] = max(1, int(parsed))
            else:
                pcfg[key] = parsed
    if "temp_graph_hours" in body and body["temp_graph_hours"] is not None:
        parsed = _as_finite_float(body["temp_graph_hours"])
        if parsed is not None:
            pcfg["temp_graph_hours"] = max(1, int(parsed))

    # Sensor lists (accept comma-separated string or list)
    for key in ("outdoor_sensors", "exclude_sensors",
                "battery_exclude_devices", "water_exclude_sensors",
                "suppress_maker_devices"):
        if key in body:
            val = body[key]
            if isinstance(val, str):
                val = [s.strip() for s in val.split(",") if s.strip()]
            if not isinstance(val, list):
                val = []
            cleaned = []
            seen = set()
            for item in val:
                token = str(item).strip()
                if not token:
                    continue
                key_norm = token.lower()
                if key_norm in seen:
                    continue
                seen.add(key_norm)
                cleaned.append(token)
            pcfg[key] = cleaned

    # Per-alert push toggles
    for key in ("battery_pushover_enabled",
                "offline_pushover_enabled",
                "water_pushover_enabled",
                "temperature_pushover_enabled",
                "smoke_pushover_enabled",
                "suppress_maker_device_alerts"):
        if key in body and body[key] is not None:
            pcfg[key] = _as_bool(body[key])

    # Primary display sensor — stored in the collector config block.
    # If changed, trigger an immediate background collection so the
    # dashboard reflects the new sensor without waiting for the interval.
    primary_changed = False
    old_primary = ""
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
    _record_system_event(
        event_type="thresholds_updated",
        level="info",
        property_id=pid,
        actor="api",
        message="Alert thresholds updated",
        details={"updated_keys": sorted(body.keys())},
    )
    if primary_changed:
        _schedule_collection_refresh(retries=3, wait_seconds=5)
        _record_system_event(
            event_type="primary_sensor_changed",
            level="info",
            property_id=pid,
            actor="api",
            message="Primary display sensor changed",
            details={"from": old_primary, "to": new_primary},
        )
        logger.info("[%s] primary sensor changed; immediate collection requested (with retry)", pid)

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
    """Clear a latched water alert or acknowledge a shutoff-valve incident."""
    alert = db.get_alert(alert_id)
    if not alert:
        return JSONResponse(status_code=404, content={"error": f"Alert {alert_id} not found"})
    if alert.get("alert_type") == "water_shutoff":
        result = _ack_valve_incident(
            str(alert.get("property_id") or ""),
            str(alert.get("sensor_id") or ""),
            actor="api",
        )
        return JSONResponse(content={
            "status": result.get("status"),
            "alert_id": alert_id,
            "property_id": result.get("property_id"),
            "device_id": result.get("device_id"),
            "cleared_alerts": result.get("cleared_alerts"),
        })
    if alert.get("alert_type") != "water":
        return JSONResponse(status_code=400, content={"error": "Only water alerts can be manually cleared"})

    changed = db.resolve_alert(alert_id)
    if not changed:
        return JSONResponse(content={"status": "already_cleared", "alert_id": alert_id})

    _record_system_event(
        event_type="water_alert_cleared",
        level="info",
        property_id=alert.get("property_id"),
        actor="api",
        message="Water alert cleared manually",
        details={
            "alert_id": alert_id,
            "sensor_id": alert.get("sensor_id"),
        },
    )
    logger.info("Water alert cleared manually: id=%s pid=%s sensor=%s",
                alert_id, alert.get("property_id"), alert.get("sensor_id"))
    return JSONResponse(content={"status": "cleared", "alert_id": alert_id})


@app.post("/api/property/{property_id}/smoke/{sensor_id}/ack")
async def ack_smoke_alarm(property_id: str, sensor_id: str,
                          _auth=Depends(_require_write_auth)):
    """Acknowledge a smoke alarm for this sensor until it returns to clear."""
    known_pids = {p.get("id") for p in CONFIG.get("properties", []) if p.get("id")}
    if property_id not in known_pids:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    sid = str(sensor_id or "").strip()
    if not sid:
        return JSONResponse(status_code=400, content={"error": "sensor_id is required"})

    name = _smoke_sensor_name(property_id, sid)
    db.set_smoke_sensor_ack(property_id, sid, acked_until_clear=True, friendly_name=name)
    cleared = db.resolve_alerts_for_sensor(property_id, "smoke", sid)
    _record_system_event(
        event_type="smoke_alarm_acknowledged",
        level="info",
        property_id=property_id,
        actor="api",
        message="Smoke alarm acknowledged",
        details={
            "sensor_id": sid,
            "friendly_name": name or sid,
            "cleared_alerts": int(cleared),
        },
    )
    logger.info("Smoke alarm acknowledged: pid=%s sensor=%s cleared=%s", property_id, sid, cleared)
    return JSONResponse(content={
        "status": "acknowledged",
        "property_id": property_id,
        "sensor_id": sid,
        "cleared_alerts": int(cleared),
    })


@app.post("/api/property/{property_id}/smoke/{sensor_id}/mute/{minutes}")
async def mute_smoke_alarm(property_id: str, sensor_id: str, minutes: int,
                           _auth=Depends(_require_write_auth)):
    """Mute smoke alarm notifications for one sensor for N minutes."""
    known_pids = {p.get("id") for p in CONFIG.get("properties", []) if p.get("id")}
    if property_id not in known_pids:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    sid = str(sensor_id or "").strip()
    if not sid:
        return JSONResponse(status_code=400, content={"error": "sensor_id is required"})

    mins = max(1, min(int(minutes), 60 * 24 * 14))  # up to 14 days
    mute_until_dt = datetime.now(timezone.utc) + timedelta(minutes=mins)
    mute_until_ts = mute_until_dt.strftime("%Y-%m-%d %H:%M:%S")
    name = _smoke_sensor_name(property_id, sid)
    db.set_smoke_sensor_mute(property_id, sid, muted_until=mute_until_ts, friendly_name=name)
    _record_system_event(
        event_type="smoke_alarm_muted",
        level="info",
        property_id=property_id,
        actor="api",
        message="Smoke alarm muted",
        details={
            "sensor_id": sid,
            "friendly_name": name or sid,
            "minutes": mins,
            "muted_until": mute_until_ts,
        },
    )
    logger.info("Smoke alarm muted: pid=%s sensor=%s minutes=%s", property_id, sid, mins)
    return JSONResponse(content={
        "status": "muted",
        "property_id": property_id,
        "sensor_id": sid,
        "minutes": mins,
        "muted_until": mute_until_dt.isoformat(),
    })


@app.post("/api/property/{property_id}/smoke/{sensor_id}/unmute")
async def unmute_smoke_alarm(property_id: str, sensor_id: str,
                             _auth=Depends(_require_write_auth)):
    """Remove mute window for a smoke sensor."""
    known_pids = {p.get("id") for p in CONFIG.get("properties", []) if p.get("id")}
    if property_id not in known_pids:
        return JSONResponse(status_code=404, content={"error": f"Property '{property_id}' not found"})

    sid = str(sensor_id or "").strip()
    if not sid:
        return JSONResponse(status_code=400, content={"error": "sensor_id is required"})

    name = _smoke_sensor_name(property_id, sid)
    db.set_smoke_sensor_mute(property_id, sid, muted_until=None, friendly_name=name)
    _record_system_event(
        event_type="smoke_alarm_unmuted",
        level="info",
        property_id=property_id,
        actor="api",
        message="Smoke alarm unmuted",
        details={
            "sensor_id": sid,
            "friendly_name": name or sid,
        },
    )
    logger.info("Smoke alarm unmuted: pid=%s sensor=%s", property_id, sid)
    return JSONResponse(content={
        "status": "unmuted",
        "property_id": property_id,
        "sensor_id": sid,
    })


_CLEARABLE_ALERT_CATEGORIES = {"temperature", "battery", "offline", "water", "smoke", "all"}


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

    valve_ack = {"acknowledged": 0, "cleared_alerts": 0}
    if category in {"water", "all"}:
        valve_ack = _ack_active_valve_incidents(pid, actor="api")

    alert_type = None if category == "all" else category
    changed = db.resolve_alerts(property_id=pid, alert_type=alert_type)
    if category in {"water", "all"}:
        changed += int(valve_ack.get("cleared_alerts") or 0)
    _record_system_event(
        event_type="alerts_cleared_by_category",
        level="info",
        property_id=pid,
        actor="api",
        message="Alerts cleared manually by category",
        details={
            "category": category,
            "cleared": changed,
            "water_shutoff_acknowledged": int(valve_ack.get("acknowledged") or 0),
        },
    )
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
    threading.Thread(target=scheduler.collect_all, daemon=True).start()
    _record_system_event(
        event_type="manual_collection_triggered",
        level="info",
        actor="api",
        message="Manual collection run requested",
    )
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
