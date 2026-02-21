"""
Alert processing and Pushover notifications.

Checks:
  - Battery SOC below threshold (EG4 or Hubitat devices)
  - Collector offline (no data for > timeout_minutes)
  - Temperature below threshold (°F)

Pushover env vars:
  PUSHOVER_USER_KEY
  PUSHOVER_API_TOKEN
"""

import logging
import os
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv

import db
import formatters

load_dotenv()
logger = logging.getLogger(__name__)

PUSHOVER_USER  = os.getenv("PUSHOVER_USER_KEY", "")
PUSHOVER_TOKEN = os.getenv("PUSHOVER_API_TOKEN", "")
PUSHOVER_URL   = "https://api.pushover.net/1/messages.json"


def _send_pushover(title: str, message: str, priority: int = 0) -> bool:
    """
    Send a Pushover notification.
    priority: -1=quiet, 0=normal, 1=high, 2=emergency (requires retry/expire)
    """
    if not PUSHOVER_USER or not PUSHOVER_TOKEN:
        logger.warning("Pushover not configured — skipping notification: %s", title)
        return False
    try:
        payload = {
            "token":   PUSHOVER_TOKEN,
            "user":    PUSHOVER_USER,
            "title":   title,
            "message": message,
            "priority": priority,
        }
        if priority == 2:
            payload.update({"retry": 60, "expire": 3600})
        resp = requests.post(PUSHOVER_URL, data=payload, timeout=10)
        resp.raise_for_status()
        logger.info("Pushover sent: %s", title)
        return True
    except Exception as exc:
        logger.error("Pushover failed: %s", exc)
        return False


def _cooldown_ok(property_id: str, alert_type: str, sensor_id: str | None,
                  cooldown_minutes: int) -> bool:
    """Return True if enough time has passed since the last identical alert."""
    last = db.get_last_alert_time(property_id, alert_type, sensor_id)
    if last is None:
        return True
    try:
        last_dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return datetime.now(timezone.utc) - last_dt > timedelta(minutes=cooldown_minutes)
    except Exception:
        return True


class AlertProcessor:
    """Evaluates collected data and fires Pushover notifications when thresholds are crossed."""

    def __init__(self, alert_cfg: dict):
        self.cfg = alert_cfg

    def process(self, snapshot: dict) -> list[dict]:
        """Run all checks against a PropertyCollector snapshot. Returns fired alerts."""
        fired = []
        pid = snapshot.get("property_id", "unknown")

        if self.cfg.get("temperature", {}).get("enabled", True):
            fired += self._check_temps(pid, snapshot)

        if self.cfg.get("battery", {}).get("enabled", True):
            fired += self._check_batteries(pid, snapshot)

        if self.cfg.get("offline", {}).get("enabled", True):
            fired += self._check_offline(pid, snapshot)

        return fired

    # ── Temperature ───────────────────────────────────────────────────────────

    def _check_temps(self, pid: str, snapshot: dict) -> list[dict]:
        cfg = self.cfg.get("temperature", {})
        threshold   = cfg.get("threshold_fahrenheit", 40)
        critical    = cfg.get("critical_fahrenheit", 32)
        cooldown    = cfg.get("cooldown_minutes", 60)
        use_push    = cfg.get("pushover_enabled", True)
        fired = []

        all_temps: dict = snapshot.get("all_temps") or {}
        primary = snapshot.get("primary_temp")
        if primary is not None:
            all_temps["primary"] = primary

        for sensor_id, temp_f in all_temps.items():
            if temp_f >= threshold:
                continue
            if not _cooldown_ok(pid, "temperature", sensor_id, cooldown):
                continue

            severity = "critical" if temp_f < critical else "medium"
            msg = (f"{'🚨 FREEZING' if temp_f < critical else '⚠️ Low temp'} at "
                   f"{snapshot.get('property_name', pid)}: "
                   f"{formatters.fmt_temp(temp_f)} ({sensor_id})")

            alert_id = db.insert_alert(pid, "temperature", msg,
                                        sensor_id=sensor_id,
                                        value=temp_f, threshold=threshold,
                                        severity=severity)
            if use_push:
                priority = 1 if severity == "critical" else 0
                ok = _send_pushover(f"Safety Monitor — {snapshot.get('property_name', pid)}", msg, priority)
                if ok:
                    db.mark_alert_pushover_sent(alert_id)

            fired.append({"type": "temperature", "sensor": sensor_id,
                           "value": temp_f, "severity": severity})
            logger.warning("TEMP ALERT [%s] %s: %.1f°F", pid, sensor_id, temp_f)

        return fired

    # ── Battery devices ───────────────────────────────────────────────────────

    def _check_batteries(self, pid: str, snapshot: dict) -> list[dict]:
        cfg = self.cfg.get("battery", {})
        low_threshold  = cfg.get("low_threshold_percent", 20)
        crit_threshold = cfg.get("critical_threshold_percent", 10)
        cooldown       = cfg.get("cooldown_minutes", 120)
        use_push       = cfg.get("pushover_enabled", True)
        excludes       = [str(x).lower() for x in cfg.get("exclude_devices", [])]
        fired = []

        # Check inverter SOC first
        soc = snapshot.get("soc")
        if soc is not None and soc < low_threshold:
            if _cooldown_ok(pid, "battery", "inverter_soc", cooldown):
                severity = "critical" if soc < crit_threshold else "medium"
                msg = (f"{'🔴 Critical' if severity == 'critical' else '⚠️ Low'} inverter battery SOC "
                       f"at {snapshot.get('property_name', pid)}: {formatters.fmt_pct(soc)}")
                alert_id = db.insert_alert(pid, "battery", msg,
                                            sensor_id="inverter_soc",
                                            value=soc, threshold=low_threshold,
                                            severity=severity)
                if use_push:
                    ok = _send_pushover(f"Safety Monitor — {snapshot.get('property_name', pid)}", msg,
                                        priority=1 if severity == "critical" else 0)
                    if ok:
                        db.mark_alert_pushover_sent(alert_id)
                fired.append({"type": "battery", "sensor": "inverter_soc",
                               "value": soc, "severity": severity})

        # Check Hubitat/HA device batteries
        for device in snapshot.get("battery_devices", []):
            name = device.get("friendly_name", device.get("entity_id", ""))
            pct  = device.get("battery_pct")
            eid  = device.get("entity_id", name)
            if name.lower() in excludes or eid.lower() in excludes:
                continue
            if pct is None or pct >= low_threshold:
                continue
            if not _cooldown_ok(pid, "battery", eid, cooldown):
                continue

            severity = "critical" if pct < crit_threshold else "medium"
            msg = (f"{'🔴 Critical' if severity == 'critical' else '⚠️ Low'} device battery "
                   f"at {snapshot.get('property_name', pid)}: {name} = {formatters.fmt_pct(pct)}")
            alert_id = db.insert_alert(pid, "battery", msg,
                                        sensor_id=eid, value=pct,
                                        threshold=low_threshold, severity=severity)
            if use_push:
                ok = _send_pushover(f"Safety Monitor — {snapshot.get('property_name', pid)}", msg,
                                    priority=0)
                if ok:
                    db.mark_alert_pushover_sent(alert_id)
            fired.append({"type": "battery", "sensor": name,
                           "value": pct, "severity": severity})

        return fired

    # ── Offline ───────────────────────────────────────────────────────────────

    def _check_offline(self, pid: str, snapshot: dict) -> list[dict]:
        cfg      = self.cfg.get("offline", {})
        timeout  = cfg.get("timeout_minutes", 30)
        cooldown = cfg.get("cooldown_minutes", 120)
        use_push = cfg.get("pushover_enabled", True)
        fired = []

        if snapshot.get("errors") and not snapshot.get("sources"):
            if _cooldown_ok(pid, "offline", None, cooldown):
                msg = (f"📡 {snapshot.get('property_name', pid)} is OFFLINE — "
                       f"no data collected. Errors: {'; '.join(snapshot['errors'])}")
                alert_id = db.insert_alert(pid, "offline", msg, severity="high")
                if use_push:
                    ok = _send_pushover(f"Safety Monitor — {snapshot.get('property_name', pid)}", msg, priority=1)
                    if ok:
                        db.mark_alert_pushover_sent(alert_id)
                fired.append({"type": "offline", "severity": "high"})

        return fired
