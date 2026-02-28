"""
Alert processing and Pushover notifications.

Checks:
  - Battery SOC below threshold (EG4 or Hubitat devices)
  - Water sensors reporting wet/leak state (latched critical until manual clear)
  - Collector offline (no data for > timeout_minutes)
  - Temperature below threshold (°F)

Per-property alert config (under each property in config.yaml):
  alerts:
    exclude_sensors:          # skip these sensor names entirely
      - "rpoffice"
    outdoor_sensors:          # apply outdoor thresholds to these
      - "Deck"
      - "Coop sensor"
    indoor_temp_warning: 40   # override global threshold
    indoor_temp_critical: 32
    outdoor_temp_warning: 15  # default 15°F
    outdoor_temp_critical: 0  # default 0°F

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

INVALID_TEMP_F = 0.0   # readings at exactly 0.0°F are treated as dead/virtual sensors


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

    def process(self, snapshot: dict, property_cfg: dict | None = None) -> list[dict]:
        """
        Run all checks against a PropertyCollector snapshot.
        property_cfg: per-property alert overrides from config.yaml
        Returns list of fired alerts.
        """
        fired = []
        pid = snapshot.get("property_id", "unknown")
        pcfg = property_cfg or {}

        if self.cfg.get("temperature", {}).get("enabled", True):
            fired += self._check_temps(pid, snapshot, pcfg)

        if self.cfg.get("battery", {}).get("enabled", True):
            fired += self._check_batteries(pid, snapshot, pcfg)

        if self.cfg.get("water", {}).get("enabled", True):
            fired += self._check_water_sensors(pid, snapshot, pcfg)

        if self.cfg.get("offline", {}).get("enabled", True):
            fired += self._check_offline(pid, snapshot, pcfg)

        return fired

    # ── Temperature ───────────────────────────────────────────────────────────

    def _check_temps(self, pid: str, snapshot: dict, property_cfg: dict) -> list[dict]:
        cfg = self.cfg.get("temperature", {})
        cooldown = cfg.get("cooldown_minutes", 60)
        use_push = cfg.get("pushover_enabled", True)

        # Per-property overrides, fall back to global
        indoor_warn = property_cfg.get("indoor_temp_warning",
                                        cfg.get("threshold_fahrenheit", 40))
        indoor_crit = property_cfg.get("indoor_temp_critical",
                                        cfg.get("critical_fahrenheit", 32))
        outdoor_warn = property_cfg.get("outdoor_temp_warning", 15)
        outdoor_crit = property_cfg.get("outdoor_temp_critical", 0)

        exclude  = {s.lower() for s in property_cfg.get("exclude_sensors", [])}
        outdoors = {s.lower() for s in property_cfg.get("outdoor_sensors", [])}

        fired = []
        all_temps: dict = dict(snapshot.get("all_temps") or {})
        primary = snapshot.get("primary_temp")
        if primary is not None and not all_temps:
            # Fallback only when no named temperature sensors are present.
            all_temps["primary"] = primary

        for sensor_id, temp_f in all_temps.items():
            # Skip excluded (virtual/dead) sensors
            if sensor_id.lower() in exclude:
                continue

            # Skip obviously invalid readings (e.g. virtual device stuck at 0.0°F)
            if temp_f == INVALID_TEMP_F:
                continue

            is_outdoor = sensor_id.lower() in outdoors
            threshold  = outdoor_warn if is_outdoor else indoor_warn
            critical   = outdoor_crit if is_outdoor else indoor_crit

            if temp_f >= threshold:
                continue
            if not _cooldown_ok(pid, "temperature", sensor_id, cooldown):
                continue

            severity      = "critical" if temp_f < critical else "medium"
            location_type = "outdoor" if is_outdoor else "indoor"
            emoji         = "🚨 FREEZING" if temp_f < critical else "⚠️ Low temp"
            msg = (f"{emoji} ({location_type}) at "
                   f"{snapshot.get('property_name', pid)}: "
                   f"{formatters.fmt_temp(temp_f)} — {sensor_id}")

            alert_id = db.insert_alert(pid, "temperature", msg,
                                        sensor_id=sensor_id,
                                        value=temp_f, threshold=threshold,
                                        severity=severity)
            if use_push:
                priority = 1 if severity == "critical" else 0
                ok = _send_pushover(
                    f"Safety Monitor — {snapshot.get('property_name', pid)}", msg, priority)
                if ok:
                    db.mark_alert_pushover_sent(alert_id)

            fired.append({"type": "temperature", "sensor": sensor_id,
                           "value": temp_f, "severity": severity,
                           "location": location_type})
            logger.warning("TEMP ALERT [%s] %s: %.1f°F (%s)",
                           pid, sensor_id, temp_f, location_type)

        return fired

    # ── Battery devices ───────────────────────────────────────────────────────

    def _check_batteries(self, pid: str, snapshot: dict, property_cfg: dict) -> list[dict]:
        cfg = self.cfg.get("battery", {})
        low_threshold  = property_cfg.get("battery_low_threshold_percent",
                                          cfg.get("low_threshold_percent", 20))
        crit_threshold = property_cfg.get("battery_critical_threshold_percent",
                                          cfg.get("critical_threshold_percent", 10))
        cooldown       = property_cfg.get("battery_cooldown_minutes",
                                          cfg.get("cooldown_minutes", 120))
        use_push       = property_cfg.get("battery_pushover_enabled",
                                          cfg.get("pushover_enabled", True))
        excludes_src   = property_cfg.get("battery_exclude_devices",
                                          cfg.get("exclude_devices", []))
        excludes       = [str(x).lower() for x in (excludes_src or [])]
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

    # ── Water leak sensors (latched) ──────────────────────────────────────────

    def _check_water_sensors(self, pid: str, snapshot: dict,
                              property_cfg: dict) -> list[dict]:
        cfg = self.cfg.get("water", {})
        use_push = property_cfg.get("water_pushover_enabled",
                                    cfg.get("pushover_enabled", True))
        excludes_src = property_cfg.get("water_exclude_sensors",
                                        cfg.get("exclude_sensors", []))
        excludes = {str(x).lower() for x in (excludes_src or [])}
        fired = []

        for sensor in snapshot.get("water_sensors", []):
            state = str(sensor.get("state") or "").strip().lower()
            if state != "wet":
                continue

            sensor_id = str(sensor.get("entity_id") or sensor.get("friendly_name") or "")
            name = sensor.get("friendly_name") or sensor_id or "Unknown sensor"
            if not sensor_id:
                continue
            if sensor_id.lower() in excludes or name.lower() in excludes:
                continue

            # Latching behavior: once wet, remain active until user clears.
            if db.find_active_alert(pid, "water", sensor_id=sensor_id):
                continue

            msg = (f"💧 WATER LEAK at {snapshot.get('property_name', pid)}: "
                   f"{name} reports WET")
            alert_id = db.insert_alert(
                pid,
                "water",
                msg,
                sensor_id=sensor_id,
                value=1.0,
                threshold=1.0,
                severity="critical",
            )
            if use_push:
                ok = _send_pushover(
                    f"Safety Monitor — {snapshot.get('property_name', pid)}",
                    msg,
                    priority=1,
                )
                if ok:
                    db.mark_alert_pushover_sent(alert_id)

            fired.append({
                "type": "water",
                "sensor": sensor_id,
                "severity": "critical",
                "state": "wet",
            })
            logger.error("WATER ALERT [%s] %s: wet", pid, name)

        return fired

    # ── Offline ───────────────────────────────────────────────────────────────

    def _check_offline(self, pid: str, snapshot: dict, property_cfg: dict) -> list[dict]:
        cfg      = self.cfg.get("offline", {})
        timeout  = property_cfg.get("offline_timeout_minutes",
                                    cfg.get("timeout_minutes", 30))
        cooldown = property_cfg.get("offline_cooldown_minutes",
                                    cfg.get("cooldown_minutes", 120))
        use_push = property_cfg.get("offline_pushover_enabled",
                                    cfg.get("pushover_enabled", True))
        fired = []

        # Only proceed if the current collection run got no data at all
        if not (snapshot.get("errors") and not snapshot.get("sources")):
            return fired

        # Respect timeout_minutes: don't alert until the property has been
        # offline for at least this long (grace period for transient failures).
        last_ok = db.get_latest_reading(pid)
        if last_ok:
            try:
                last_ts = last_ok.get("collected_at", "")
                # Handle both old ISO+offset format and new SQLite UTC format
                last_dt = datetime.fromisoformat(last_ts.replace("Z", "+00:00"))
                if not last_dt.tzinfo:
                    last_dt = last_dt.replace(tzinfo=timezone.utc)
                offline_secs = (datetime.now(timezone.utc) - last_dt).total_seconds()
                if offline_secs < timeout * 60:
                    logger.debug(
                        "[%s] offline but within grace period (%.0fs / %dm)",
                        pid, offline_secs, timeout)
                    return fired
            except Exception as exc:
                logger.warning("[%s] could not parse last reading timestamp: %s", pid, exc)

        if _cooldown_ok(pid, "offline", None, cooldown):
            msg = (f"📡 {snapshot.get('property_name', pid)} is OFFLINE — "
                   f"no data collected for >{timeout}m. "
                   f"Errors: {'; '.join(snapshot['errors'])}")
            alert_id = db.insert_alert(pid, "offline", msg, severity="high")
            if use_push:
                ok = _send_pushover(
                    f"Safety Monitor — {snapshot.get('property_name', pid)}", msg, priority=1)
                if ok:
                    db.mark_alert_pushover_sent(alert_id)
            fired.append({"type": "offline", "severity": "high"})

        return fired
