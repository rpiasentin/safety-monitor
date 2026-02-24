"""
APScheduler-based collection engine.

Jobs:
  collect_all   — runs every N minutes, polls all properties
  daily_summary — runs once daily at configured report_time (Mountain)
"""

import logging
import os
from datetime import datetime

import pytz
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

import alerts as alert_module
import db
from aggregator import PropertyCollector

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
_property_collectors: list[PropertyCollector] = []
_alert_processor: alert_module.AlertProcessor | None = None
_property_alert_cfgs: dict = {}   # pid → per-property alerts override dict


def update_property_alert_cfg(pid: str, new_cfg: dict) -> None:
    """Update in-memory alert config for one property (called after config.yaml save)."""
    _property_alert_cfgs[pid] = new_cfg


def update_primary_temp_sensor(pid: str, sensor_name: str) -> bool:
    """
    Patch the running collector's primary temp sensor live — no restart needed.
    Works by finding the HubitatCloudCollector or HACollector for the property
    and updating its temp_sensor / primary_temp_sensor attribute.
    Returns True if a collector was found and patched.
    """
    for pc in _property_collectors:
        if pc.prop_id != pid:
            continue
        for _ctype, collector in pc.collectors:
            if hasattr(collector, "temp_sensor"):          # HubitatCloudCollector
                collector.temp_sensor = sensor_name
                logger.info("[%s] primary_temp_sensor updated live → %s", pid, sensor_name)
                return True
            if hasattr(collector, "primary_temp_sensor"):  # HACollector
                collector.primary_temp_sensor = sensor_name
                logger.info("[%s] primary_temp_sensor updated live → %s", pid, sensor_name)
                return True
    logger.warning("[%s] update_primary_temp_sensor: no matching collector found", pid)
    return False


def _load_config() -> dict:
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(cfg_path) as f:
        return yaml.safe_load(f)


def collect_all() -> None:
    """Poll every enabled property and run alert checks. Called by APScheduler."""
    logger.info("=== Collection run starting at %s ===",
                datetime.now().strftime("%H:%M:%S"))
    for pc in _property_collectors:
        try:
            snapshot = pc.run()
            pid = snapshot.get("property_id", "?")
            soc = snapshot.get("soc")
            temp = snapshot.get("primary_temp")
            errs = snapshot.get("errors", [])
            logger.info("[%s] soc=%s  temp=%s°F  errors=%d",
                        pid,
                        f"{soc:.1f}%" if soc else "—",
                        f"{temp:.1f}" if temp else "—",
                        len(errs))
            if _alert_processor:
                pcfg = _property_alert_cfgs.get(pid, {})
                _alert_processor.process(snapshot, pcfg)
        except Exception as exc:
            logger.error("Collection run error [%s]: %s", pc.prop_id, exc)
    logger.info("=== Collection run complete ===")


def daily_summary() -> None:
    """Send a Pushover daily summary of all properties."""
    logger.info("Sending daily summary...")
    cfg = _load_config()
    tz  = pytz.timezone(cfg.get("system", {}).get("timezone", "America/Denver"))
    now = datetime.now(tz)

    all_latest = db.get_latest_readings_all()
    lines = [f"📊 Safety Monitor — {now.strftime('%a %b %-d, %Y')}"]
    lines.append("")

    properties = cfg.get("properties", [])
    for prop in properties:
        pid  = prop["id"]
        name = prop.get("name", pid)
        row  = all_latest.get(pid)
        if row:
            soc  = row.get("soc")
            volt = row.get("voltage")
            pv   = row.get("pv_power")
            temp = row.get("primary_temp")
            t_soc = row.get("tesla_soc")
            parts = [f"• {name}"]
            if soc is not None:
                parts.append(f"  Battery: {soc:.0f}%  {volt:.1f}V" if volt else f"  Battery: {soc:.0f}%")
            if pv is not None:
                parts.append(f"  PV: {pv:.0f}W")
            if temp is not None:
                parts.append(f"  Temp: {temp:.1f}°F")
            if t_soc is not None:
                parts.append(f"  Tesla: {t_soc:.0f}%")
            lines.extend(parts)
        else:
            lines.append(f"• {name}: No data")
        lines.append("")

    recent_alerts = db.get_recent_alerts(hours=24)
    if recent_alerts:
        lines.append(f"⚠️  {len(recent_alerts)} alert(s) in last 24h")
    else:
        lines.append("✅ No alerts in last 24h")

    msg = "\n".join(lines)
    alert_module._send_pushover("Safety Monitor — Daily Summary", msg, priority=0)


def start(config: dict) -> None:
    """Initialise collectors, alert processor, and start the scheduler."""
    global _scheduler, _property_collectors, _alert_processor, _property_alert_cfgs

    db.init_db()

    properties = config.get("properties", [])

    # Build property collectors
    _property_collectors = [
        PropertyCollector(p) for p in properties
        if p.get("enabled", True)
    ]
    logger.info("Loaded %d property collectors", len(_property_collectors))

    # Per-property alert overrides (alerts: block under each property)
    _property_alert_cfgs = {
        p["id"]: p.get("alerts", {})
        for p in properties
        if p.get("enabled", True)
    }

    # Alert processor
    _alert_processor = alert_module.AlertProcessor(config.get("alerts", {}))

    # Scheduler
    tz = config.get("system", {}).get("timezone", "America/Denver")
    interval = config.get("system", {}).get("collection_interval_minutes", 15)
    report_time = config.get("system", {}).get("report_time", "08:00")
    rh, rm = (int(x) for x in report_time.split(":"))

    _scheduler = BackgroundScheduler(timezone=tz)
    _scheduler.add_job(collect_all, IntervalTrigger(minutes=interval),
                        id="collect_all", replace_existing=True,
                        misfire_grace_time=120)
    _scheduler.add_job(daily_summary, CronTrigger(hour=rh, minute=rm, timezone=tz),
                        id="daily_summary", replace_existing=True)

    _scheduler.start()
    logger.info("Scheduler started — collecting every %d min, daily summary at %s %s",
                interval, report_time, tz)

    # Run an immediate collection so the dashboard has data on startup
    collect_all()


def stop() -> None:
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")
