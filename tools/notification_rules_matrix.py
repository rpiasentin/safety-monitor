#!/usr/bin/env python3
"""
Deterministic alert-rule regression matrix.

Purpose:
- Validate that per-property rule toggles in the UI/config are enforced by
  alert processing logic.
- Run without external APIs, network calls, or real DB writes.

Usage:
  python3 tools/notification_rules_matrix.py
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
import sys
import types
import traceback

_SCRIPT_PATH = globals().get("__file__", "")
if _SCRIPT_PATH and _SCRIPT_PATH != "<stdin>":
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(_SCRIPT_PATH), ".."))
else:
    ROOT_DIR = os.getcwd()
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

try:
    import requests  # noqa: F401
except ModuleNotFoundError:
    requests_stub = types.ModuleType("requests")

    def _post_unavailable(*_args, **_kwargs):
        raise RuntimeError("requests unavailable in test harness")

    requests_stub.post = _post_unavailable
    sys.modules["requests"] = requests_stub

try:
    from dotenv import load_dotenv as _load_dotenv  # noqa: F401
except ModuleNotFoundError:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda *_a, **_k: None
    sys.modules["dotenv"] = dotenv_stub

try:
    import yaml  # noqa: F401
except ModuleNotFoundError:
    yaml_stub = types.ModuleType("yaml")
    yaml_stub.safe_load = lambda *_a, **_k: {}
    sys.modules["yaml"] = yaml_stub

if "db" not in sys.modules:
    db_stub = types.ModuleType("db")

    def _db_unpatched(*_args, **_kwargs):
        raise RuntimeError("db stub called before harness patching")

    for _name in (
        "get_last_alert_time",
        "insert_alert",
        "mark_alert_pushover_sent",
        "find_active_alert",
        "get_latest_reading",
        "get_system_events",
        "get_smoke_sensor_state_map",
        "get_shutoff_valve_state_map",
        "upsert_smoke_sensor_state",
        "upsert_shutoff_valve_state",
        "resolve_alerts_for_sensor",
        "insert_system_event",
    ):
        setattr(db_stub, _name, _db_unpatched)
    sys.modules["db"] = db_stub

if "formatters" not in sys.modules:
    formatters_stub = types.ModuleType("formatters")
    formatters_stub.fmt_temp = lambda f, show_unit=True: (
        f"{float(f):.1f}°F" if show_unit else f"{float(f):.1f}"
    )
    formatters_stub.fmt_pct = lambda p: f"{float(p):.0f}%"
    sys.modules["formatters"] = formatters_stub

import alerts


class FakeDB:
    def __init__(self):
        self._next_id = 1
        self.alerts = []
        self.push_marked_ids = set()
        self.last_alert = {}
        self.latest_reading = {}
        self.active_alert = {}
        self.smoke_state = {}
        self.valve_state = {}
        self.system_events = []

    def get_last_alert_time(self, property_id, alert_type, sensor_id):
        return self.last_alert.get((property_id, alert_type, sensor_id))

    def insert_alert(self, property_id, alert_type, message, sensor_id=None,
                     value=None, threshold=None, severity=None):
        alert_id = self._next_id
        self._next_id += 1
        row = {
            "id": alert_id,
            "property_id": property_id,
            "alert_type": alert_type,
            "sensor_id": sensor_id,
            "value": value,
            "threshold": threshold,
            "severity": severity,
            "message": message,
            "pushover_sent": 0,
        }
        self.alerts.append(row)
        self.last_alert[(property_id, alert_type, sensor_id)] = (
            datetime.now(timezone.utc).isoformat()
        )
        if sensor_id:
            self.active_alert[(property_id, alert_type, sensor_id)] = alert_id
        return alert_id

    def mark_alert_pushover_sent(self, alert_id):
        self.push_marked_ids.add(alert_id)
        for row in self.alerts:
            if row["id"] == alert_id:
                row["pushover_sent"] = 1
                break

    def find_active_alert(self, property_id, alert_type, sensor_id=None):
        if sensor_id is None:
            for (pid, atype, _sid), _aid in self.active_alert.items():
                if pid == property_id and atype == alert_type:
                    return 1
            return None
        return self.active_alert.get((property_id, alert_type, sensor_id))

    def get_latest_reading(self, property_id):
        return self.latest_reading.get(property_id)

    def get_system_events(self, limit=200, level=None, property_id=None, event_type=None):
        rows = list(self.system_events)
        if level:
            rows = [r for r in rows if str(r.get("level") or "").lower() == str(level).lower()]
        if property_id:
            rows = [r for r in rows if r.get("property_id") == property_id]
        if event_type:
            rows = [r for r in rows if str(r.get("event_type") or "").lower() == str(event_type).lower()]
        rows.reverse()
        return rows[:limit]

    def get_smoke_sensor_state_map(self, property_id):
        return self.smoke_state.get(property_id, {})

    def get_shutoff_valve_state_map(self, property_id):
        return self.valve_state.get(property_id, {})

    def upsert_smoke_sensor_state(self, property_id, sensor_id, friendly_name,
                                  last_state, first_alarm_at, last_alarm_at,
                                  acked_until_clear, muted_until):
        state = self.smoke_state.setdefault(property_id, {})
        state[sensor_id] = {
            "friendly_name": friendly_name,
            "last_state": last_state,
            "first_alarm_at": first_alarm_at,
            "last_alarm_at": last_alarm_at,
            "acked_until_clear": int(bool(acked_until_clear)),
            "muted_until": muted_until,
        }

    def upsert_shutoff_valve_state(self, property_id, valve_id, friendly_name,
                                   last_state, last_closed_at, acked_until_open,
                                   expected_closed, trigger_sensor_id,
                                   trigger_sensor_name):
        state = self.valve_state.setdefault(property_id, {})
        state[valve_id] = {
            "friendly_name": friendly_name,
            "last_state": last_state,
            "last_closed_at": last_closed_at,
            "acked_until_open": int(bool(acked_until_open)),
            "expected_closed": int(bool(expected_closed)),
            "trigger_sensor_id": trigger_sensor_id,
            "trigger_sensor_name": trigger_sensor_name,
        }

    def resolve_alerts_for_sensor(self, property_id, alert_type, sensor_id):
        key = (property_id, alert_type, sensor_id)
        if key in self.active_alert:
            del self.active_alert[key]
            return 1
        return 0

    def insert_system_event(self, **kwargs):
        row = dict(kwargs)
        row.setdefault("created_at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
        details = row.pop("details", None)
        if details is not None and "details_json" not in row:
            row["details_json"] = json.dumps(details, sort_keys=True)
        self.system_events.append(row)


class FakePush:
    def __init__(self, should_succeed=True):
        self.calls = []
        self.should_succeed = should_succeed

    def __call__(self, title, message, priority=0):
        self.calls.append({
            "title": title,
            "message": message,
            "priority": priority,
        })
        return self.should_succeed


class AlertHarness:
    def __init__(self):
        self.db = FakeDB()
        self.push = FakePush()
        self._orig = {}

    def patch(self):
        targets = {
            "get_last_alert_time": self.db.get_last_alert_time,
            "insert_alert": self.db.insert_alert,
            "mark_alert_pushover_sent": self.db.mark_alert_pushover_sent,
            "find_active_alert": self.db.find_active_alert,
            "get_latest_reading": self.db.get_latest_reading,
            "get_system_events": self.db.get_system_events,
            "get_smoke_sensor_state_map": self.db.get_smoke_sensor_state_map,
            "get_shutoff_valve_state_map": self.db.get_shutoff_valve_state_map,
            "upsert_smoke_sensor_state": self.db.upsert_smoke_sensor_state,
            "upsert_shutoff_valve_state": self.db.upsert_shutoff_valve_state,
            "resolve_alerts_for_sensor": self.db.resolve_alerts_for_sensor,
            "insert_system_event": self.db.insert_system_event,
        }
        for name, repl in targets.items():
            self._orig[name] = getattr(alerts.db, name)
            setattr(alerts.db, name, repl)
        self._orig["_send_pushover"] = alerts._send_pushover
        alerts._send_pushover = self.push

    def restore(self):
        for name, orig in self._orig.items():
            if name == "_send_pushover":
                alerts._send_pushover = orig
            else:
                setattr(alerts.db, name, orig)


def _processor_cfg():
    return {
        "temperature": {
            "enabled": True,
            "threshold_fahrenheit": 40,
            "critical_fahrenheit": 32,
            "cooldown_minutes": 1,
            "pushover_enabled": True,
        },
        "battery": {
            "enabled": True,
            "low_threshold_percent": 20,
            "critical_threshold_percent": 10,
            "cooldown_minutes": 1,
            "pushover_enabled": True,
        },
        "water": {
            "enabled": True,
            "pushover_enabled": True,
        },
        "smoke": {
            "enabled": True,
            "sustain_minutes": 1,
            "cooldown_minutes": 1,
            "pushover_enabled": True,
            "pushover_priority": 1,
        },
        "offline": {
            "enabled": True,
            "timeout_minutes": 30,
            "cooldown_minutes": 1,
            "pushover_enabled": True,
        },
    }


def _base_snapshot():
    return {
        "property_id": "fm",
        "property_name": "Forget Me Not",
        "all_temps": {},
        "battery_devices": [],
        "water_sensors": [],
        "valve_devices": [],
        "smoke_devices": [],
        "maker_devices": [],
        "maker_temperature_names": [],
        "sources": ["hubitat"],
        "errors": [],
    }


def case_temperature_push_toggle():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["battery"]["enabled"] = False
        cfg["water"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["all_temps"] = {"Main Hall": 30.0}
        prop_cfg = {
            "indoor_temp_warning": 40,
            "indoor_temp_critical": 32,
            "temperature_pushover_enabled": False,
            "temperature_cooldown_minutes": 1,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1 and fired[0]["type"] == "temperature"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["pushover_sent"] == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_battery_push_toggle():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["water"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["battery_devices"] = [{
            "entity_id": "lock.front_door",
            "friendly_name": "Front Door Lock",
            "battery_pct": 5.0,
        }]
        prop_cfg = {
            "battery_low_threshold_percent": 20,
            "battery_critical_threshold_percent": 10,
            "battery_pushover_enabled": False,
            "battery_cooldown_minutes": 1,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1 and fired[0]["type"] == "battery"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["pushover_sent"] == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_water_push_toggle():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["water_sensors"] = [{
            "entity_id": "water.basement_sink",
            "friendly_name": "Basement Sink Leak",
            "state": "wet",
        }]
        prop_cfg = {
            "water_pushover_enabled": False,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1 and fired[0]["type"] == "water"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["pushover_sent"] == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_water_shutoff_push_toggle():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "93",
            "friendly_name": "Main shutoff",
            "state": "closed",
        }]
        prop_cfg = {
            "water_pushover_enabled": False,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1 and fired[0]["type"] == "water_shutoff"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["alert_type"] == "water_shutoff"
        assert harness.db.alerts[0]["pushover_sent"] == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_water_shutoff_inverted_water_on_suppressed():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "39",
            "friendly_name": "Main water cutoff",
            "state": "closed",
        }]
        fired = proc.process(snap, {
            "water_valve_service_on_map": {"39": "closed"},
        })
        assert len(fired) == 0
        assert len(harness.db.alerts) == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_water_shutoff_inverted_water_off_alerts():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "39",
            "friendly_name": "Main water cutoff",
            "state": "open",
        }]
        fired = proc.process(snap, {
            "water_valve_service_on_map": {"39": "closed"},
            "water_pushover_enabled": False,
        })
        assert len(fired) == 1 and fired[0]["type"] == "water_shutoff"
        assert len(harness.db.alerts) == 1
        assert "WATER OFF" in harness.db.alerts[0]["message"]
    finally:
        harness.restore()


def case_water_shutoff_relay_water_on_suppressed():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "82",
            "friendly_name": "High Country water cutoff",
            "state": "on",
        }]
        fired = proc.process(snap, {
            "water_cutoff_service_on_map": {"82": "on"},
        })
        assert len(fired) == 0
        assert len(harness.db.alerts) == 0
    finally:
        harness.restore()


def case_water_shutoff_relay_water_off_alerts():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "82",
            "friendly_name": "High Country water cutoff",
            "state": "off",
        }]
        fired = proc.process(snap, {
            "water_cutoff_service_on_map": {"82": "on"},
            "water_pushover_enabled": False,
        })
        assert len(fired) == 1 and fired[0]["type"] == "water_shutoff"
        assert len(harness.db.alerts) == 1
        assert "WATER OFF" in harness.db.alerts[0]["message"]
    finally:
        harness.restore()


def case_water_shutoff_excluded_device_clears():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "39",
            "friendly_name": "Main water cutoff",
            "state": "open",
        }]
        harness.db.active_alert[("fm", "water_shutoff", "39")] = 77
        harness.db.valve_state["fm"] = {
            "39": {
                "friendly_name": "Main water cutoff",
                "last_state": "open",
                "last_closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "acked_until_open": 0,
                "expected_closed": 0,
                "trigger_sensor_id": None,
                "trigger_sensor_name": None,
            }
        }
        fired = proc.process(snap, {
            "water_valve_exclude_ids": ["39"],
        })
        assert len(fired) == 0
        assert ("fm", "water_shutoff", "39") not in harness.db.active_alert
        assert any(
            str(ev.get("event_type") or "") == "water_incident_resolved"
            and "excluded_device" in str(ev.get("details_json") or "")
            for ev in harness.db.system_events
        )
    finally:
        harness.restore()


def case_water_shutoff_ack_suppresses_realert():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "93",
            "friendly_name": "Main shutoff",
            "state": "closed",
        }]
        harness.db.valve_state["fm"] = {
            "93": {
                "friendly_name": "Main shutoff",
                "last_state": "closed",
                "last_closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "acked_until_open": 1,
                "expected_closed": 0,
                "trigger_sensor_id": "88",
                "trigger_sensor_name": "Laundry room water sensor",
            }
        }
        fired = proc.process(snap, {})
        assert len(fired) == 0
        assert len(harness.db.alerts) == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_water_shutoff_expected_close_suppressed():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "93",
            "friendly_name": "Main shutoff",
            "state": "closed",
        }]
        harness.db.valve_state["fm"] = {
            "93": {
                "friendly_name": "Main shutoff",
                "last_state": "closed",
                "last_closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "acked_until_open": 0,
                "expected_closed": 1,
                "trigger_sensor_id": None,
                "trigger_sensor_name": None,
            }
        }
        fired = proc.process(snap, {})
        assert len(fired) == 0
        assert len(harness.db.alerts) == 0
    finally:
        harness.restore()


def case_water_shutoff_reopen_resolves():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["valve_devices"] = [{
            "entity_id": "93",
            "friendly_name": "Main shutoff",
            "state": "open",
        }]
        harness.db.valve_state["fm"] = {
            "93": {
                "friendly_name": "Main shutoff",
                "last_state": "closed",
                "last_closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "acked_until_open": 0,
                "expected_closed": 0,
                "trigger_sensor_id": "88",
                "trigger_sensor_name": "Laundry room water sensor",
            }
        }
        harness.db.active_alert[("fm", "water_shutoff", "93")] = 99
        fired = proc.process(snap, {})
        assert len(fired) == 0
        assert ("fm", "water_shutoff", "93") not in harness.db.active_alert
        assert any(
            str(ev.get("event_type") or "") == "water_incident_resolved"
            for ev in harness.db.system_events
        )
    finally:
        harness.restore()


def case_offline_push_toggle():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["water"]["enabled"] = False
        cfg["smoke"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["sources"] = []
        snap["errors"] = ["collector timeout"]
        old_ts = datetime.now(timezone.utc) - timedelta(hours=2)
        harness.db.latest_reading["fm"] = {
            "collected_at": old_ts.strftime("%Y-%m-%d %H:%M:%S"),
        }
        prop_cfg = {
            "offline_timeout_minutes": 30,
            "offline_cooldown_minutes": 1,
            "offline_pushover_enabled": False,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1 and fired[0]["type"] == "offline"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["pushover_sent"] == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_smoke_push_toggle():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["battery"]["enabled"] = False
        cfg["water"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["smoke_devices"] = [{
            "entity_id": "smoke.basement",
            "friendly_name": "Basement Smoke",
            "state": "alarm",
            "status": "critical",
        }]
        started = datetime.now(timezone.utc) - timedelta(minutes=5)
        harness.db.smoke_state["fm"] = {
            "smoke.basement": {
                "last_state": "alarm",
                "first_alarm_at": started.strftime("%Y-%m-%d %H:%M:%S"),
                "last_alarm_at": started.strftime("%Y-%m-%d %H:%M:%S"),
                "acked_until_clear": 0,
                "muted_until": None,
            }
        }
        prop_cfg = {
            "smoke_sustain_minutes": 1,
            "smoke_cooldown_minutes": 1,
            "smoke_pushover_enabled": False,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1 and fired[0]["type"] == "smoke"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["pushover_sent"] == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_maker_global_suppression():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["water"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["all_temps"] = {"Basement Door": 10.0}
        snap["maker_temperature_names"] = ["Basement Door"]
        snap["maker_devices"] = [{
            "entity_id": "lock.basement_door",
            "friendly_name": "Basement Door",
        }]
        snap["battery_devices"] = [{
            "entity_id": "lock.basement_door",
            "friendly_name": "Basement Door",
            "battery_pct": 5.0,
        }]
        prop_cfg = {
            "indoor_temp_warning": 40,
            "indoor_temp_critical": 32,
            "battery_low_threshold_percent": 20,
            "battery_critical_threshold_percent": 10,
            "suppress_maker_device_alerts": True,
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 0
        assert len(harness.db.alerts) == 0
        assert len(harness.push.calls) == 0
    finally:
        harness.restore()


def case_maker_per_device_suppression():
    harness = AlertHarness()
    harness.patch()
    try:
        cfg = _processor_cfg()
        cfg["temperature"]["enabled"] = False
        cfg["water"]["enabled"] = False
        cfg["smoke"]["enabled"] = False
        cfg["offline"]["enabled"] = False

        proc = alerts.AlertProcessor(cfg)
        snap = _base_snapshot()
        snap["maker_devices"] = [
            {"entity_id": "lock.basement_door", "friendly_name": "Basement Door"},
            {"entity_id": "lock.front_door", "friendly_name": "Front Door"},
        ]
        snap["battery_devices"] = [
            {
                "entity_id": "lock.basement_door",
                "friendly_name": "Basement Door",
                "battery_pct": 5.0,
            },
            {
                "entity_id": "lock.front_door",
                "friendly_name": "Front Door",
                "battery_pct": 5.0,
            },
        ]
        prop_cfg = {
            "battery_low_threshold_percent": 20,
            "battery_critical_threshold_percent": 10,
            "suppress_maker_device_alerts": False,
            "suppress_maker_devices": ["lock.basement_door"],
        }
        fired = proc.process(snap, prop_cfg)
        assert len(fired) == 1
        assert fired[0]["type"] == "battery"
        assert len(harness.db.alerts) == 1
        assert harness.db.alerts[0]["sensor_id"] == "lock.front_door"
    finally:
        harness.restore()


CASES = [
    ("temperature push toggle honored", case_temperature_push_toggle),
    ("battery push toggle honored", case_battery_push_toggle),
    ("water push toggle honored", case_water_push_toggle),
    ("water shutoff push toggle honored", case_water_shutoff_push_toggle),
    ("water shutoff inverted valve water-on suppressed", case_water_shutoff_inverted_water_on_suppressed),
    ("water shutoff inverted valve water-off alerts", case_water_shutoff_inverted_water_off_alerts),
    ("water shutoff relay water-on suppressed", case_water_shutoff_relay_water_on_suppressed),
    ("water shutoff relay water-off alerts", case_water_shutoff_relay_water_off_alerts),
    ("water shutoff excluded device clears", case_water_shutoff_excluded_device_clears),
    ("water shutoff ack suppresses re-alert", case_water_shutoff_ack_suppresses_realert),
    ("water shutoff expected close suppressed", case_water_shutoff_expected_close_suppressed),
    ("water shutoff reopen resolves", case_water_shutoff_reopen_resolves),
    ("offline push toggle honored", case_offline_push_toggle),
    ("smoke push toggle honored", case_smoke_push_toggle),
    ("maker global suppression honored", case_maker_global_suppression),
    ("maker per-device suppression honored", case_maker_per_device_suppression),
]


def main():
    failures = []
    print("Running notification rules regression matrix...")
    for name, fn in CASES:
        try:
            fn()
            print(f"PASS: {name}")
        except Exception as exc:
            failures.append((name, exc))
            print(f"FAIL: {name}: {exc}")
            traceback.print_exc()

    print("")
    print(f"Total: {len(CASES)}")
    print(f"Passed: {len(CASES) - len(failures)}")
    print(f"Failed: {len(failures)}")

    if failures:
        raise SystemExit(1)

    print("Notification rules regression matrix passed.")


if __name__ == "__main__":
    main()
