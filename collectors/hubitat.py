"""
Hubitat Cloud API collector — used for properties with Hubitat hubs.

Env vars (per-property, fallback to generic):
  HUBITAT_{PROPERTY_ID}_TOKEN   e.g. HUBITAT_FM_TOKEN, HUBITAT_HC_TOKEN
  HUBITAT_CLOUD_TOKEN           fallback if no per-property token set

Config block:
  type: hubitat_cloud
  endpoint: "https://cloud.hubitat.com/api/<uuid>/apps/<id>/devices/all"
  primary_temp_sensor: "Device Label"   # matches Hubitat device label
"""

import logging
import os

import requests
from dotenv import load_dotenv

from collectors.base import BaseCollector

load_dotenv()
logger = logging.getLogger(__name__)

HUBITAT_TOKEN = os.getenv("HUBITAT_CLOUD_TOKEN", "")
TIMEOUT = 10


class HubitatCloudClient:
    """Thin wrapper around the Hubitat Maker API cloud endpoint."""

    def __init__(self, endpoint: str, api_token: str = HUBITAT_TOKEN):
        self.endpoint  = endpoint
        self.api_token = api_token

    def get_all_devices(self) -> list[dict]:
        resp = requests.get(
            self.endpoint,
            params={"access_token": self.api_token},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()

    def _command_base(self) -> str:
        """Return Maker API base URL for command endpoints."""
        endpoint = self.endpoint.split("?", 1)[0].rstrip("/")
        if "/devices/all" in endpoint:
            return endpoint.split("/devices/all", 1)[0]
        if "/devices/" in endpoint:
            return endpoint.split("/devices/", 1)[0]
        if endpoint.endswith("/devices"):
            return endpoint.rsplit("/devices", 1)[0]
        return endpoint

    @staticmethod
    def _command_names(device: dict) -> set[str]:
        names: set[str] = set()
        for cmd in (device.get("commands") or []):
            if isinstance(cmd, dict):
                name = cmd.get("name")
            else:
                name = cmd
            if name:
                names.add(str(name).strip().lower())
        return names

    def command_device(self, device_id: str, command: str) -> dict:
        """Invoke a Maker API command for a single device (lock/unlock)."""
        cmd = str(command or "").strip().lower()
        if cmd not in {"lock", "unlock"}:
            raise ValueError(f"Unsupported Hubitat command: {command}")
        url = f"{self._command_base()}/devices/{device_id}/{cmd}"
        resp = requests.get(
            url,
            params={"access_token": self.api_token},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        try:
            payload = resp.json()
        except Exception:
            payload = {"raw": resp.text}
        return {
            "device_id": str(device_id),
            "command": cmd,
            "ok": True,
            "response": payload,
        }

    def command_locks(self, command: str, locks: list[dict] | None = None) -> dict:
        """
        Run lock/unlock for all lock devices in the property.
        Returns {attempted, succeeded, failed, results}.
        """
        cmd = str(command or "").strip().lower()
        if cmd not in {"lock", "unlock"}:
            raise ValueError(f"Unsupported Hubitat command: {command}")

        if locks is None:
            locks = self.get_lock_devices()

        results = []
        attempted = 0
        succeeded = 0

        capability_key = f"can_{cmd}"
        for lock in locks:
            if lock.get(capability_key) is False:
                continue
            device_id = str(lock.get("entity_id") or "").strip()
            if not device_id:
                continue

            attempted += 1
            try:
                res = self.command_device(device_id, cmd)
                res["friendly_name"] = lock.get("friendly_name") or device_id
                results.append(res)
                succeeded += 1
            except Exception as exc:
                results.append({
                    "device_id": device_id,
                    "friendly_name": lock.get("friendly_name") or device_id,
                    "command": cmd,
                    "ok": False,
                    "error": str(exc),
                })

        failed = attempted - succeeded
        return {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "results": results,
        }

    @staticmethod
    def _attr_value(attrs, key: str):
        """Extract a value from attributes whether dict or list format."""
        if isinstance(attrs, dict):
            return attrs.get(key)
        # list format: [{"name": "temperature", "currentValue": "50.3"}, ...]
        for attr in attrs:
            if attr.get("name") == key:
                return attr.get("currentValue")
        return None

    def get_temperature_sensors(self, devices: list[dict] | None = None) -> dict[str, float]:
        """Return {device_label: °F}. Hubitat reports in °F by default."""
        if devices is None:
            devices = self.get_all_devices()
        result: dict[str, float] = {}
        for d in devices:
            attrs = d.get("attributes", {})
            val = self._attr_value(attrs, "temperature")
            if val is not None:
                try:
                    result[d.get("label", d.get("name", str(d.get("id"))))] = float(val)
                except (ValueError, TypeError):
                    pass
        return result

    @staticmethod
    def _normalize_ts(raw_ts) -> str | None:
        """
        Normalize a Hubitat lastActivity timestamp to SQLite UTC format
        (YYYY-MM-DD HH:MM:SS).  Hubitat emits formats like:
          "2026-02-27 14:32:10+0000"
          "2026-02-27T14:32:10.123+0000"
          null / missing
        """
        if not raw_ts:
            return None
        from datetime import datetime, timezone
        try:
            s = str(raw_ts).strip()
            # Normalize +0000 → +00:00 so fromisoformat accepts it
            if s.endswith("+0000"):
                s = s[:-5] + "+00:00"
            elif s.endswith("-0000"):
                s = s[:-5] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(raw_ts)

    def get_all_devices_with_activity(self,
                                       devices: list[dict] | None = None) -> list[dict]:
        """
        Return every device from the hub with all available fields:
          entity_id, friendly_name, device_type, battery_pct, last_activity

        battery_pct is None for non-battery devices.  All devices are included
        so the activity view can detect dead sensors regardless of device class.
        """
        if devices is None:
            devices = self.get_all_devices()
        result = []
        for d in devices:
            attrs = d.get("attributes", {})
            # Battery — may be absent for non-battery devices
            batt_raw = self._attr_value(attrs, "battery")
            battery_pct = None
            if batt_raw is not None:
                try:
                    battery_pct = float(batt_raw)
                except (ValueError, TypeError):
                    pass
            result.append({
                "entity_id":     str(d.get("id")),
                "friendly_name": d.get("label") or d.get("name") or f"Device {d.get('id')}",
                "device_type":   d.get("type", ""),
                "battery_pct":   battery_pct,
                "last_activity": self._normalize_ts(
                    d.get("lastActivity")
                    or d.get("last_activity")
                    or d.get("date")
                    or self._attr_value(attrs, "lastActivity")
                    or self._attr_value(attrs, "last_activity")
                    or self._attr_value(attrs, "date")
                ),
            })
        return result

    def get_battery_devices(self, devices: list[dict] | None = None) -> list[dict]:
        """Return list of {entity_id, friendly_name, battery_pct} for battery devices.
        Kept for backwards compatibility — use get_all_devices_with_activity() for
        the full device set including activity timestamps.
        """
        if devices is None:
            devices = self.get_all_devices()
        result = []
        for d in devices:
            attrs = d.get("attributes", {})
            val = self._attr_value(attrs, "battery")
            if val is not None:
                try:
                    result.append({
                        "entity_id":     str(d.get("id")),
                        "friendly_name": d.get("label", d.get("name", "Unknown")),
                        "battery_pct":   float(val),
                        "unit":          "%",
                        "type":          d.get("type", ""),
                    })
                except (ValueError, TypeError):
                    pass
        return result

    @staticmethod
    def _normalize_lock_state(raw_state) -> str:
        if raw_state is None:
            return "unknown"
        s = str(raw_state).strip().lower()
        if s in {"locked", "lock"}:
            return "locked"
        if s in {"unlocked", "unlock"}:
            return "unlocked"
        if s in {"locking"}:
            return "locking"
        if s in {"unlocking"}:
            return "unlocking"
        if s in {"jammed", "unknown", "unavailable"}:
            return s
        return s

    def get_lock_devices(self, devices: list[dict] | None = None) -> list[dict]:
        """Return lock status + command capability for all lock devices."""
        if devices is None:
            devices = self.get_all_devices()

        out = []
        for d in devices:
            attrs = d.get("attributes", {})
            commands = self._command_names(d)
            raw_state = self._attr_value(attrs, "lock")
            dev_type = str(d.get("type", "")).lower()
            label = str(d.get("label") or d.get("name") or "")

            is_lock = (
                raw_state is not None
                or "lock" in dev_type
                or "lock" in label.lower()
                or "lock" in commands
                or "unlock" in commands
            )
            if not is_lock:
                continue

            state = self._normalize_lock_state(raw_state)
            can_lock = ("lock" in commands) or state in {"unlocked", "unlocking"}
            can_unlock = ("unlock" in commands) or state in {"locked", "locking"}

            out.append({
                "entity_id": str(d.get("id")),
                "friendly_name": d.get("label") or d.get("name") or f"Device {d.get('id')}",
                "device_type": d.get("type", ""),
                "state": state,
                "can_lock": bool(can_lock),
                "can_unlock": bool(can_unlock),
                "last_activity": self._normalize_ts(
                    d.get("lastActivity")
                    or d.get("last_activity")
                    or d.get("date")
                    or self._attr_value(attrs, "lastActivity")
                    or self._attr_value(attrs, "last_activity")
                    or self._attr_value(attrs, "date")
                ),
            })

        return out

    @staticmethod
    def _normalize_water_state(raw_state) -> str | None:
        """Map vendor-specific leak states to canonical wet/dry values."""
        if raw_state is None:
            return None
        s = str(raw_state).strip().lower()
        wet_vals = {"wet", "detected", "leak", "leaking", "open", "active", "on", "true", "1"}
        dry_vals = {"dry", "clear", "closed", "inactive", "off", "false", "0", "normal"}
        if s in wet_vals:
            return "wet"
        if s in dry_vals:
            return "dry"
        return s

    @staticmethod
    def _normalize_alarm_state(raw_state) -> str | None:
        """Normalize smoke/CO alarm states to clear/test/alarm/unknown."""
        if raw_state is None:
            return None
        s = str(raw_state).strip().lower()
        clear_vals = {"clear", "normal", "off", "inactive", "idle", "ok", "all clear"}
        test_vals = {"test", "tested", "testing"}
        alarm_vals = {"detected", "smoke", "alarm", "alert", "active", "on", "emergency"}
        unknown_vals = {"unknown", "unavailable", "none", "null"}

        if s in clear_vals:
            return "clear"
        if s in test_vals:
            return "test"
        if s in alarm_vals:
            return "alarm"
        if s in unknown_vals:
            return "unknown"
        return s

    def get_smoke_sensors(self, devices: list[dict] | None = None) -> list[dict]:
        """Return smoke/CO detector states for dashboard property safety panels."""
        if devices is None:
            devices = self.get_all_devices()

        out = []
        for d in devices:
            attrs = d.get("attributes", {})
            smoke_raw = (
                self._attr_value(attrs, "smoke")
                or self._attr_value(attrs, "smokeDetector")
                or self._attr_value(attrs, "smoke_status")
            )
            co_raw = (
                self._attr_value(attrs, "carbonMonoxide")
                or self._attr_value(attrs, "carbon_monoxide")
                or self._attr_value(attrs, "co")
            )

            if smoke_raw is None and co_raw is None:
                continue

            smoke_state = self._normalize_alarm_state(smoke_raw)
            co_state = self._normalize_alarm_state(co_raw)
            states = [s for s in (smoke_state, co_state) if s]

            if "alarm" in states:
                status = "critical"
                state = "alarm"
            elif "test" in states:
                status = "warning"
                state = "test"
            elif states and all(s == "clear" for s in states):
                status = "good"
                state = "clear"
            elif not states:
                status = "unknown"
                state = "unknown"
            else:
                status = "warning"
                state = states[0]

            out.append({
                "entity_id": str(d.get("id")),
                "friendly_name": d.get("label") or d.get("name") or f"Device {d.get('id')}",
                "device_type": d.get("type", ""),
                "state": state,
                "status": status,
                "smoke_state": smoke_state,
                "co_state": co_state,
                "last_activity": self._normalize_ts(
                    d.get("lastActivity")
                    or d.get("last_activity")
                    or d.get("date")
                    or self._attr_value(attrs, "lastActivity")
                    or self._attr_value(attrs, "last_activity")
                    or self._attr_value(attrs, "date")
                ),
            })

        return out

    def get_water_sensors(self, devices: list[dict] | None = None) -> list[dict]:
        """Return leak sensor states from Hubitat attributes when present."""
        if devices is None:
            devices = self.get_all_devices()

        out = []
        for d in devices:
            attrs = d.get("attributes", {})
            raw_state = self._attr_value(attrs, "water")
            if raw_state is None:
                raw_state = self._attr_value(attrs, "leak")
            if raw_state is None:
                continue

            out.append({
                "entity_id":     str(d.get("id")),
                "friendly_name": d.get("label") or d.get("name") or f"Device {d.get('id')}",
                "device_type":   d.get("type", ""),
                "state":         self._normalize_water_state(raw_state),
                "raw_state":     str(raw_state),
                "last_activity": self._normalize_ts(
                    d.get("lastActivity")
                    or d.get("last_activity")
                    or d.get("date")
                    or self._attr_value(attrs, "lastActivity")
                    or self._attr_value(attrs, "last_activity")
                    or self._attr_value(attrs, "date")
                ),
            })

        return out


class HubitatCloudCollector(BaseCollector):
    """Collects Hubitat cloud data for properties without local HA access."""

    def __init__(self, property_id: str, cfg: dict):
        super().__init__(property_id, cfg)
        # Token priority: config block → per-property env → generic env
        token = (
            cfg.get("token")
            or os.getenv(f"HUBITAT_{property_id.upper()}_TOKEN")
            or HUBITAT_TOKEN
        )
        self.client      = HubitatCloudClient(cfg["endpoint"], api_token=token)
        self.temp_sensor = cfg.get("primary_temp_sensor")

    def collect(self) -> dict | None:
        try:
            devices = self.client.get_all_devices()
        except Exception as exc:
            return self._fail(exc)

        temps      = self.client.get_temperature_sensors(devices)
        batts      = self.client.get_battery_devices(devices)
        all_devs   = self.client.get_all_devices_with_activity(devices)
        locks      = self.client.get_lock_devices(devices)
        smokes     = self.client.get_smoke_sensors(devices)
        waters     = self.client.get_water_sensors(devices)

        primary_temp = temps.get(self.temp_sensor) if self.temp_sensor else None
        if primary_temp is None and temps:
            primary_temp = next(iter(temps.values()))

        return self._ok({
            "source":          "hubitat_cloud",
            "property_id":     self.property_id,
            "temperatures":    temps,
            "primary_temp":    primary_temp,
            "battery_devices": batts,
            "lock_devices":    locks,
            "smoke_devices":   smokes,
            "water_sensors":  waters,
            # Full device list with lastActivity — used for device activity view
            "all_devices":     all_devs,
        })
