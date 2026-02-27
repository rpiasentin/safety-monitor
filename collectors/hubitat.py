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

        primary_temp = temps.get(self.temp_sensor) if self.temp_sensor else None
        if primary_temp is None and temps:
            primary_temp = next(iter(temps.values()))

        return self._ok({
            "source":          "hubitat_cloud",
            "property_id":     self.property_id,
            "temperatures":    temps,
            "primary_temp":    primary_temp,
            "battery_devices": batts,
            # Full device list with lastActivity — used for device activity view
            "all_devices":     all_devs,
        })
