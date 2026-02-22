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

    def get_battery_devices(self, devices: list[dict] | None = None) -> list[dict]:
        """Return list of {entity_id, friendly_name, battery_pct} for battery devices."""
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

        temps   = self.client.get_temperature_sensors(devices)
        batts   = self.client.get_battery_devices(devices)

        primary_temp = temps.get(self.temp_sensor) if self.temp_sensor else None
        if primary_temp is None and temps:
            primary_temp = next(iter(temps.values()))

        return self._ok({
            "source":          "hubitat_cloud",
            "property_id":     self.property_id,
            "temperatures":    temps,
            "primary_temp":    primary_temp,
            "battery_devices": batts,
        })
