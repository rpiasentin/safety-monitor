"""
Home Assistant REST API collector.
Pulls temperature sensors, Hubitat battery devices, and Tesla data
for any property whose Hubitat hub is integrated via the HA HACS addon.

Env vars required:
  HA_LONG_LIVED_TOKEN  — long-lived access token from HA user profile

Config block (from config.yaml):
  type: ha_api
  location_id: fm              # used to filter entity IDs
  primary_temp_sensor: "sensor.fm_main_temp"
  include_tesla: true          # optional, only on hc
"""

import logging
import os

import requests
from dotenv import load_dotenv

from collectors.base import BaseCollector

load_dotenv()
logger = logging.getLogger(__name__)

HA_URL   = os.getenv("HA_URL", "http://haos-vm.local:8123")
HA_TOKEN = os.getenv("HA_LONG_LIVED_TOKEN", "")
TIMEOUT  = 10


class HAClient:
    """Thin wrapper around the HA REST API."""

    def __init__(self, url: str = HA_URL, token: str = HA_TOKEN):
        self.url = url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def get_states(self) -> list[dict]:
        resp = requests.get(f"{self.url}/api/states",
                            headers=self.headers, timeout=TIMEOUT)
        resp.raise_for_status()
        return resp.json()

    def get_state(self, entity_id: str) -> dict | None:
        try:
            resp = requests.get(f"{self.url}/api/states/{entity_id}",
                                headers=self.headers, timeout=TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.debug("HA get_state(%s) failed: %s", entity_id, exc)
            return None

    def get_temperature_sensors(self, location_id: str,
                                 states: list[dict] | None = None) -> dict[str, float]:
        """Return {entity_id: °F} for temperature sensors matching location_id."""
        if states is None:
            states = self.get_states()
        result: dict[str, float] = {}
        for s in states:
            eid = s.get("entity_id", "")
            if (location_id in eid
                    and "temperature" in eid.lower()
                    and s.get("state") not in ("unknown", "unavailable", None)):
                try:
                    val = float(s["state"])
                    unit = s.get("attributes", {}).get("unit_of_measurement", "°F")
                    # Normalise to °F
                    if unit in ("°C", "C"):
                        val = val * 9 / 5 + 32
                    result[eid] = round(val, 1)
                except (ValueError, TypeError):
                    pass
        return result

    def get_battery_devices(self, location_id: str,
                              states: list[dict] | None = None) -> list[dict]:
        """Return list of {entity_id, friendly_name, battery_pct} for location."""
        if states is None:
            states = self.get_states()
        result = []
        for s in states:
            eid = s.get("entity_id", "")
            attrs = s.get("attributes", {})
            if (location_id in eid
                    and "battery" in eid.lower()
                    and s.get("state") not in ("unknown", "unavailable", None)):
                try:
                    result.append({
                        "entity_id": eid,
                        "friendly_name": attrs.get("friendly_name", eid),
                        "battery_pct": float(s["state"]),
                        "unit": attrs.get("unit_of_measurement", "%"),
                    })
                except (ValueError, TypeError):
                    pass
        return result

    def get_tesla_data(self) -> dict | None:
        """Pull Tesla vehicle data via HA Tesla integration entities."""
        entities = {
            "soc":     "sensor.tesla_battery_level",
            "power":   "sensor.tesla_charging_power",
            "range":   "sensor.tesla_range",
            "charger": "sensor.tesla_charger_power",
        }
        out: dict = {}
        for key, eid in entities.items():
            s = self.get_state(eid)
            if s and s.get("state") not in ("unknown", "unavailable", None):
                try:
                    out[key] = float(s["state"])
                except (ValueError, TypeError):
                    pass
        if not out:
            return None
        charging_power = out.get("power", 0) or out.get("charger", 0)
        return {
            "soc_percent":      out.get("soc"),
            "charging_power_kw": round(charging_power / 1000, 2) if charging_power else 0,
            "charging":          (charging_power or 0) > 0.1,
            "range_miles":      out.get("range"),
        }


class HACollector(BaseCollector):
    """Collects HA data for one property (temps + battery devices + optional Tesla)."""

    def __init__(self, property_id: str, cfg: dict):
        super().__init__(property_id, cfg)
        self.client = HAClient()
        self.location_id   = cfg.get("location_id", property_id)
        self.temp_sensor   = cfg.get("primary_temp_sensor")
        self.include_tesla = cfg.get("include_tesla", False)

    def collect(self) -> dict | None:
        try:
            states = self.client.get_states()
        except Exception as exc:
            return self._fail(exc)

        temps   = self.client.get_temperature_sensors(self.location_id, states)
        devices = self.client.get_battery_devices(self.location_id, states)

        primary_temp = None
        if self.temp_sensor and self.temp_sensor in temps:
            primary_temp = temps[self.temp_sensor]
        elif temps:
            primary_temp = next(iter(temps.values()))

        result = {
            "source":        "ha_api",
            "property_id":   self.property_id,
            "temperatures":  temps,
            "primary_temp":  primary_temp,
            "battery_devices": devices,
        }

        if self.include_tesla:
            result["tesla"] = self.client.get_tesla_data()

        return self._ok(result)
