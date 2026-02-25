"""
Home Assistant REST API collector.
Pulls temperature sensors, Hubitat battery devices, and Tesla data
for any property whose Hubitat hub is integrated via the HA HACS addon.

Env vars required:
  HA_LONG_LIVED_TOKEN  — long-lived access token from HA user profile

Config block (from config.yaml):
  type: ha_api
  url: "http://192.168.1.115:8123"   # optional — overrides HA_URL env var
  location_id: fm                    # used to filter entity IDs for temps/batteries
  primary_temp_sensor: "sensor.fm_main_temp"
  include_tesla: true                # optional, defaults false
  include_temps: true                # optional, defaults true
  include_batteries: true            # optional, defaults true
  tesla_type: energy                 # "vehicle" (default) or "energy" (Powerwall/solar)
  tesla_vehicle_prefix: "tesla"      # prefix of HA entity IDs, e.g. "piasentin"

Token resolution order (per-collector):
  1. config block token: (not recommended — use env var instead)
  2. HA_{PROPERTY_ID}_TOKEN env var (e.g. HA_HC_TOKEN)
  3. HA_LONG_LIVED_TOKEN env var (global fallback)
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

    def get_tesla_energy_data(self, prefix: str = "powerwall") -> dict | None:
        """Pull Tesla Powerwall/solar data via HA Tesla Energy integration.
        prefix: entity ID prefix, e.g. "piasentin"
          Expects entities like:
            sensor.{prefix}_charge            — battery SOC %
            sensor.{prefix}_solar_power       — solar generation kW
            sensor.{prefix}_battery_power     — battery kW (+ charging, - discharging)
            sensor.{prefix}_site_power        — grid kW (- exporting)
            sensor.{prefix}_load_power        — home consumption kW
            sensor.{prefix}_backup_reserve    — backup reserve %
            binary_sensor.{prefix}_grid_status — on = grid online
        """
        numeric_map = {
            "soc":            f"sensor.{prefix}_charge",
            "solar_power":    f"sensor.{prefix}_solar_power",
            "battery_power":  f"sensor.{prefix}_battery_power",
            "site_power":     f"sensor.{prefix}_site_power",
            "load_power":     f"sensor.{prefix}_load_power",
            "backup_reserve": f"sensor.{prefix}_backup_reserve",
        }
        out: dict = {}
        for key, eid in numeric_map.items():
            s = self.get_state(eid)
            if s and s.get("state") not in ("unknown", "unavailable", None):
                try:
                    out[key] = float(s["state"])
                except (ValueError, TypeError):
                    pass
        grid_s = self.get_state(f"binary_sensor.{prefix}_grid_status")
        if grid_s and grid_s.get("state") not in ("unknown", "unavailable", None):
            out["grid_online"] = grid_s["state"] == "on"
        if not out:
            return None
        battery_power = out.get("battery_power", 0)
        return {
            "soc_percent":        out.get("soc"),
            "solar_power_kw":     round(out.get("solar_power", 0), 3),
            "battery_power_kw":   round(battery_power, 3),
            "charging":           battery_power > 0.1,
            "discharging":        battery_power < -0.1,
            "site_power_kw":      round(out.get("site_power", 0), 3),
            "load_power_kw":      round(out.get("load_power", 0), 3),
            "backup_reserve_pct": out.get("backup_reserve"),
            "grid_online":        out.get("grid_online", True),
        }

    def get_tesla_data(self, prefix: str = "tesla") -> dict | None:
        """Pull Tesla vehicle data via HA Tesla integration entities.
        prefix: the entity ID prefix, e.g. 'tesla' → 'sensor.tesla_battery_level'
                or 'my_model_y' → 'sensor.my_model_y_battery_level'
        """
        entities = {
            "soc":     f"sensor.{prefix}_battery_level",
            "power":   f"sensor.{prefix}_charging_power",
            "range":   f"sensor.{prefix}_range",
            "charger": f"sensor.{prefix}_charger_power",
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
        # URL: config block > env var
        url   = cfg.get("url") or HA_URL
        # Token: config block > per-property env > global env
        token = (cfg.get("token")
                 or os.getenv(f"HA_{property_id.upper()}_TOKEN")
                 or HA_TOKEN)
        self.client          = HAClient(url=url, token=token)
        self.location_id     = cfg.get("location_id", property_id)
        self.temp_sensor     = cfg.get("primary_temp_sensor")
        self.include_tesla   = cfg.get("include_tesla", False)
        self.include_temps   = cfg.get("include_temps", True)
        self.include_batt    = cfg.get("include_batteries", True)
        self.tesla_prefix    = cfg.get("tesla_vehicle_prefix", "tesla")
        self.tesla_type      = cfg.get("tesla_type", "vehicle")  # "vehicle" or "energy"

    def collect(self) -> dict | None:
        try:
            states = self.client.get_states()
        except Exception as exc:
            return self._fail(exc)

        temps   = self.client.get_temperature_sensors(self.location_id, states) \
                  if self.include_temps else {}
        devices = self.client.get_battery_devices(self.location_id, states) \
                  if self.include_batt else []

        primary_temp = None
        if self.temp_sensor and self.temp_sensor in temps:
            primary_temp = temps[self.temp_sensor]
        elif temps:
            primary_temp = next(iter(temps.values()))

        result = {
            "source":          "ha_api",
            "property_id":     self.property_id,
            "temperatures":    temps,
            "primary_temp":    primary_temp,
            "battery_devices": devices,
        }

        if self.include_tesla:
            if self.tesla_type == "energy":
                result["tesla"] = self.client.get_tesla_energy_data(prefix=self.tesla_prefix)
            else:
                result["tesla"] = self.client.get_tesla_data(prefix=self.tesla_prefix)

        return self._ok(result)
