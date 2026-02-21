"""
DataAggregator — collects from all sources for one property,
writes results to the DB, and returns a unified snapshot dict.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import db
from collectors.eg4 import EG4Client
from collectors.ha_api import HACollector
from collectors.hubitat import HubitatCloudCollector
from collectors.victron import VictronClient

logger = logging.getLogger(__name__)


def _collector_for(property_id: str, coll_cfg: dict):
    """Instantiate the right collector class from a config block."""
    t = coll_cfg.get("type")
    if t == "eg4":
        return EG4Client()          # reads from .env / config directly
    if t == "victron":
        return VictronClient()
    if t == "ha_api":
        return HACollector(property_id, coll_cfg)
    if t == "hubitat_cloud":
        return HubitatCloudCollector(property_id, coll_cfg)
    logger.warning("Unknown collector type: %s", t)
    return None


class PropertyCollector:
    """Manages all collectors for a single property and merges their output."""

    def __init__(self, prop_cfg: dict):
        self.prop_id   = prop_cfg["id"]
        self.prop_name = prop_cfg.get("name", self.prop_id)
        self.enabled   = prop_cfg.get("enabled", True)
        self.collectors = []
        for coll_cfg in prop_cfg.get("collectors", []):
            c = _collector_for(self.prop_id, coll_cfg)
            if c:
                self.collectors.append((coll_cfg.get("type"), c))

    def run(self) -> dict[str, Any]:
        """Run all collectors, persist to DB, return merged snapshot."""
        if not self.enabled:
            return {"property_id": self.prop_id, "enabled": False}

        snapshot: dict[str, Any] = {
            "property_id":    self.prop_id,
            "property_name":  self.prop_name,
            "collected_at":   datetime.now(timezone.utc).isoformat(),
            "sources":        {},
            "errors":         [],
        }

        for ctype, collector in self.collectors:
            try:
                data = (collector.collect()
                        if hasattr(collector, "collect")
                        else collector.get_status())
            except Exception as exc:
                logger.error("[%s/%s] unhandled error: %s", self.prop_id, ctype, exc)
                snapshot["errors"].append(f"{ctype}: {exc}")
                data = None

            if data is None:
                snapshot["errors"].append(f"{ctype}: returned no data")
                continue

            snapshot["sources"][ctype] = data

            # Persist to DB
            db.upsert_reading(self.prop_id, ctype, data)

            # Persist Hubitat device battery list
            devices = data.get("battery_devices", [])
            if devices:
                db.upsert_hubitat_devices(self.prop_id, devices)

        # Build rolled-up fields for quick dashboard access
        snapshot.update(_rollup(snapshot["sources"]))
        return snapshot


def _rollup(sources: dict) -> dict:
    """
    Produce flat summary fields from the raw source dicts.
    Priority: eg4 for solar SOC/voltage; victron for PV power confirmation;
              ha_api for temps and devices.
    """
    out: dict = {}

    eg4 = sources.get("eg4") or sources.get("EG4Client") or {}
    vic = sources.get("victron") or sources.get("VictronCollector") or {}
    ha  = sources.get("ha_api") or sources.get("HACollector") or {}
    hub = sources.get("hubitat_cloud") or sources.get("HubitatCloudCollector") or {}

    # Solar
    out["soc"]            = eg4.get("soc") or vic.get("soc")
    out["battery_voltage"]= eg4.get("voltage") or vic.get("voltage")
    out["pv_power_w"]     = eg4.get("pv_total_power") or vic.get("pv_power")
    out["inverter_temp"]  = eg4.get("max_cell_temp")
    out["victron_soc"]    = vic.get("soc")

    # Temperature (prefer ha_api, fall back to hubitat_cloud)
    ha_temp  = ha.get("primary_temp")
    hub_temp = hub.get("primary_temp")
    out["primary_temp"]      = ha_temp if ha_temp is not None else hub_temp
    out["all_temps"]         = {**(ha.get("temperatures") or {}),
                                 **(hub.get("temperatures") or {})}

    # Battery devices
    out["battery_devices"] = (ha.get("battery_devices") or []) + \
                              (hub.get("battery_devices") or [])

    # Tesla
    out["tesla"] = ha.get("tesla")

    return out
