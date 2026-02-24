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
        return EG4Client()
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

            # Persist individual source row
            db.upsert_reading(self.prop_id, ctype, data)

            # Persist Hubitat device battery list
            devices = data.get("battery_devices", [])
            if devices:
                db.upsert_hubitat_devices(self.prop_id, devices)

        # Build rolled-up fields (canonical field names matching db schema)
        snapshot.update(_rollup(snapshot["sources"]))

        # Persist one "merged" row that the dashboard queries — guarantees
        # a single complete row per property rather than one row per source.
        if snapshot["sources"]:
            db.upsert_reading(self.prop_id, "merged", snapshot)

        return snapshot


def _rollup(sources: dict) -> dict:
    """
    Produce flat merged fields from all source dicts.
    Uses the same field names as the raw collector dicts so that
    db.upsert_reading() can read them without any mapping.
    """
    eg4 = sources.get("eg4") or {}
    vic = sources.get("victron") or {}
    ha  = sources.get("ha_api") or {}
    hub = sources.get("hubitat_cloud") or {}

    out: dict = {}

    # ── Solar / Battery rollup ───────────────────────────────────────────────
    # SOC    — EG4 BMS cloud is authoritative (live from inverter BMS);
    #           Victron SmartShunt is fallback (Coulomb-counting, accurate short-term)
    # Voltage — Victron SmartShunt is primary (direct cell measurement)
    # EG4 banner is NOT used for any live value (static boot-time snapshot)
    out["soc"]            = eg4.get("soc") or vic.get("soc")
    out["voltage"]        = vic.get("voltage") or eg4.get("voltage")
    out["victron_soc"]    = vic.get("soc")          # side-by-side comparison
    out["max_cell_temp"]  = eg4.get("max_cell_temp")

    # Per-MPPT / per-string generation
    # EG4 internal: ppv (total), ppv1 (string 1), ppv2 (string 2)
    out["pv_eg4"]         = eg4.get("pv_total_power")   # EG4 total MPPT (all strings summed)
    out["pv_eg4_1"]       = eg4.get("pv_string_1")      # EG4 string 1 (ppv1)
    out["pv_eg4_2"]       = eg4.get("pv_string_2")      # EG4 string 2 (ppv2)
    out["pv_victron_1"]   = vic.get("pv_charger_288")   # Victron MPPT charger 288
    out["pv_victron_2"]   = vic.get("pv_charger_289")   # Victron MPPT charger 289

    # Total PV = EG4 MPPT + both Victron MPPTs
    pv_eg4 = eg4.get("pv_total_power") or 0.0
    pv_vic = vic.get("pv_power")        or 0.0   # system/0/Dc/Pv/Power = 288+289 combined
    out["pv_total_power"] = (pv_eg4 + pv_vic) if (pv_eg4 or pv_vic) else None

    # Power flows
    # battery_charging_power: net W from SmartShunt (positive = charging, negative = discharging)
    out["battery_charging_power"] = vic.get("power")
    out["current"]                = vic.get("current")   # stored in battery_current DB column
    # power_to_user / load_power: house AC demand from EG4 cloud
    load_w = eg4.get("power_to_user")
    out["power_to_user"] = load_w    # picked up by upsert_reading → load_power DB column
    out["load_power"]    = load_w    # available in raw_json for dashboard template

    # Temperature (prefer HA, fall back to Hubitat cloud)
    out["primary_temp"]  = ha.get("primary_temp") if ha.get("primary_temp") is not None \
                           else hub.get("primary_temp")
    out["all_temps"]     = {**(ha.get("temperatures") or {}),
                             **(hub.get("temperatures") or {})}

    # Device batteries (merged from all sources)
    out["battery_devices"] = (ha.get("battery_devices") or []) + \
                              (hub.get("battery_devices") or [])

    # Tesla (only from ha_api, High Country)
    out["tesla"] = ha.get("tesla")

    return out
