"""
DataAggregator — collects from all sources for one property,
writes results to the DB, and returns a unified snapshot dict.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import db
from collectors.eg4 import EG4Client
from collectors.ha_api import HACollector
from collectors.hubitat import HubitatCloudCollector
from collectors.victron import VictronClient

logger = logging.getLogger(__name__)


def _parse_utc_timestamp(raw_ts: str | None) -> datetime | None:
    """Parse mixed timestamp formats and normalize to UTC."""
    if not raw_ts:
        return None
    raw = str(raw_ts).strip()
    if not raw:
        return None

    # SQLite timestamps are stored as "YYYY-MM-DD HH:MM:SS" in UTC.
    try:
        return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except Exception:
        pass

    # Fallback: ISO-8601 variants.
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    except Exception:
        return None


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
        self._last_stale_fallback_event_at: datetime | None = None
        self._stale_fallback_max_minutes = max(
            5,
            int(prop_cfg.get("stale_fallback_max_minutes", 120)),
        )
        self._has_tesla_collector = any(
            (c.get("type") == "ha_api") and bool(c.get("include_tesla", False))
            for c in prop_cfg.get("collectors", [])
        )
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

            # Persist Hubitat device list.
            # Prefer all_devices (includes every device + lastActivity);
            # fall back to battery_devices for backwards compat.
            devices = data.get("all_devices") or data.get("battery_devices", [])
            if devices:
                prune_missing = bool(data.get("all_devices")) and ctype == "hubitat_cloud"
                sync = db.upsert_hubitat_devices(
                    self.prop_id,
                    devices,
                    prune_missing=prune_missing,
                )
                pruned = int(sync.get("pruned", 0))
                if pruned > 0:
                    logger.warning("[%s] pruned %d removed Hubitat device(s)", self.prop_id, pruned)
                    try:
                        db.insert_system_event(
                            event_type="hubitat_device_prune",
                            level="warning",
                            property_id=self.prop_id,
                            actor="collector",
                            message=f"Pruned {pruned} removed Hubitat device(s)",
                            details={
                                "pruned": pruned,
                                "upserted": int(sync.get("upserted", 0)),
                            },
                        )
                    except Exception:
                        logger.debug("[%s] failed to persist prune decision event", self.prop_id, exc_info=True)

        # Build rolled-up fields (canonical field names matching db schema)
        snapshot.update(_rollup(snapshot["sources"]))
        self._apply_stale_tesla_fallback(snapshot)

        # Persist one "merged" row that the dashboard queries — guarantees
        # a single complete row per property rather than one row per source.
        if snapshot["sources"]:
            db.upsert_reading(self.prop_id, "merged", snapshot)

        return snapshot

    def _apply_stale_tesla_fallback(self, snapshot: dict[str, Any]) -> None:
        """
        Keep Tesla/Powerwall dashboard fields populated during short HA outages by
        borrowing only missing fields from the most recent merged snapshot.
        """
        if not self._has_tesla_collector:
            return

        missing_tesla_block = not (isinstance(snapshot.get("tesla"), dict) and snapshot.get("tesla"))
        missing_energy_fields = any(
            snapshot.get(field) is None
            for field in ("soc", "load_power", "battery_charging_power", "grid_power", "grid_online")
        )
        if not (missing_tesla_block or missing_energy_fields):
            return

        prev = db.get_latest_reading(self.prop_id, source="merged")
        if not prev:
            return

        prev_ts = _parse_utc_timestamp(prev.get("collected_at"))
        if not prev_ts:
            return
        age = datetime.now(timezone.utc) - prev_ts
        max_age = timedelta(minutes=self._stale_fallback_max_minutes)
        if age > max_age:
            return

        try:
            prev_raw = json.loads(prev.get("raw_json") or "{}")
        except Exception:
            prev_raw = {}
        if not isinstance(prev_raw, dict):
            return

        fallback_fields = (
            "tesla",
            "soc",
            "pv_total_power",
            "load_power",
            "power_to_user",
            "battery_charging_power",
            "grid_power",
            "grid_online",
        )
        applied_fields: list[str] = []
        for field in fallback_fields:
            prev_val = prev_raw.get(field)
            if prev_val is None:
                continue
            cur_val = snapshot.get(field)
            if field == "tesla":
                if isinstance(cur_val, dict) and cur_val:
                    continue
            elif cur_val is not None:
                continue
            snapshot[field] = prev_val
            applied_fields.append(field)

        if not applied_fields:
            return

        age_minutes = max(1, int(age.total_seconds() // 60))
        snapshot.setdefault("source_warnings", []).append({
            "type": "stale_fallback",
            "source": "ha_api",
            "status": "warning",
            "age_minutes": age_minutes,
            "applied_fields": sorted(applied_fields),
            "message": (
                f"Using recent Home Assistant Tesla values ({age_minutes}m old) "
                "while live HA feed is unavailable."
            ),
        })
        logger.warning(
            "[%s] Applied stale Tesla fallback (%dm old): %s",
            self.prop_id,
            age_minutes,
            ", ".join(sorted(applied_fields)),
        )

        now = datetime.now(timezone.utc)
        if (
            self._last_stale_fallback_event_at is None
            or (now - self._last_stale_fallback_event_at) >= timedelta(minutes=15)
        ):
            try:
                db.insert_system_event(
                    event_type="stale_tesla_fallback_applied",
                    level="warning",
                    property_id=self.prop_id,
                    actor="collector",
                    message=(
                        f"Applied stale Tesla fallback fields while HA feed is unavailable "
                        f"({age_minutes}m old)"
                    ),
                    details={
                        "source": "ha_api",
                        "age_minutes": age_minutes,
                        "fields": sorted(applied_fields),
                        "max_age_minutes": self._stale_fallback_max_minutes,
                    },
                )
                self._last_stale_fallback_event_at = now
            except Exception:
                logger.debug(
                    "[%s] Failed to persist stale Tesla fallback decision event",
                    self.prop_id,
                    exc_info=True,
                )


def _coalesce(*vals):
    """Return first non-None value, treating 0 / 0.0 as valid data (not falsy)."""
    return next((v for v in vals if v is not None), None)


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
    # Use _coalesce (not `or`) so that a valid 0% SOC or 0V isn't skipped.
    out["soc"]            = _coalesce(eg4.get("soc"), vic.get("soc"))
    out["voltage"]        = _coalesce(vic.get("voltage"), eg4.get("voltage"))
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
    # Use explicit None checks so that a legitimately-zero reading (night) isn't skipped.
    _pv_eg4_raw = eg4.get("pv_total_power")
    _pv_vic_raw = vic.get("pv_power")   # system/0/Dc/Pv/Power = 288+289 combined
    if _pv_eg4_raw is not None or _pv_vic_raw is not None:
        out["pv_total_power"] = (_pv_eg4_raw or 0.0) + (_pv_vic_raw or 0.0)
    else:
        out["pv_total_power"] = None

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

    # Water/leak sensors (latched critical alert uses these states)
    out["water_sensors"] = (ha.get("water_sensors") or []) + \
                             (hub.get("water_sensors") or [])

    # Property security/safety rollups (currently from Hubitat cloud feeds)
    out["lock_devices"] = list(hub.get("lock_devices") or [])
    out["smoke_devices"] = list(hub.get("smoke_devices") or [])

    # Tesla (only from ha_api, High Country)
    out["tesla"] = ha.get("tesla")

    # Tesla Energy (Powerwall/solar) fallback — fills canonical fields when
    # no EG4/Victron present, so HC shows the same gauges as FM.
    #
    # Important: do not gate this on solar_power only. Some Tesla entities can
    # be temporarily unavailable while SOC/grid/load are still valid, and we
    # still want the HC dashboard energy section to render.
    tesla = out["tesla"] or {}
    if isinstance(tesla, dict) and tesla:
        def _kw_to_w(kw):
            return round(kw * 1000, 1) if kw is not None else None

        if out["soc"] is None and tesla.get("soc_percent") is not None:
            out["soc"] = tesla.get("soc_percent")
        if out["pv_total_power"] is None and tesla.get("solar_power_kw") is not None:
            out["pv_total_power"] = _kw_to_w(tesla.get("solar_power_kw"))
        if out["load_power"] is None and tesla.get("load_power_kw") is not None:
            w = _kw_to_w(tesla.get("load_power_kw"))
            out["load_power"]    = w
            out["power_to_user"] = w
        if out["battery_charging_power"] is None and tesla.get("battery_power_kw") is not None:
            out["battery_charging_power"] = _kw_to_w(tesla.get("battery_power_kw"))
        # Grid power — Powerwall-only field (negative = exporting, positive = importing)
        if tesla.get("site_power_kw") is not None:
            out["grid_power"] = _kw_to_w(tesla.get("site_power_kw"))
        if tesla.get("grid_online") is not None:
            out["grid_online"] = tesla.get("grid_online")

    return out
