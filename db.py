"""
SQLite data access layer.
All reads/writes go through this module.

Schema:
  readings             — one row per collection run per source per property
  hubitat_devices      — latest battery level per device per property
  smoke_sensor_state   — per-sensor smoke alarm lifecycle + mute/ack state
  shutoff_valve_state  — per-valve shutoff incident lifecycle + ack state
  system_events        — critical system decisions and operator actions
  alerts               — triggered alert history with cooldown tracking
"""

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def _get_db_path() -> str:
    import yaml
    cfg_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    try:
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        return cfg.get("system", {}).get("db_path", "data/safety_monitor.db")
    except Exception:
        return "data/safety_monitor.db"


DB_PATH = os.getenv("DB_PATH", _get_db_path())

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id     TEXT    NOT NULL,
    source          TEXT    NOT NULL,
    collected_at    TEXT    NOT NULL,
    soc             REAL,
    voltage         REAL,
    pv_power        REAL,
    temperature     REAL,
    primary_temp    REAL,
    load_power      REAL,
    battery_current REAL,
    tesla_soc       REAL,
    tesla_charging  INTEGER,
    tesla_power_kw  REAL,
    grid_power      REAL,
    raw_json        TEXT
);

CREATE TABLE IF NOT EXISTS hubitat_devices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id     TEXT    NOT NULL,
    entity_id       TEXT    NOT NULL,
    friendly_name   TEXT,
    battery_pct     REAL,
    collected_at    TEXT    NOT NULL,
    UNIQUE(property_id, entity_id) ON CONFLICT REPLACE
);

CREATE TABLE IF NOT EXISTS smoke_sensor_state (
    property_id         TEXT    NOT NULL,
    sensor_id           TEXT    NOT NULL,
    friendly_name       TEXT,
    last_state          TEXT    NOT NULL,
    first_alarm_at      TEXT,
    last_alarm_at       TEXT,
    acked_until_clear   INTEGER DEFAULT 0,
    muted_until         TEXT,
    updated_at          TEXT    NOT NULL,
    PRIMARY KEY(property_id, sensor_id)
);

CREATE TABLE IF NOT EXISTS shutoff_valve_state (
    property_id         TEXT    NOT NULL,
    valve_id            TEXT    NOT NULL,
    friendly_name       TEXT,
    last_state          TEXT    NOT NULL,
    last_closed_at      TEXT,
    acked_until_open    INTEGER DEFAULT 0,
    expected_closed     INTEGER DEFAULT 0,
    trigger_sensor_id   TEXT,
    trigger_sensor_name TEXT,
    updated_at          TEXT    NOT NULL,
    PRIMARY KEY(property_id, valve_id)
);

CREATE TABLE IF NOT EXISTS alerts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    property_id     TEXT    NOT NULL,
    alert_type      TEXT    NOT NULL,
    sensor_id       TEXT,
    value           REAL,
    threshold       REAL,
    severity        TEXT,
    message         TEXT,
    triggered_at    TEXT    NOT NULL,
    pushover_sent   INTEGER DEFAULT 0,
    resolved_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_readings_property_time
    ON readings(property_id, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_property_time
    ON alerts(property_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_smoke_state_updated
    ON smoke_sensor_state(property_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_valve_state_updated
    ON shutoff_valve_state(property_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS system_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      TEXT    NOT NULL,
    level           TEXT    NOT NULL,
    event_type      TEXT    NOT NULL,
    property_id     TEXT,
    actor           TEXT,
    message         TEXT    NOT NULL,
    details_json    TEXT
);

CREATE INDEX IF NOT EXISTS idx_system_events_created
    ON system_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_system_events_type
    ON system_events(event_type, created_at DESC);
"""


def init_db(path: str = DB_PATH) -> None:
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.executescript(SCHEMA)
        # Migrations: add columns introduced after initial schema
        migrations = [
            "ALTER TABLE readings ADD COLUMN grid_power REAL",
            # Device activity view columns (added after initial schema)
            "ALTER TABLE hubitat_devices ADD COLUMN last_activity TEXT",
            "ALTER TABLE hubitat_devices ADD COLUMN device_type TEXT",
        ]
        for sql in migrations:
            try:
                conn.execute(sql)
            except sqlite3.OperationalError:
                pass  # column already exists
    logger.info("Database ready at %s", path)


@contextmanager
def get_conn(path: str = DB_PATH):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _now() -> str:
    # Use SQLite-native UTC format (YYYY-MM-DD HH:MM:SS) so that
    # datetime('now', '-N hours') comparisons in queries work correctly.
    # isoformat() produces "+00:00" suffix which breaks lexicographic
    # comparison against SQLite's datetime() output.
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ── Readings ──────────────────────────────────────────────────────────────────

def upsert_reading(property_id: str, source: str, data: dict,
                   path: str = DB_PATH) -> None:
    """Insert a new reading row from a collector data dict."""
    now = _now()
    raw = json.dumps(data)

    # Solar data (eg4 / victron)
    soc     = data.get("soc")
    voltage = data.get("voltage")
    pv      = data.get("pv_total_power")
    temp    = data.get("max_cell_temp")
    load    = data.get("power_to_user")
    current = data.get("current")
    p_temp  = data.get("primary_temp")

    # Tesla (nested in ha_api result)
    tesla = data.get("tesla") or {}
    t_soc  = tesla.get("soc_percent")
    t_chrg = int(tesla.get("charging", False))
    t_pwr  = tesla.get("charging_power_kw")

    # Grid power (Powerwall/energy systems — from _rollup canonical field)
    grid_pwr = data.get("grid_power")

    with get_conn(path) as conn:
        conn.execute("""
            INSERT INTO readings
              (property_id, source, collected_at,
               soc, voltage, pv_power, temperature, primary_temp,
               load_power, battery_current,
               tesla_soc, tesla_charging, tesla_power_kw,
               grid_power, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (property_id, source, now,
              soc, voltage, pv, temp, p_temp,
              load, current,
              t_soc, t_chrg, t_pwr,
              grid_pwr, raw))


def get_latest_reading(property_id: str, source: str | None = None,
                        path: str = DB_PATH) -> dict | None:
    """Return most recent reading for a property (optionally filtered by source)."""
    with get_conn(path) as conn:
        if source:
            row = conn.execute("""
                SELECT * FROM readings
                WHERE property_id=? AND source=?
                ORDER BY id DESC LIMIT 1
            """, (property_id, source)).fetchone()
        else:
            row = conn.execute("""
                SELECT * FROM readings
                WHERE property_id=?
                ORDER BY id DESC LIMIT 1
            """, (property_id,)).fetchone()
    return dict(row) if row else None


def get_latest_readings_all(path: str = DB_PATH) -> dict[str, dict]:
    """Return most recent reading per property, keyed by property_id."""
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT r.*
            FROM readings r
            INNER JOIN (
                SELECT property_id, MAX(id) as max_id
                FROM readings GROUP BY property_id
            ) latest ON r.property_id=latest.property_id
                         AND r.id=latest.max_id
        """).fetchall()
    return {dict(r)["property_id"]: dict(r) for r in rows}


def get_readings_history(property_id: str, hours: int = 24,
                          path: str = DB_PATH) -> list[dict]:
    """Return all readings for a property within the last N hours."""
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT * FROM readings
            WHERE property_id=?
              AND collected_at >= datetime('now', ?)
            ORDER BY collected_at ASC
        """, (property_id, f"-{hours} hours")).fetchall()
    return [dict(r) for r in rows]


def get_temperature_history(property_id: str, sensor_name: str, hours: int = 24,
                            path: str = DB_PATH) -> list[dict]:
    """
    Return a single sensor's temperature trend from merged raw_json all_temps.
    Output rows: {collected_at, temperature_f}
    """
    if not sensor_name:
        return []
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT collected_at, raw_json
            FROM readings
            WHERE property_id=?
              AND source='merged'
              AND collected_at >= datetime('now', ?)
            ORDER BY collected_at ASC
        """, (property_id, f"-{hours} hours")).fetchall()

    out = []
    for r in rows:
        try:
            raw = json.loads(r["raw_json"] or "{}")
            all_temps = raw.get("all_temps") or {}
            if sensor_name not in all_temps:
                continue
            temp_val = float(all_temps[sensor_name])
            out.append({
                "collected_at": r["collected_at"],
                "temperature_f": temp_val,
            })
        except Exception:
            continue
    return out


# ── Hubitat devices ───────────────────────────────────────────────────────────

def upsert_hubitat_devices(property_id: str, devices: list[dict],
                            prune_missing: bool = True,
                            path: str = DB_PATH) -> dict[str, int]:
    """
    Upsert latest Hubitat device snapshot for one property.

    Also prunes rows for devices that disappeared upstream so removed devices
    no longer appear in dashboard/device activity views.

    Safety behavior: when the upstream payload is empty/unusable, no prune is
    performed to avoid accidental mass deletion during transient API failures.
    """
    now = _now()
    seen_ids: set[str] = set()
    upserted = 0
    pruned = 0

    with get_conn(path) as conn:
        for d in (devices or []):
            entity_id = str(d.get("entity_id", "")).strip()
            if not entity_id or entity_id in seen_ids:
                continue
            seen_ids.add(entity_id)
            conn.execute("""
                INSERT OR REPLACE INTO hubitat_devices
                  (property_id, entity_id, friendly_name, battery_pct,
                   last_activity, device_type, collected_at)
                VALUES (?,?,?,?,?,?,?)
            """, (property_id,
                  entity_id,
                  d.get("friendly_name", ""),
                  d.get("battery_pct"),
                  d.get("last_activity"),
                  d.get("device_type") or d.get("type", ""),
                  now))
            upserted += 1

        if prune_missing and seen_ids:
            placeholders = ",".join("?" for _ in seen_ids)
            sql = f"""DELETE FROM hubitat_devices
                     WHERE property_id=?
                       AND entity_id NOT IN ({placeholders})"""
            cur = conn.execute(sql, (property_id, *sorted(seen_ids)))
            pruned = int(cur.rowcount or 0)

    return {"upserted": upserted, "pruned": pruned}


def get_hubitat_devices(property_id: str | None = None,
                         path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        if property_id:
            rows = conn.execute("""
                SELECT * FROM hubitat_devices WHERE property_id=?
                ORDER BY battery_pct ASC
            """, (property_id,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM hubitat_devices ORDER BY property_id, battery_pct ASC
            """).fetchall()
    return [dict(r) for r in rows]


def get_hubitat_devices_activity(property_id: str,
                                  path: str = DB_PATH) -> list[dict]:
    """
    Return all Hubitat devices for a property sorted for the activity view:
    NULL last_activity first (never reported), then oldest activity first
    (most stale at the top), most-recently-active at the bottom.
    """
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT * FROM hubitat_devices
            WHERE property_id=?
            ORDER BY
                CASE WHEN COALESCE(last_activity, collected_at) IS NULL THEN 0 ELSE 1 END ASC,
                COALESCE(last_activity, collected_at) ASC
        """, (property_id,)).fetchall()
    return [dict(r) for r in rows]


# ── Smoke sensor state ────────────────────────────────────────────────────────

def get_smoke_sensor_state(property_id: str,
                           sensor_id: str,
                           path: str = DB_PATH) -> dict | None:
    with get_conn(path) as conn:
        row = conn.execute("""
            SELECT * FROM smoke_sensor_state
            WHERE property_id=? AND sensor_id=?
            LIMIT 1
        """, (property_id, sensor_id)).fetchone()
    return dict(row) if row else None


def get_smoke_sensor_states(property_id: str,
                            path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT * FROM smoke_sensor_state
            WHERE property_id=?
            ORDER BY updated_at DESC, sensor_id ASC
        """, (property_id,)).fetchall()
    return [dict(r) for r in rows]


def get_smoke_sensor_state_map(property_id: str,
                               path: str = DB_PATH) -> dict[str, dict]:
    rows = get_smoke_sensor_states(property_id, path=path)
    out: dict[str, dict] = {}
    for row in rows:
        sid = str(row.get("sensor_id") or "").strip()
        if sid:
            out[sid] = row
    return out


def upsert_smoke_sensor_state(property_id: str,
                              sensor_id: str,
                              friendly_name: str = "",
                              last_state: str = "unknown",
                              first_alarm_at: str | None = None,
                              last_alarm_at: str | None = None,
                              acked_until_clear: bool = False,
                              muted_until: str | None = None,
                              path: str = DB_PATH) -> None:
    now = _now()
    with get_conn(path) as conn:
        conn.execute("""
            INSERT INTO smoke_sensor_state
              (property_id, sensor_id, friendly_name, last_state,
               first_alarm_at, last_alarm_at, acked_until_clear,
               muted_until, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(property_id, sensor_id) DO UPDATE SET
              friendly_name=excluded.friendly_name,
              last_state=excluded.last_state,
              first_alarm_at=excluded.first_alarm_at,
              last_alarm_at=excluded.last_alarm_at,
              acked_until_clear=excluded.acked_until_clear,
              muted_until=excluded.muted_until,
              updated_at=excluded.updated_at
        """, (
            property_id,
            sensor_id,
            (friendly_name or ""),
            str(last_state or "unknown").strip().lower(),
            first_alarm_at,
            last_alarm_at,
            1 if acked_until_clear else 0,
            muted_until,
            now,
        ))


def set_smoke_sensor_ack(property_id: str,
                         sensor_id: str,
                         acked_until_clear: bool = True,
                         friendly_name: str = "",
                         path: str = DB_PATH) -> None:
    row = get_smoke_sensor_state(property_id, sensor_id, path=path)
    now = _now()
    if row is None:
        upsert_smoke_sensor_state(
            property_id=property_id,
            sensor_id=sensor_id,
            friendly_name=friendly_name or sensor_id,
            last_state="unknown",
            first_alarm_at=None,
            last_alarm_at=None,
            acked_until_clear=acked_until_clear,
            muted_until=None,
            path=path,
        )
        return
    with get_conn(path) as conn:
        conn.execute("""
            UPDATE smoke_sensor_state
            SET acked_until_clear=?, updated_at=?, friendly_name=?
            WHERE property_id=? AND sensor_id=?
        """, (
            1 if acked_until_clear else 0,
            now,
            friendly_name or row.get("friendly_name") or sensor_id,
            property_id,
            sensor_id,
        ))


def set_smoke_sensor_mute(property_id: str,
                          sensor_id: str,
                          muted_until: str | None,
                          friendly_name: str = "",
                          path: str = DB_PATH) -> None:
    row = get_smoke_sensor_state(property_id, sensor_id, path=path)
    now = _now()
    if row is None:
        upsert_smoke_sensor_state(
            property_id=property_id,
            sensor_id=sensor_id,
            friendly_name=friendly_name or sensor_id,
            last_state="unknown",
            first_alarm_at=None,
            last_alarm_at=None,
            acked_until_clear=False,
            muted_until=muted_until,
            path=path,
        )
        return
    with get_conn(path) as conn:
        conn.execute("""
            UPDATE smoke_sensor_state
            SET muted_until=?, updated_at=?, friendly_name=?
            WHERE property_id=? AND sensor_id=?
        """, (
            muted_until,
            now,
            friendly_name or row.get("friendly_name") or sensor_id,
            property_id,
            sensor_id,
        ))


# ── Shutoff valve state ────────────────────────────────────────────────────────

def get_shutoff_valve_state(property_id: str,
                            valve_id: str,
                            path: str = DB_PATH) -> dict | None:
    with get_conn(path) as conn:
        row = conn.execute("""
            SELECT * FROM shutoff_valve_state
            WHERE property_id=? AND valve_id=?
            LIMIT 1
        """, (property_id, valve_id)).fetchone()
    return dict(row) if row else None


def get_shutoff_valve_states(property_id: str,
                             path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT * FROM shutoff_valve_state
            WHERE property_id=?
            ORDER BY updated_at DESC, valve_id ASC
        """, (property_id,)).fetchall()
    return [dict(r) for r in rows]


def get_shutoff_valve_state_map(property_id: str,
                                path: str = DB_PATH) -> dict[str, dict]:
    rows = get_shutoff_valve_states(property_id, path=path)
    out: dict[str, dict] = {}
    for row in rows:
        valve_id = str(row.get("valve_id") or "").strip()
        if valve_id:
            out[valve_id] = row
    return out


def upsert_shutoff_valve_state(property_id: str,
                               valve_id: str,
                               friendly_name: str = "",
                               last_state: str = "unknown",
                               last_closed_at: str | None = None,
                               acked_until_open: bool = False,
                               expected_closed: bool = False,
                               trigger_sensor_id: str | None = None,
                               trigger_sensor_name: str | None = None,
                               path: str = DB_PATH) -> None:
    now = _now()
    with get_conn(path) as conn:
        conn.execute("""
            INSERT INTO shutoff_valve_state
              (property_id, valve_id, friendly_name, last_state, last_closed_at,
               acked_until_open, expected_closed, trigger_sensor_id,
               trigger_sensor_name, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(property_id, valve_id) DO UPDATE SET
              friendly_name=excluded.friendly_name,
              last_state=excluded.last_state,
              last_closed_at=excluded.last_closed_at,
              acked_until_open=excluded.acked_until_open,
              expected_closed=excluded.expected_closed,
              trigger_sensor_id=excluded.trigger_sensor_id,
              trigger_sensor_name=excluded.trigger_sensor_name,
              updated_at=excluded.updated_at
        """, (
            property_id,
            valve_id,
            friendly_name or valve_id,
            str(last_state or "unknown").strip().lower(),
            last_closed_at,
            1 if acked_until_open else 0,
            1 if expected_closed else 0,
            trigger_sensor_id,
            trigger_sensor_name,
            now,
        ))


def set_shutoff_valve_ack(property_id: str,
                          valve_id: str,
                          acked_until_open: bool = True,
                          friendly_name: str = "",
                          path: str = DB_PATH) -> None:
    row = get_shutoff_valve_state(property_id, valve_id, path=path) or {}
    upsert_shutoff_valve_state(
        property_id=property_id,
        valve_id=valve_id,
        friendly_name=friendly_name or row.get("friendly_name") or valve_id,
        last_state=row.get("last_state") or "unknown",
        last_closed_at=row.get("last_closed_at"),
        acked_until_open=acked_until_open,
        expected_closed=bool(int(row.get("expected_closed") or 0)),
        trigger_sensor_id=row.get("trigger_sensor_id"),
        trigger_sensor_name=row.get("trigger_sensor_name"),
        path=path,
    )


def set_shutoff_valve_expected_closed(property_id: str,
                                      valve_id: str,
                                      expected_closed: bool = True,
                                      friendly_name: str = "",
                                      path: str = DB_PATH) -> None:
    row = get_shutoff_valve_state(property_id, valve_id, path=path) or {}
    upsert_shutoff_valve_state(
        property_id=property_id,
        valve_id=valve_id,
        friendly_name=friendly_name or row.get("friendly_name") or valve_id,
        last_state=row.get("last_state") or "unknown",
        last_closed_at=row.get("last_closed_at"),
        acked_until_open=bool(int(row.get("acked_until_open") or 0)),
        expected_closed=expected_closed,
        trigger_sensor_id=row.get("trigger_sensor_id"),
        trigger_sensor_name=row.get("trigger_sensor_name"),
        path=path,
    )


# ── Alerts ────────────────────────────────────────────────────────────────────

def insert_alert(property_id: str, alert_type: str, message: str,
                  sensor_id: str = None, value: float = None,
                  threshold: float = None, severity: str = "medium",
                  path: str = DB_PATH) -> int:
    now = _now()
    with get_conn(path) as conn:
        cur = conn.execute("""
            INSERT INTO alerts
              (property_id, alert_type, sensor_id, value, threshold,
               severity, message, triggered_at)
            VALUES (?,?,?,?,?,?,?,?)
        """, (property_id, alert_type, sensor_id, value,
              threshold, severity, message, now))
        return cur.lastrowid


def mark_alert_pushover_sent(alert_id: int, path: str = DB_PATH) -> None:
    with get_conn(path) as conn:
        conn.execute("UPDATE alerts SET pushover_sent=1 WHERE id=?", (alert_id,))


def get_last_alert_time(property_id: str, alert_type: str,
                         sensor_id: str = None, path: str = DB_PATH) -> str | None:
    """Return ISO timestamp of most recent matching alert, for cooldown checks."""
    with get_conn(path) as conn:
        if sensor_id:
            row = conn.execute("""
                SELECT triggered_at FROM alerts
                WHERE property_id=? AND alert_type=? AND sensor_id=?
                ORDER BY id DESC LIMIT 1
            """, (property_id, alert_type, sensor_id)).fetchone()
        else:
            row = conn.execute("""
                SELECT triggered_at FROM alerts
                WHERE property_id=? AND alert_type=?
                ORDER BY id DESC LIMIT 1
            """, (property_id, alert_type)).fetchone()
    return row[0] if row else None


def get_recent_alerts(hours: int = 48, path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT * FROM alerts
            WHERE triggered_at >= datetime('now', ?)
            ORDER BY id DESC
        """, (f"-{hours} hours",)).fetchall()
    return [dict(r) for r in rows]


def get_active_alerts(alert_type: str | None = None,
                      path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        if alert_type:
            rows = conn.execute("""
                SELECT * FROM alerts
                WHERE resolved_at IS NULL AND alert_type=?
                ORDER BY id DESC
            """, (alert_type,)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM alerts
                WHERE resolved_at IS NULL
                ORDER BY id DESC
            """).fetchall()
    return [dict(r) for r in rows]


def get_dashboard_alerts(hours: int = 24,
                         recent_limit: int = 20,
                         path: str = DB_PATH) -> list[dict]:
    """
    Alerts shown on the dashboard:
      1) all unresolved water + shutoff + smoke alerts (latched until clear/ack)
      2) recent non-water/non-shutoff/non-smoke alerts in the time window, collapsed so repeated
         triggers for the same property/type/sensor appear as one row with
         repeat_count.
    """
    with get_conn(path) as conn:
        latched_rows = conn.execute("""
            SELECT * FROM alerts
            WHERE alert_type IN ('water', 'water_shutoff', 'smoke')
              AND resolved_at IS NULL
            ORDER BY id DESC
        """).fetchall()
        recent_rows = conn.execute("""
            SELECT * FROM alerts
            WHERE alert_type NOT IN ('water', 'water_shutoff', 'smoke')
              AND resolved_at IS NULL
              AND triggered_at >= datetime('now', ?)
            ORDER BY id DESC
        """, (f"-{hours} hours",)).fetchall()

    # Keep all active latched alerts (water/smoke) as individual rows.
    latched_out = []
    for r in latched_rows:
        d = dict(r)
        d["repeat_count"] = 1
        latched_out.append(d)

    # Collapse repeated non-water alerts by property/type/sensor.
    grouped: dict[tuple[str, str, str], dict] = {}
    for r in recent_rows:
        d = dict(r)
        key = (
            str(d.get("property_id") or ""),
            str(d.get("alert_type") or ""),
            str(d.get("sensor_id") or ""),
        )
        if key not in grouped:
            d["repeat_count"] = 1
            grouped[key] = d
        else:
            grouped[key]["repeat_count"] = int(grouped[key].get("repeat_count", 1)) + 1

    recent_out = sorted(
        grouped.values(),
        key=lambda row: int(row.get("id") or 0),
        reverse=True,
    )
    if int(recent_limit) > 0:
        recent_out = recent_out[:int(recent_limit)]

    out = latched_out + recent_out
    out.sort(key=lambda r: int(r.get("id") or 0), reverse=True)
    return out


def get_alert(alert_id: int, path: str = DB_PATH) -> dict | None:
    with get_conn(path) as conn:
        row = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    return dict(row) if row else None


def resolve_alert(alert_id: int, path: str = DB_PATH) -> bool:
    with get_conn(path) as conn:
        cur = conn.execute("""
            UPDATE alerts
            SET resolved_at=?
            WHERE id=? AND resolved_at IS NULL
        """, (_now(), alert_id))
        return cur.rowcount > 0


def resolve_alerts(property_id: str,
                   alert_type: str | None = None,
                   path: str = DB_PATH) -> int:
    """Resolve all unresolved alerts for a property, optionally by category."""
    now = _now()
    with get_conn(path) as conn:
        if alert_type:
            cur = conn.execute("""
                UPDATE alerts
                SET resolved_at=?
                WHERE property_id=?
                  AND alert_type=?
                  AND resolved_at IS NULL
            """, (now, property_id, alert_type))
        else:
            cur = conn.execute("""
                UPDATE alerts
                SET resolved_at=?
                WHERE property_id=?
                  AND resolved_at IS NULL
            """, (now, property_id))
    return int(cur.rowcount or 0)


def resolve_alerts_for_sensor(property_id: str,
                              alert_type: str,
                              sensor_id: str,
                              path: str = DB_PATH) -> int:
    """Resolve all unresolved alerts for a property/type/sensor."""
    now = _now()
    with get_conn(path) as conn:
        cur = conn.execute("""
            UPDATE alerts
            SET resolved_at=?
            WHERE property_id=?
              AND alert_type=?
              AND sensor_id=?
              AND resolved_at IS NULL
        """, (now, property_id, alert_type, sensor_id))
    return int(cur.rowcount or 0)


def find_active_alert(property_id: str,
                      alert_type: str,
                      sensor_id: str | None = None,
                      path: str = DB_PATH) -> dict | None:
    with get_conn(path) as conn:
        if sensor_id is None:
            row = conn.execute("""
                SELECT * FROM alerts
                WHERE property_id=?
                  AND alert_type=?
                  AND resolved_at IS NULL
                ORDER BY id DESC
                LIMIT 1
            """, (property_id, alert_type)).fetchone()
        else:
            row = conn.execute("""
                SELECT * FROM alerts
                WHERE property_id=?
                  AND alert_type=?
                  AND sensor_id=?
                  AND resolved_at IS NULL
                ORDER BY id DESC
                LIMIT 1
            """, (property_id, alert_type, sensor_id)).fetchone()
    return dict(row) if row else None


# ── System events / decision log ──────────────────────────────────────────────

def insert_system_event(event_type: str,
                        message: str,
                        level: str = "info",
                        property_id: str | None = None,
                        actor: str | None = "system",
                        details: dict | None = None,
                        path: str = DB_PATH) -> int:
    """Persist a critical system decision / operator action event."""
    details_json = json.dumps(details, ensure_ascii=True, sort_keys=True) if details is not None else None
    with get_conn(path) as conn:
        cur = conn.execute("""
            INSERT INTO system_events
              (created_at, level, event_type, property_id, actor, message, details_json)
            VALUES (?,?,?,?,?,?,?)
        """, (_now(),
              str(level or "info").strip().lower(),
              str(event_type or "unknown").strip().lower(),
              property_id,
              actor,
              message,
              details_json))
        return int(cur.lastrowid)


def get_system_events(limit: int = 200,
                      level: str | None = None,
                      property_id: str | None = None,
                      event_type: str | None = None,
                      path: str = DB_PATH) -> list[dict]:
    """Return newest system decision events first."""
    where = []
    params: list = []

    if level:
        where.append("level=?")
        params.append(str(level).strip().lower())
    if property_id:
        where.append("property_id=?")
        params.append(property_id)
    if event_type:
        where.append("event_type=?")
        params.append(str(event_type).strip().lower())

    sql = "SELECT * FROM system_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"

    try:
        lim = max(1, min(int(limit), 1000))
    except Exception:
        lim = 200
    sql += " LIMIT ?"
    params.append(lim)

    with get_conn(path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    return [dict(r) for r in rows]


def get_system_events_page(limit: int = 200,
                           cursor: int | None = None,
                           level: str | None = None,
                           property_id: str | None = None,
                           event_type: str | None = None,
                           path: str = DB_PATH) -> tuple[list[dict], int | None]:
    """
    Cursor pagination for decisions:
      - ordered by id DESC (newest first)
      - cursor means "older than this id" (id < cursor)
    Returns (rows, next_cursor).
    """
    where = []
    params: list = []

    if level:
        where.append("level=?")
        params.append(str(level).strip().lower())
    if property_id:
        where.append("property_id=?")
        params.append(property_id)
    if event_type:
        where.append("event_type=?")
        params.append(str(event_type).strip().lower())
    if cursor is not None:
        where.append("id < ?")
        params.append(int(cursor))

    sql = "SELECT * FROM system_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC"

    try:
        lim = max(1, min(int(limit), 1000))
    except Exception:
        lim = 200
    sql += " LIMIT ?"
    params.append(lim)

    with get_conn(path) as conn:
        rows = conn.execute(sql, tuple(params)).fetchall()
    out = [dict(r) for r in rows]
    next_cursor = int(out[-1]["id"]) if len(out) >= lim else None
    return out, next_cursor


def get_latest_merged_all(path: str = DB_PATH) -> dict[str, dict]:
    """
    Return the most recent 'merged' row per property.
    Preferred over get_latest_readings_all() for the dashboard —
    merged rows contain the full cross-source rollup.
    Falls back to any source if no merged row exists yet.
    """
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT r.*
            FROM readings r
            INNER JOIN (
                SELECT property_id, MAX(id) as max_id
                FROM readings
                WHERE source = 'merged'
                GROUP BY property_id
            ) latest ON r.property_id = latest.property_id
                     AND r.id = latest.max_id
                     AND r.source = 'merged'
        """).fetchall()
        result = {dict(r)["property_id"]: dict(r) for r in rows}

        # Fall back: properties that have no 'merged' row yet
        if len(result) < len(conn.execute(
                "SELECT DISTINCT property_id FROM readings").fetchall()):
            fallback_rows = conn.execute("""
                SELECT r.*
                FROM readings r
                INNER JOIN (
                    SELECT property_id, MAX(id) as max_id
                    FROM readings GROUP BY property_id
                ) latest ON r.property_id = latest.property_id
                         AND r.id = latest.max_id
            """).fetchall()
            for r in fallback_rows:
                d = dict(r)
                if d["property_id"] not in result:
                    result[d["property_id"]] = d

    return result
