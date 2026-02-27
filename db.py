"""
SQLite data access layer.
All reads/writes go through this module.

Schema:
  readings        — one row per collection run per source per property
  hubitat_devices — latest battery level per device per property
  alerts          — triggered alert history with cooldown tracking
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


# ── Hubitat devices ───────────────────────────────────────────────────────────

def upsert_hubitat_devices(property_id: str, devices: list[dict],
                            path: str = DB_PATH) -> None:
    now = _now()
    with get_conn(path) as conn:
        for d in devices:
            conn.execute("""
                INSERT OR REPLACE INTO hubitat_devices
                  (property_id, entity_id, friendly_name, battery_pct,
                   last_activity, device_type, collected_at)
                VALUES (?,?,?,?,?,?,?)
            """, (property_id,
                  d.get("entity_id", ""),
                  d.get("friendly_name", ""),
                  d.get("battery_pct"),
                  d.get("last_activity"),
                  d.get("device_type") or d.get("type", ""),
                  now))


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
                CASE WHEN last_activity IS NULL THEN 0 ELSE 1 END ASC,
                last_activity ASC
        """, (property_id,)).fetchall()
    return [dict(r) for r in rows]


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
                ORDER BY triggered_at DESC LIMIT 1
            """, (property_id, alert_type, sensor_id)).fetchone()
        else:
            row = conn.execute("""
                SELECT triggered_at FROM alerts
                WHERE property_id=? AND alert_type=?
                ORDER BY triggered_at DESC LIMIT 1
            """, (property_id, alert_type)).fetchone()
    return row[0] if row else None


def get_recent_alerts(hours: int = 48, path: str = DB_PATH) -> list[dict]:
    with get_conn(path) as conn:
        rows = conn.execute("""
            SELECT * FROM alerts
            WHERE triggered_at >= datetime('now', ?)
            ORDER BY triggered_at DESC
        """, (f"-{hours} hours",)).fetchall()
    return [dict(r) for r in rows]


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
