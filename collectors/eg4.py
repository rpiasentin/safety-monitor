"""
EG4 Inverter Data Collector
============================
Device details (confirmed Feb 2026 via live probe of 192.168.2.49):

  Protocol:     SolarmanV5 data-logger (WiFi dongle)
  Dongle serial: BJ43500603  (ASCII, bytes 17–28 of banner)
  Logger SN:    4372670006  (appears in banner as ASCII "4372670006")
  Firmware:     FAAB-2122   (confirmed in EG4 portal)

  Port 80:  CLOSED  — no HTTP REST API on LAN
  Port 8000: OPEN   — SolarmanV5 binary protocol

HOW DATA IS COLLECTED — Raw TCP Banner Method
─────────────────────────────────────────────
Every TCP connection to port 8000 triggers the data-logger to send a 197-byte
"hello" banner containing identification strings AND live inverter register
values packed as big-endian uint16 pairs.

pysolarmanv5 CANNOT be used because the logger SN (4372670006) exceeds uint32
max, causing a struct.pack overflow in the library.

Confirmed byte offsets (Feb 2026, cross-referenced against Victron MQTT + EG4 portal):
  [60:62]   BE uint16 ÷ 10 → Battery voltage      53.2 V  (Victron=53.1 V ✓)
  [80:82]   BE uint16 ÷ 10 → PV power            132.4 W  (Victron≈127 W ✓)
  [84:86]   BE uint16 ÷  1 → Total capacity         152    (capacity units, for SOC calc)
  [162:164] BE uint16 ÷ 10 → Remaining capacity    86.8    (same units as total)
  [188:190] BE uint16 ÷  1 → Temperature           23 °C  (ambient ✓)

  SOC is derived: (86.8 / 152) × 100 = 57.1%  → EG4 portal confirmed 57% ✓

  Not yet identified in banner: battery current, charge/discharge power, load power.
  Use cloud API (EG4_USE_CLOUD=true) to supplement if those fields are needed.

Cloud API (monitor.eg4electronics.com):
  Login endpoint:
    POST /WManage/api/login
    Body: {"account": EG4_USERNAME, "password": EG4_PASSWORD}
  Status endpoint (after login, token in response):
    GET  /WManage/api/device/getRealTimeData?sn=<LOGGER_SN>
  Set EG4_USE_CLOUD=true in .env to prefer cloud over banner parse.

Add to .env:
  EG4_LOCAL_IP=192.168.2.49
  EG4_USE_CLOUD=false
  EG4_USERNAME=your@email.com
  EG4_PASSWORD=yourpassword
"""

import json
import logging
import os
import socket
import struct
import time

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

EG4_LOCAL_IP  = os.getenv("EG4_LOCAL_IP",  "192.168.2.49")
EG4_PORT      = int(os.getenv("EG4_PORT",  "8000"))
EG4_USERNAME  = os.getenv("EG4_USERNAME",  "")
EG4_PASSWORD  = os.getenv("EG4_PASSWORD",  "")
EG4_LOGGER_SN = os.getenv("EG4_LOGGER_SN", "4372670006")   # for cloud API

EG4_CLOUD_URL = "https://monitor.eg4electronics.com"
EG4_USE_CLOUD = os.getenv("EG4_USE_CLOUD", "false").lower() == "true"

TIMEOUT   = 10  # seconds
CACHE_TTL = 30  # seconds

# ── Banner byte offset map (big-endian uint16) ────────────────────────────────
# All confirmed Feb 2026 via live cross-reference against Victron MQTT and EG4 portal.
#
# Each entry: byte_offset → (field_name, scale_divisor)
#
# SOC is NOT stored directly — it is CALCULATED:
#   remaining_capacity [162] ÷ 10 = 86.8  (units match total_capacity)
#   total_capacity     [84]  ÷  1 = 152
#   SOC = (86.8 / 152) × 100 = 57.1%  → portal confirmed 57% ✓
#
# Grid voltages [86] and [88] are present but not included in the output
# dict — add them if needed (US split-phase L1=226.4V, L2=245.2V typical).

BANNER_OFFSETS = {
    60:  ("voltage",            10.0),  # 532÷10=53.2 V  (Victron=53.1 V ✓)
    80:  ("pv_total_power",     10.0),  # 1324÷10=132.4 W (Victron≈127 W ✓)
    84:  ("_total_capacity",     1.0),  # 152 — used only for SOC calc, not in output
    162: ("_remaining_capacity", 10.0), # 868÷10=86.8 — used only for SOC calc
    188: ("max_cell_temp",       1.0),  # 23 °C direct (portal: ambient temp ✓)
}

BANNER_LENGTH = 197  # bytes

BANNER_LENGTH = 197  # bytes


class EG4Client:
    """
    Collects real-time data from an EG4 inverter via its SolarmanV5 data-logger.

    Primary method: raw TCP banner parse (port 8000).
    Fallback method: EG4 cloud API (monitor.eg4electronics.com).

    Field names follow pylxpweb canonical convention so the rest of the codebase
    doesn't need to know which transport was used.
    """

    def __init__(self):
        self._cache: dict = {}
        self._cache_ts: float = 0.0
        self._cloud_token: str | None = None

    # ── Public API ────────────────────────────────────────────────────────────

    def get_status(self) -> dict | None:
        """Return a normalised status dict or None on failure."""
        return self._fetch()

    def get_soc(self) -> float | None:
        """Battery state of charge (0–100 %)."""
        data = self._fetch()
        if data is None:
            return None
        val = data.get("soc")
        return max(0.0, min(100.0, float(val))) if val is not None else None

    def get_pv_power(self) -> float | None:
        """Total PV/solar input power in watts."""
        data = self._fetch()
        return data.get("pv_total_power") if data else None

    def get_battery_power(self) -> dict | None:
        """
        Returns dict:
          charging_power    (W, positive when charging)
          discharging_power (W, positive when discharging)
          net_power         (W, positive = charging)
          voltage           (V)
          current           (A)
          temperature       (°C, or None)
        """
        data = self._fetch()
        if data is None:
            return None
        charge    = data.get("charge_power",    0.0) or 0.0
        discharge = data.get("discharge_power", 0.0) or 0.0
        return {
            "charging_power":    charge,
            "discharging_power": discharge,
            "net_power":         charge - discharge,
            "voltage":           data.get("voltage"),
            "current":           data.get("current"),
            "temperature":       data.get("max_cell_temp"),
        }

    def get_load(self) -> float | None:
        """AC load power in watts."""
        data = self._fetch()
        return data.get("power_to_user") if data else None

    # ── Internal fetch (cache wrapper) ────────────────────────────────────────

    def _fetch(self) -> dict | None:
        if self._cache and (time.time() - self._cache_ts) < CACHE_TTL:
            return self._cache
        if EG4_USE_CLOUD:
            result = self._fetch_cloud()
        else:
            result = self._fetch_banner()
            if result is None and EG4_USERNAME:
                logger.warning("EG4 banner parse failed, falling back to cloud API.")
                result = self._fetch_cloud()
        if result:
            self._cache    = result
            self._cache_ts = time.time()
        return result

    # ── Raw TCP banner method ─────────────────────────────────────────────────

    def _fetch_banner(self) -> dict | None:
        """
        Connect to port 8000, read the 197-byte SolarmanV5 hello banner, and
        extract inverter data from confirmed byte offsets.

        The device sends the banner immediately on connection — no request needed.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(TIMEOUT)
            s.connect((EG4_LOCAL_IP, EG4_PORT))
            data = b""
            deadline = time.time() + TIMEOUT
            while len(data) < BANNER_LENGTH and time.time() < deadline:
                chunk = s.recv(512)
                if not chunk:
                    break
                data += chunk
            s.close()
        except Exception as exc:
            logger.error("EG4 banner TCP connection failed: %s", exc)
            return None

        if len(data) < BANNER_LENGTH:
            logger.error(
                "EG4 banner too short: got %d bytes (expected %d).",
                len(data), BANNER_LENGTH,
            )
            return None

        return self._parse_banner(data[:BANNER_LENGTH])

    def _parse_banner(self, data: bytes) -> dict:
        """
        Decode all confirmed uint16 BE fields from the 197-byte SolarmanV5 banner.

        SOC is derived: (remaining_capacity / total_capacity) × 100
          [162] ÷ 10 = remaining (86.8),  [84] ÷ 1 = total (152)
          86.8 / 152 × 100 = 57.1%  — confirmed against EG4 portal (57%) ✓
        """
        raw_fields: dict = {}

        for offset, (field, divisor) in BANNER_OFFSETS.items():
            if offset + 2 > len(data):
                logger.debug("EG4 banner: offset %d out of range for '%s'.", offset, field)
                continue
            raw = struct.unpack(">H", data[offset:offset+2])[0]
            if raw == 0:
                continue
            raw_fields[field] = raw / divisor if divisor != 1.0 else float(raw)

        # Build the output dict, excluding internal fields (prefixed with _)
        result: dict = {k: v for k, v in raw_fields.items() if not k.startswith("_")}

        # Calculate SOC from remaining and total capacity
        remaining = raw_fields.get("_remaining_capacity")  # e.g. 86.8
        total     = raw_fields.get("_total_capacity")       # e.g. 152
        if remaining is not None and total and total > 0:
            result["soc"] = round((remaining / total) * 100, 1)
        else:
            logger.warning("EG4 banner: could not calculate SOC (remaining=%s, total=%s).", remaining, total)

        logger.debug(
            "EG4 banner: voltage=%.1fV  soc=%.1f%%  pv=%.0fW  temp=%s°C",
            result.get("voltage", 0),
            result.get("soc", 0),
            result.get("pv_total_power", 0),
            result.get("max_cell_temp"),
        )
        return result

    # ── Cloud API method ──────────────────────────────────────────────────────

    def _fetch_cloud(self) -> dict | None:
        """
        Fetch live data from monitor.eg4electronics.com.

        Auth flow:
          1. POST /WManage/api/login  → token
          2. GET  /WManage/api/device/getRealTimeData?sn=<LOGGER_SN>  → data

        Requires EG4_USERNAME and EG4_PASSWORD in .env.
        """
        if not EG4_USERNAME or not EG4_PASSWORD:
            logger.warning(
                "EG4 cloud fetch skipped: EG4_USERNAME/EG4_PASSWORD not set in .env."
            )
            return None

        token = self._cloud_login()
        if token is None:
            return None

        try:
            resp = requests.get(
                f"{EG4_CLOUD_URL}/WManage/api/device/getRealTimeData",
                params={"sn": EG4_LOGGER_SN},
                headers={"Authorization": f"Bearer {token}"},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            raw = resp.json()
            logger.debug("EG4 cloud raw: %s", json.dumps(raw)[:300])
            return self._normalise_cloud(raw)
        except Exception as exc:
            logger.error("EG4 cloud data fetch error: %s", exc)
            self._cloud_token = None   # force re-login next time
            return None

    def _cloud_login(self) -> str | None:
        """Login to EG4 cloud and return bearer token, caching it in self._cloud_token."""
        if self._cloud_token:
            return self._cloud_token
        try:
            resp = requests.post(
                f"{EG4_CLOUD_URL}/WManage/api/login",
                json={"account": EG4_USERNAME, "password": EG4_PASSWORD},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            body = resp.json()
            token = (
                body.get("token")
                or body.get("data", {}).get("token")
                or body.get("access_token")
            )
            if not token:
                logger.error("EG4 cloud login: no token in response: %s", body)
                return None
            self._cloud_token = token
            logger.debug("EG4 cloud login OK, token cached.")
            return token
        except Exception as exc:
            logger.error("EG4 cloud login error: %s", exc)
            return None

    def _normalise_cloud(self, raw: dict) -> dict:
        """
        Map EG4 cloud API field names → pylxpweb canonical names.
        Cloud API uses pylxpweb-compatible names so very little mapping is needed.
        """
        data = raw.get("data", raw)   # unwrap envelope if present
        mapping = {
            "soc":             ["soc", "SOC", "batSoc"],
            "voltage":         ["voltage", "batVoltage", "vBat"],
            "current":         ["current", "batCurrent"],
            "charge_power":    ["charge_power", "chargePower"],
            "discharge_power": ["discharge_power", "dischargePower"],
            "pv_total_power":  ["pv_total_power", "pvPower", "pvTotalPower"],
            "power_to_user":   ["power_to_user", "loadPower", "powerToUser"],
            "power_to_grid":   ["power_to_grid", "gridPower"],
            "max_cell_temp":   ["max_cell_temp", "batTemp", "maxCellTemp"],
        }
        result: dict = {}
        for canonical, variants in mapping.items():
            for key in variants:
                if key in data:
                    try:
                        val = float(data[key])
                        if key == "vBat":
                            val /= 10.0
                        result[canonical] = val
                    except (TypeError, ValueError):
                        pass
                    break
        return result
