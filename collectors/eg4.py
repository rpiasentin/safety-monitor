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
        self._session: requests.Session | None = None   # authenticated cloud session

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

        Confirmed auth flow (reverse-engineered Feb 2026):
          1. GET  /WManage/                              → establishes JSESSIONID cookie
          2. POST /WManage/api/login  form-encoded       → sets session auth cookie
          3. POST /WManage/api/inverter/getInverterRuntime  serialNum=LOGGER_SN  → data

        IMPORTANT: login MUST use form-encoded data (not JSON) — JSON returns HTTP 500.
        Session cookie (JSESSIONID) must persist across all requests via requests.Session.

        Requires EG4_USERNAME and EG4_PASSWORD in .env.
        """
        if not EG4_USERNAME or not EG4_PASSWORD:
            logger.warning(
                "EG4 cloud fetch skipped: EG4_USERNAME/EG4_PASSWORD not set in .env."
            )
            return None

        if not self._session and not self._cloud_login():
            return None

        try:
            resp = self._session.post(
                f"{EG4_CLOUD_URL}/WManage/api/inverter/getInverterRuntime",
                data={"serialNum": EG4_LOGGER_SN},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            raw = resp.json()
            if not raw.get("success"):
                logger.warning("EG4 cloud runtime: success=false — %s", raw)
                self._session = None   # force re-login next time
                return None
            logger.debug(
                "EG4 cloud runtime: soc=%s ppv=%sW (s1=%s s2=%s) pCharge=%sW peps=%sW tinner=%s°C",
                raw.get("soc"), raw.get("ppv"),
                raw.get("ppv1"), raw.get("ppv2"),
                raw.get("pCharge"), raw.get("peps"), raw.get("tinner"),
            )
            return self._normalise_cloud(raw)
        except Exception as exc:
            logger.error("EG4 cloud runtime fetch error: %s", exc)
            self._session = None   # force re-login next time
            return None

    def _cloud_login(self) -> bool:
        """
        Establish a requests.Session, get JSESSIONID, and log in via form-encoded POST.
        Returns True on success.  Stores session in self._session.
        """
        try:
            s = requests.Session()
            s.headers.update({
                "User-Agent": "Mozilla/5.0 (compatible; SafetyMonitor/1.0)",
                "Origin":     EG4_CLOUD_URL,
                "Accept":     "application/json, text/plain, */*",
            })
            # Step 1 — establish JSESSIONID cookie
            s.get(f"{EG4_CLOUD_URL}/WManage/", timeout=TIMEOUT)
            # Step 2 — form-encoded login (JSON returns HTTP 500)
            resp = s.post(
                f"{EG4_CLOUD_URL}/WManage/api/login",
                data={"account": EG4_USERNAME, "password": EG4_PASSWORD},
                timeout=TIMEOUT,
            )
            resp.raise_for_status()
            body = resp.json()
            if not body.get("success"):
                logger.error("EG4 cloud login failed: %s", body)
                return False
            self._session = s
            logger.info("EG4 cloud login OK (userId=%s)", body.get("userId"))
            return True
        except Exception as exc:
            logger.error("EG4 cloud login error: %s", exc)
            self._session = None
            return False

    def _normalise_cloud(self, raw: dict) -> dict:
        """
        Map EG4 getInverterRuntime response fields → canonical names.

        Confirmed field names from live API response (Feb 2026):
          ppv        → pv_total_power   (total EG4 MPPT output, W)
          soc        → soc              (battery SOC, 0-100)
          vBat       → voltage          (battery voltage × 0.1 → V, e.g. 540 → 54.0V)
          pCharge    → charge_power     (battery charging power, W)
          pDisCharge → discharge_power  (battery discharging power, W)
          peps       → power_to_user    (EPS/backup load = house demand, W)
          tinner     → max_cell_temp    (inverter internal temperature, °C)
        """
        result: dict = {}

        if (v := raw.get("soc")) is not None:
            result["soc"] = float(v)

        if (v := raw.get("vBat")) is not None:
            result["voltage"] = float(v) / 10.0          # 540 → 54.0 V

        if (v := raw.get("ppv")) is not None:
            result["pv_total_power"] = float(v)           # W — sum of all EG4 strings

        # Per-string MPPT breakdown (ppv1, ppv2 — matches Victron pv_charger_288/289 pattern)
        if (v := raw.get("ppv1")) is not None:
            result["pv_string_1"] = float(v)
        if (v := raw.get("ppv2")) is not None:
            result["pv_string_2"] = float(v)
        if (v := raw.get("ppv3")) is not None:
            result["pv_string_3"] = float(v)

        if (v := raw.get("pCharge")) is not None:
            result["charge_power"] = float(v)             # W

        if (v := raw.get("pDisCharge")) is not None:
            result["discharge_power"] = float(v)          # W

        # House load: prefer EPS output (peps); fall back to pToUser for grid-tied mode
        peps = raw.get("peps")
        ptou = raw.get("pToUser")
        load = peps if (peps is not None and peps > 0) else ptou
        if load is not None:
            result["power_to_user"] = float(load)

        if (v := raw.get("tinner")) is not None:
            result["max_cell_temp"] = float(v)            # °C inverter internal temp

        return result
