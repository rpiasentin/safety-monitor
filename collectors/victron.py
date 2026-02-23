"""
Victron Venus OS Data Collector
================================
CONFIRMED via live probe of 192.168.2.132 (Feb 2026):

  Device:      Victron GX (Venus OS)
  Portal ID:   c0619ab88ee0
  Battery:     SmartShunt 500A/50mV  (instance 279, service ttyS7)
  Chargers:    Two MPPT solar chargers (instances 288 and 289)
  Protocol:    MQTT broker on port 1883, no auth on LAN

  IMPORTANT — system/0/Dc/Battery/Soc does NOT exist as an individual topic
  on this installation. SOC is only published via the Batteries JSON array:
    N/c0619ab88ee0/system/0/Batteries

  That single topic returns a complete snapshot of all battery fields.

Confirmed live topic map:
  system/0/Batteries             → JSON array, one entry per battery service
    [0].soc                      → State of charge (%, float)
    [0].voltage                  → Battery voltage (V)
    [0].current                  → Current (A, negative = discharging)
    [0].power                    → Power (W, negative = discharging)
    [0].state                    → 0=Idle  1=Charging  2=Discharging
    [0].timetogo                 → Seconds remaining (float, or null)
    [0].name                     → "SmartShunt 500A/50mV"
    [0].instance                 → 279
    [0].active_battery_service   → true
    [0].ConsumedAmphours         → Cumulative Ah drawn (negative float)

  system/0/Dc/Battery/Voltage    → Battery voltage (V)       — also in array
  system/0/Dc/Battery/Current    → Battery current (A)       — also in array
  system/0/Dc/Battery/Power      → Battery power (W)         — also in array
  system/0/Dc/Battery/TimeToGo   → Time remaining (s)        — also in array
  system/0/Dc/Pv/Power           → Combined solar input (W)  — UNIQUE to this topic
  system/0/Dc/Pv/Current         → Combined solar current (A)
  system/0/Dc/System/Power       → Total system DC load (W)
  system/0/Dc/System/Current     → Total system DC current (A)

  battery/279/Dc/0/Current       → Direct from SmartShunt (A)
  battery/279/Dc/0/Voltage       → Direct from SmartShunt (V)
  battery/279/Dc/0/Power         → Direct from SmartShunt (W)
  battery/279/TimeToGo           → Direct from SmartShunt (s)
  battery/279/ConsumedAmphours   → Cumulative Ah (negative float)

  solarcharger/288/Yield/Power   → Charger 1 output (W)
  solarcharger/289/Yield/Power   → Charger 2 output (W)
  solarcharger/288/Pv/V          → Charger 1 panel voltage (V)
  solarcharger/289/Pv/V          → Charger 2 panel voltage (V)

  NOT AVAILABLE:
  - Battery temperature (SmartShunt 500A/50mV has no temp sensor)
  - system/0/Dc/Battery/Soc (individual topic does not exist on this unit)

Sign convention (confirmed from live data):
  Negative current/power → battery discharging
  Positive current/power → battery charging
  State 2 seen with negative power confirmed as discharging

Add to .env:
  VICTRON_PORTAL_ID=c0619ab88ee0
"""

import json
import logging
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

VICTRON_IP        = os.getenv("VICTRON_IP", "192.168.2.132")
VICTRON_PORT      = int(os.getenv("VICTRON_MQTT_PORT", "1883"))
VICTRON_PORTAL_ID = os.getenv("VICTRON_PORTAL_ID", "c0619ab88ee0")

# Battery service instance (confirmed: 279 = SmartShunt 500A/50mV)
BATTERY_INSTANCE  = int(os.getenv("VICTRON_BATTERY_INSTANCE", "279"))

TIMEOUT     = 15   # seconds to wait for MQTT messages
CACHE_TTL   = 30   # seconds to reuse cached values within same run


class VictronClient:
    """
    Collects real-time data from the Victron GX device at 192.168.2.132.

    Primary data source: MQTT topic system/0/Batteries (JSON array).
    This single topic contains SOC, voltage, current, power, state, and
    timetogo for the SmartShunt 500A/50mV (instance 279).

    PV power comes from system/0/Dc/Pv/Power (combined across both chargers).

    Requires: paho-mqtt==1.6.1  (pip install paho-mqtt==1.6.1)
    """

    def __init__(self):
        self.ip        = VICTRON_IP
        self.port      = VICTRON_PORT
        self.pid       = VICTRON_PORTAL_ID
        self._cache: dict = {}
        self._cache_ts: float = 0.0

    # ── Public API ────────────────────────────────────────────────────────────

    def get_status(self) -> dict | None:
        """Return a normalised status dict or None on failure."""
        return self._fetch()

    def get_soc(self) -> float | None:
        """Battery state of charge (0–100 %). Source: system/0/Batteries[0].soc"""
        data = self._fetch()
        if data is None:
            return None
        val = data.get("soc")
        return round(max(0.0, min(100.0, float(val))), 1) if val is not None else None

    def get_charging_power(self) -> float | None:
        """
        Net battery power in watts.
        Negative = discharging.  Positive = charging.
        Source: system/0/Batteries[0].power
        """
        data = self._fetch()
        return data.get("power") if data else None

    def get_battery_data(self) -> dict | None:
        """
        Full battery snapshot:
          soc              (%, 0–100)
          voltage          (V)
          current          (A, negative = discharging)
          power            (W, negative = discharging)
          state            (0=Idle / 1=Charging / 2=Discharging)
          timetogo         (seconds, or None)
          pv_power         (W, combined solar input from both MPPT chargers)
          consumed_ah      (Ah cumulative, negative float)
          temperature      (always None — SmartShunt 500A has no temp sensor)
          device_name      "SmartShunt 500A/50mV"
        """
        data = self._fetch()
        if data is None:
            return None
        return {
            "soc":          data.get("soc"),
            "voltage":      data.get("voltage"),
            "current":      data.get("current"),
            "power":        data.get("power"),
            "state":        data.get("state"),
            "timetogo":     data.get("timetogo"),
            "pv_power":     data.get("pv_power"),
            "consumed_ah":  data.get("consumed_ah"),
            "temperature":  None,   # SmartShunt 500A/50mV has no temperature sensor
            "device_name":  data.get("device_name", "SmartShunt 500A/50mV"),
        }

    # ── Internal fetch ────────────────────────────────────────────────────────

    def _fetch(self) -> dict | None:
        """Fetch via MQTT, using cache if fresh enough."""
        if self._cache and (time.time() - self._cache_ts) < CACHE_TTL:
            return self._cache
        return self._fetch_mqtt()

    def _fetch_mqtt(self) -> dict | None:
        """
        Subscribe to the two key topics:
          1. N/{pid}/system/0/Batteries      — full battery JSON array (has SOC)
          2. N/{pid}/system/0/Dc/Pv/Power    — combined PV power

        Sends a keepalive read request first to ensure fresh values are published.
        """
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            logger.error(
                "paho-mqtt not installed. Run: pip install paho-mqtt==1.6.1\n"
                "Then add 'paho-mqtt==1.6.1' to requirements.txt"
            )
            return None

        pid      = self.pid
        result   = {}
        got_batt = False
        got_pv   = False

        TOPIC_BATTERIES  = f"N/{pid}/system/0/Batteries"
        TOPIC_PV_POWER   = f"N/{pid}/system/0/Dc/Pv/Power"
        TOPIC_MPPT_288   = f"N/{pid}/solarcharger/288/Yield/Power"
        TOPIC_MPPT_289   = f"N/{pid}/solarcharger/289/Yield/Power"

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                client.subscribe(TOPIC_BATTERIES)
                client.subscribe(TOPIC_PV_POWER)
                client.subscribe(TOPIC_MPPT_288)
                client.subscribe(TOPIC_MPPT_289)
                # Keepalive triggers the broker to publish fresh retained values
                client.publish(f"R/{pid}/keepalive", payload="[]")
            else:
                logger.error("Victron MQTT connect failed: rc=%d", rc)

        def on_message(client, userdata, msg):
            nonlocal got_batt, got_pv
            try:
                payload = json.loads(msg.payload.decode())
                value   = payload.get("value") if isinstance(payload, dict) else payload

                if msg.topic == TOPIC_BATTERIES:
                    # value is a list; grab the active battery service entry
                    if isinstance(value, list) and value:
                        batt = next(
                            (b for b in value if b.get("active_battery_service")),
                            value[0]
                        )
                        result["soc"]         = batt.get("soc")
                        result["voltage"]     = batt.get("voltage")
                        result["current"]     = batt.get("current")
                        result["power"]       = batt.get("power")
                        result["state"]       = batt.get("state")
                        result["timetogo"]    = batt.get("timetogo")
                        result["consumed_ah"] = batt.get("ConsumedAmphours")
                        result["device_name"] = batt.get("name", "SmartShunt 500A/50mV")
                        got_batt = True
                        logger.debug(
                            "Victron battery: soc=%.1f%% power=%.1fW voltage=%.2fV state=%s",
                            result["soc"] or 0,
                            result["power"] or 0,
                            result["voltage"] or 0,
                            result["state"],
                        )

                elif msg.topic == TOPIC_PV_POWER:
                    result["pv_power"] = value
                    got_pv = True
                    logger.debug("Victron PV combined: %.1fW", value or 0)

                elif msg.topic == TOPIC_MPPT_288:
                    result["pv_charger_288"] = value
                    logger.debug("Victron MPPT 288: %.1fW", value or 0)

                elif msg.topic == TOPIC_MPPT_289:
                    result["pv_charger_289"] = value
                    logger.debug("Victron MPPT 289: %.1fW", value or 0)

            except Exception as exc:
                logger.warning("Victron MQTT parse error on %s: %s", msg.topic, exc)

        client = mqtt.Client(client_id="safety-monitor-victron", clean_session=True)
        client.on_connect = on_connect
        client.on_message = on_message

        try:
            client.connect(self.ip, self.port, keepalive=30)
            client.loop_start()
            deadline = time.time() + TIMEOUT
            while (not (got_batt and got_pv)) and time.time() < deadline:
                time.sleep(0.1)
            client.loop_stop()
            client.disconnect()
        except Exception as exc:
            logger.error("Victron MQTT connection error: %s", exc)
            return None

        if not got_batt:
            logger.error(
                "Victron: did not receive Batteries topic within %ss. "
                "Check that portal ID '%s' is correct and MQTT broker is running.",
                TIMEOUT, pid,
            )
            return None

        if not got_pv:
            logger.warning("Victron: PV power topic not received — solar may be offline.")
            result.setdefault("pv_power", None)

        self._cache    = result
        self._cache_ts = time.time()
        return result
