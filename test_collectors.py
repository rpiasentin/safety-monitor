#!/usr/bin/env python3
"""
Stepwise collector test script.
Run on CT104 with .env populated.
Each test is independent — comment out any you don't want to run.

Usage:
  python3 test_collectors.py            # all tests
  python3 test_collectors.py eg4        # specific collector
  python3 test_collectors.py ha victron # multiple
"""

import json
import sys
import time
from dotenv import load_dotenv

load_dotenv()

TESTS = sys.argv[1:] if len(sys.argv) > 1 else ["eg4", "victron", "ha", "hubitat", "db"]

def banner(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)

def show(data):
    if data is None:
        print("  ❌ RETURNED NONE")
        return
    print(json.dumps(data, indent=2, default=str))


# ── EG4 ───────────────────────────────────────────────────────────────────────
if "eg4" in TESTS:
    banner("EG4 — raw TCP banner (192.168.2.49:8000)")
    try:
        from collectors.eg4 import EG4Client
        client = EG4Client()
        t0 = time.time()
        data = client.get_status()
        elapsed = time.time() - t0
        print(f"  Elapsed: {elapsed:.2f}s")
        show(data)
        if data:
            print(f"\n  ✅ SOC={data.get('soc')}%  Voltage={data.get('voltage')}V"
                  f"  PV={data.get('pv_total_power')}W  Temp={data.get('max_cell_temp')}°C")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")


# ── Victron ───────────────────────────────────────────────────────────────────
if "victron" in TESTS:
    banner("Victron — MQTT (192.168.2.132:1883, portal c0619ab88ee0)")
    try:
        from collectors.victron import VictronClient
        client = VictronClient()
        t0 = time.time()
        data = client.get_status()
        elapsed = time.time() - t0
        print(f"  Elapsed: {elapsed:.2f}s")
        show(data)
        if data:
            print(f"\n  ✅ SOC={data.get('soc')}%  Voltage={data.get('voltage')}V"
                  f"  Power={data.get('power')}W")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")


# ── HA API ────────────────────────────────────────────────────────────────────
if "ha" in TESTS:
    banner("Home Assistant API — Forgetmenot (fm)")
    try:
        from collectors.ha_api import HACollector, HAClient
        import os
        if not os.getenv("HA_LONG_LIVED_TOKEN"):
            print("  ⚠️  HA_LONG_LIVED_TOKEN not set in .env — skipping")
        else:
            # Quick ping
            client = HAClient()
            states = client.get_states()
            print(f"  HA reachable — {len(states)} entities found")
            # Forgetmenot collector
            col = HACollector("fm", {
                "location_id": "fm",
                "primary_temp_sensor": "sensor.fm_main_temp",
            })
            data = col.collect()
            show(data)
            if data:
                print(f"\n  ✅ Temp={data.get('primary_temp')}°F"
                      f"  Devices={len(data.get('battery_devices', []))}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")

    banner("Home Assistant API — High Country (hc) with Tesla")
    try:
        import os
        if not os.getenv("HA_LONG_LIVED_TOKEN"):
            print("  ⚠️  HA_LONG_LIVED_TOKEN not set — skipping")
        else:
            from collectors.ha_api import HACollector
            col = HACollector("hc", {
                "location_id": "hc",
                "primary_temp_sensor": "sensor.hc_main_temp",
                "include_tesla": True,
            })
            data = col.collect()
            show(data)
            if data:
                tesla = data.get("tesla") or {}
                print(f"\n  ✅ Temp={data.get('primary_temp')}°F"
                      f"  Tesla SOC={tesla.get('soc_percent')}%")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")


# ── Hubitat cloud ─────────────────────────────────────────────────────────────
if "hubitat" in TESTS:
    banner("Hubitat Cloud API — Redwood (rd)")
    try:
        import os
        if not os.getenv("HUBITAT_CLOUD_TOKEN"):
            print("  ⚠️  HUBITAT_CLOUD_TOKEN not set in .env — skipping")
        else:
            from collectors.hubitat import HubitatCloudCollector
            col = HubitatCloudCollector("rd", {
                "endpoint": "https://cloud.hubitat.com/api/0709134d-2660-43bc-a6d5-a48d98e8ad1b/apps/58/devices/all",
                "primary_temp_sensor": "sensor.rd_main_temp",
            })
            data = col.collect()
            show(data)
            if data:
                print(f"\n  ✅ Temp={data.get('primary_temp')}°F"
                      f"  Devices={len(data.get('battery_devices', []))}")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")


# ── DB round-trip ──────────────────────────────────────────────────────────────
if "db" in TESTS:
    banner("Database round-trip")
    try:
        import db, os
        os.makedirs("data", exist_ok=True)
        db.init_db()
        db.upsert_reading("test", "unit_test", {
            "soc": 75.0, "voltage": 52.5, "pv_total_power": 200.0,
            "primary_temp": 55.0,
        })
        row = db.get_latest_reading("test", "unit_test")
        print(f"  Wrote and read back: soc={row['soc']}%  volt={row['voltage']}V"
              f"  pv={row['pv_power']}W  temp={row['primary_temp']}°F")
        print("  ✅ SQLite OK")
        # Cleanup
        import sqlite3
        with sqlite3.connect(db.DB_PATH) as conn:
            conn.execute("DELETE FROM readings WHERE property_id='test'")
    except Exception as e:
        print(f"  ❌ ERROR: {e}")


print("\n" + "="*60)
print("  Tests complete — fix any ❌ before running main.py")
print("="*60 + "\n")
