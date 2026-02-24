#!/usr/bin/env python3
"""
List all Tesla-related entity IDs from a Home Assistant instance.
Run from /opt/safety-monitor/app:
  HA_HC_TOKEN=<token> python3 tools/find_tesla_entities.py http://192.168.1.115:8123
"""
import os, sys, requests

url   = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://192.168.1.115:8123"
token = os.getenv("HA_HC_TOKEN") or os.getenv("HA_LONG_LIVED_TOKEN", "")

if not token:
    print("ERROR: set HA_HC_TOKEN=<token> before running", file=sys.stderr)
    sys.exit(1)

resp = requests.get(f"{url}/api/states",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=15)
resp.raise_for_status()

states = resp.json()
tesla  = [s for s in states if "tesla" in s.get("entity_id", "").lower()]

if not tesla:
    print("No entities with 'tesla' in entity_id found.")
    print("Check the integration name — try 'model' or the car name:")
    other = [s for s in states if any(k in s.get("entity_id","").lower()
                                       for k in ("battery_level","charging_power","range"))]
    for s in other[:20]:
        print(f"  {s['entity_id']:60s}  = {s['state']} {s.get('attributes',{}).get('unit_of_measurement','')}")
else:
    print(f"Found {len(tesla)} Tesla entities:\n")
    for s in sorted(tesla, key=lambda x: x["entity_id"]):
        unit  = s.get("attributes", {}).get("unit_of_measurement", "")
        state = s.get("state", "")
        print(f"  {s['entity_id']:60s}  = {state} {unit}")

    # Suggest prefix
    eids = [s["entity_id"] for s in tesla if s["entity_id"].startswith("sensor.")]
    if eids:
        # Try to infer common prefix
        parts = eids[0].replace("sensor.", "").rsplit("_", 2)
        if len(parts) > 1:
            print(f"\nSuggested tesla_vehicle_prefix: \"{parts[0]}\"")
