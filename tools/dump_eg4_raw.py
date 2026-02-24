#!/usr/bin/env python3
"""
Dump the raw EG4 cloud API response so we can see all field names.
Run from /opt/safety-monitor/app:
  python3 tools/dump_eg4_raw.py
"""
import json, os, sys
import requests
from dotenv import load_dotenv

load_dotenv()

CLOUD_URL = "https://monitor.eg4electronics.com"
USERNAME  = os.getenv("EG4_USERNAME", "")
PASSWORD  = os.getenv("EG4_PASSWORD", "")
LOGGER_SN = os.getenv("EG4_LOGGER_SN", "4372670006")
TIMEOUT   = 15

if not USERNAME or not PASSWORD:
    print("ERROR: EG4_USERNAME / EG4_PASSWORD not set in .env", file=sys.stderr)
    sys.exit(1)

s = requests.Session()
s.headers.update({"User-Agent": "Mozilla/5.0 (compatible; SafetyMonitor/1.0)",
                   "Origin": CLOUD_URL, "Accept": "application/json"})

# Step 1 — JSESSIONID
print("→ GET /WManage/ ...", end=" ", flush=True)
r = s.get(f"{CLOUD_URL}/WManage/", timeout=TIMEOUT)
print(r.status_code, dict(s.cookies))

# Step 2 — login
print("→ POST /WManage/api/login ...", end=" ", flush=True)
r = s.post(f"{CLOUD_URL}/WManage/api/login",
           data={"account": USERNAME, "password": PASSWORD}, timeout=TIMEOUT)
print(r.status_code, r.text[:120])
if not r.json().get("success"):
    print("LOGIN FAILED"); sys.exit(1)

# Step 3 — getInverterRuntime
print("→ POST /WManage/api/inverter/getInverterRuntime ...", end=" ", flush=True)
r = s.post(f"{CLOUD_URL}/WManage/api/inverter/getInverterRuntime",
           data={"serialNum": LOGGER_SN}, timeout=TIMEOUT)
print(r.status_code)
raw = r.json()

print("\n── Full raw response ──────────────────────────────────────────")
print(json.dumps(raw, indent=2))

print("\n── Key fields (solar/battery) ─────────────────────────────────")
for key in sorted(raw.keys()):
    v = raw[key]
    if v not in (None, "", 0, "0") or key in ("ppv","pvPower","soc","pCharge","peps","pToUser","vBat"):
        print(f"  {key:30s} = {v}")
