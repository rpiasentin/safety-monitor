# EG4 WManage Cloud API — Reverse-Engineering Findings

**Device:** EG4 18kPV inverter with SolarmanV5 WiFi dongle
**Logger serial:** 4372670006
**Cloud portal:** https://monitor.eg4electronics.com
**Discovered:** February 2026 — live probing + community forum cross-reference

---

## 1. Local TCP Banner (Port 8000) — Static, Not Live

Every TCP connection to port 8000 triggers the data-logger to emit a 197-byte
"hello" banner.  **This banner is a static boot-time snapshot** — the values
never update between power cycles.

Confirmed byte offsets (big-endian uint16):

| Offset | Field | Divisor | Example value |
|--------|-------|---------|---------------|
| 60 | Battery voltage | ÷10 | 532 → 53.2 V |
| 80 | PV total power | ÷10 | 1324 → 132.4 W |
| 84 | Total capacity (SOC denominator) | ÷1 | 152 |
| 162 | Remaining capacity (SOC numerator) | ÷10 | 868 → 86.8 |
| 188 | Temperature (ambient) | ÷1 | 23 °C |

SOC is derived: `(remaining_capacity / total_capacity) × 100`
Example: 86.8 ÷ 152 × 100 = 57.1% — confirmed against EG4 portal (57%) ✓

**Why it's useless for live monitoring:** The SolarmanV5 dongle firmware only
populates the banner from cached register values at boot.  During a live sunny
afternoon the portal showed SOC=69%, PV=2665 W while the banner consistently
returned SOC=57.1%, PV=132.4 W across every 15-minute poll.

**Conclusion:** Use the cloud API for all live data.  The banner is retained
only as a last-resort fallback when the cloud is unreachable.

---

## 2. Cloud API Authentication

### Why JSON fails

The `/WManage/api/login` endpoint is a Java (Tomcat) servlet that reads
`application/x-www-form-urlencoded` parameters.  Sending `Content-Type:
application/json` returns **HTTP 500** unconditionally.

### Correct 3-step auth flow

```
Step 1 — Establish JSESSIONID cookie
  GET https://monitor.eg4electronics.com/WManage/
  (no body, just needs to hit the server to receive Set-Cookie: JSESSIONID=...)

Step 2 — Log in with form-encoded body (NOT JSON)
  POST https://monitor.eg4electronics.com/WManage/api/login
  Content-Type: application/x-www-form-urlencoded
  Body: account=<email>&password=<password>

  Success response:
    { "success": true, "userId": 18536 }

  Failure response:
    { "success": false, "msg": "account or password error" }

Step 3 — All subsequent requests use the same session (JSESSIONID cookie)
  The requests.Session() object handles cookie persistence automatically.
```

### Python implementation

```python
import requests

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; SafetyMonitor/1.0)",
    "Origin": "https://monitor.eg4electronics.com",
    "Accept": "application/json, text/plain, */*",
})

# Step 1: establish session cookie
session.get("https://monitor.eg4electronics.com/WManage/", timeout=10)

# Step 2: form-encoded login (data= not json=)
resp = session.post(
    "https://monitor.eg4electronics.com/WManage/api/login",
    data={"account": "user@email.com", "password": "secret"},
    timeout=10,
)
body = resp.json()
assert body["success"] is True
```

---

## 3. Live Data Endpoint

### Endpoint discovery

Several endpoints were explored and rejected:

| Endpoint | Method | Result |
|----------|--------|--------|
| `/WManage/api/device/getRealTimeData?sn=...` | GET | 404 — endpoint removed |
| `/WManage/api/device/getDeviceList` | POST | 200, returns device list (no live data) |
| `/WManage/api/plant/getPlantList` | POST | 200, returns plant metadata (plantId=19042 "forget me") |
| `/WManage/api/inverter/getInverterInfo` | POST `serialNum=` | 200, static device info only |
| **`/WManage/api/inverter/getInverterRuntime`** | **POST `serialNum=`** | **200 — full live data ✓** |

### Request

```
POST https://monitor.eg4electronics.com/WManage/api/inverter/getInverterRuntime
Content-Type: application/x-www-form-urlencoded
Body: serialNum=4372670006
```

Note: the parameter is `serialNum` — not `sn`, not `serialNumber`, not `serial_num`.

### Response (confirmed from live data, Feb 2026)

```json
{
  "success": true,
  "ppv": 3016,
  "soc": 78,
  "vBat": 540,
  "pCharge": 1794,
  "pDisCharge": 0,
  "peps": 1152,
  "pToUser": 0,
  "tinner": 42,
  ...many more fields...
}
```

### Field mapping to canonical names

| API field | Canonical name | Notes |
|-----------|---------------|-------|
| `ppv` | `pv_total_power` | Total MPPT output, watts |
| `soc` | `soc` | Battery SOC, 0–100 |
| `vBat` | `voltage` | Battery voltage × 0.1 → V (e.g. 540 → 54.0 V) |
| `pCharge` | `charge_power` | Battery charging power, W |
| `pDisCharge` | `discharge_power` | Battery discharging power, W |
| `peps` | `power_to_user` | EPS/backup load = house AC demand, W |
| `pToUser` | `power_to_user` | Grid-tied mode fallback (prefer `peps` when > 0) |
| `tinner` | `max_cell_temp` | Inverter internal temperature, °C |

**House load logic:** `peps` is the EPS (off-grid) output — this is what
powers the house when operating in backup/off-grid mode.  Use `peps` when
`peps > 0`; fall back to `pToUser` for grid-tied scenarios.

---

## 4. Additional Useful Endpoints

These were confirmed working during the Feb 2026 probe session:

### Plant list
```
POST /WManage/api/plant/getPlantList
(no body required)
```
Returns: `[{ "plantId": 19042, "name": "forget me", ... }]`

### Inverter static info
```
POST /WManage/api/inverter/getInverterInfo
Body: serialNum=4372670006
```
Returns: model, firmware version, rated power, installation date, etc.

### Device list
```
POST /WManage/api/device/getDeviceList
Body: plantId=19042
```
Returns list of devices associated with a plant.

---

## 5. Session Persistence Notes

- The `JSESSIONID` cookie expires after inactivity (Tomcat default: 30 min).
- When `getInverterRuntime` returns `{"success": false}`, it usually means the
  session has expired.  The correct response is to set `self._session = None`
  and re-authenticate on the next call.
- The `requests.Session()` object persists cookies automatically — no manual
  cookie handling needed.

---

## 6. Community References

These open-source projects were cross-referenced during discovery:

- [twistedroutes/eg4_inverter_api](https://github.com/twistedroutes/eg4_inverter_api)
  — Python wrapper, confirmed endpoint naming
- [joyfulhouse/eg4_web_monitor](https://github.com/joyfulhouse/eg4_web_monitor)
  — Alternative monitor, confirmed auth flow

Both use the same 3-step auth and `getInverterRuntime` endpoint.

---

## 7. What the EG4 Cloud Does NOT Provide

- **Per-MPPT breakdown:** `ppv` is the total of all EG4 internal MPPTs —
  there is no per-channel breakdown in the cloud API.
- **Victron MPPT data:** The two external Victron MPPT chargers (288 and 289)
  are invisible to the EG4 cloud.  Use Victron MQTT for those.
- **Battery current:** Available via Victron SmartShunt MQTT, not EG4 cloud.

---

*Document maintained in `docs/EG4_API_FINDINGS.md` — update as new endpoints
are discovered.*
