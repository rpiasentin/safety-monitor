# Safety Monitor Backlog Status

Last updated: 2026-03-12 (light/full summary load profiles)

## Delivered In This Pass

0. Water leak incident hardening
- Separated Hubitat shutoff valves from generic water sensors.
- Added property-card safety visibility for:
  - water sensors
  - shutoff valves
- Added shutoff valve controls from the dashboard:
  - open/close per valve
  - open/close all valves for a property
- Added collector-side transition logging for:
  - `water_sensor_wet`
  - `water_sensor_cleared`
  - `water_shutoff_closed`
  - `water_shutoff_opened`
- Fixed valve misclassification so devices like the Lariat main shutoff no longer appear as leak sensors.
- Added persistent shutoff-valve incident lifecycle state:
  - active `water_shutoff` alerts on unexpected closed valves
  - acknowledgement until valve reopen
  - auto-resolve on valve reopen
- Added incident timeline events:
  - `water_incident_opened`
  - `water_incident_acknowledged`
  - `water_incident_resolved`
- Added dashboard acknowledgement action for active shutoff incidents.

1. Notification/deploy hardening
- Added deterministic alert-rule regression matrix script:
  - `tools/notification_rules_matrix.py`
  - Covers push toggles for temperature, battery, water, smoke, offline
  - Covers maker-device suppression (global + per-device)
  - Covers shutoff-valve push toggle, acknowledgement latch, expected-close suppression, and reopen resolution
- Added controlled preflight mode:
  - `make controlled-pass` (sets `SM_CONTROLLED_PASS=1` + ownership auto-fix)
- Added CT104 post-deploy ownership/runtime guard:
  - `tools/ct104_post_deploy_guard.sh`
  - `make post-deploy-guard`
- Preflight now validates CT104 `config.yaml` ownership and can auto-fix to:
  - `safetymon:safetymon`

2. Hubitat device lifecycle hardening
- Added auto-pruning for `hubitat_devices` records when devices disappear from Hubitat.
- Kept prune safety guard: no mass delete on empty/invalid upstream payloads.

3. Critical decision traceability
- Added persistent `system_events` table in SQLite.
- Added event helpers in `db.py`.
- Added dedicated page: `/decisions`.
- Added API endpoint: `GET /api/system/decisions`.
- Wired key operator/system actions into event logging.

4. Property lock controls
- Added lock state extraction from Hubitat feed.
- Added property-card lock indicators.
- Added action endpoints:
  - `POST /api/property/{property_id}/locks/all/{lock|unlock}`
  - `POST /api/property/{property_id}/locks/{device_id}/{lock|unlock}`
- Added dashboard controls for lock/unlock all and per-lock actions.

5. Property smoke/CO visibility
- Added smoke/CO state extraction from Hubitat feed.
- Added smoke/CO status panel to each property card.

6. Mobile usability pass
- Kept thumb-safe control sizing for new lock actions.
- Updated property safety rows for smaller screens.

7. Home Assistant Tesla resilience
- Added Home Assistant feed-health box support for properties using `ha_api`.
- Added time-bounded stale Tesla/Powerwall fallback when HA feed is temporarily unavailable.
- Added warning text on property cards while stale fallback is active.
- Added decision-log event type: `stale_tesla_fallback_applied`.

8. Smoke/CO escalation + controls
- Added sustained smoke/CO alarm escalation policy (`alerts.smoke.sustain_minutes`).
- Added smoke alarm alert type with persistent active alerts until clear/ack.

9. Summary load profiles
- Added explicit summary routes:
  - `/system/summary/full`
  - `/system/summary/light`
  - `/property/{id}/summary/full`
  - `/property/{id}/summary/light`
- Added load-profile-aware shell navigation so Summary links preserve the current profile.
- Added `X-SM-Load-Profile` support so upstream proxies can default traffic into the lighter summary mode.
- Added a lighter system summary rendering path for public/witness traffic.
- Reduced property-page payload in `light` mode by omitting heavier sections like energy snapshot, batteries, and container health from the initial render.
- Added per-sensor smoke controls on property cards:
  - acknowledge until clear
  - mute for configurable minutes
  - unmute
- Added audit events for smoke actions and alarm lifecycle.

9. Decisions page scale improvements
- Added cursor-based pagination for `/decisions`.
- Added cursor support for `GET /api/system/decisions` via `cursor` query param + `X-Next-Cursor` header.
- Added incident export endpoint:
  - `GET /api/system/decisions/export?format=csv|json`

10. Local agent tooling hardening
- Added repo-local agent notes:
  - `AGENTS.md`
- Added Python version pin for local development:
  - `.python-version` => `3.11`
- Added local browser automation bootstrap expectations:
  - `node`, `npm`, `npx`
  - `playwright-cli` or the Codex Playwright wrapper
- Updated `tools/preflight_access.sh` to validate:
  - repo virtualenv exists at `.venv`
  - local Python runtime is `3.11+`
  - core imports succeed locally (`fastapi`, `jinja2`, `requests`, `yaml`)
  - local browser automation commands are available before UI verification work begins
- Updated README bootstrap instructions so new agents can reproduce the same local setup before touching deploys.
- Verified headed browser access against the live CT104 dashboard through a local SSH tunnel.

## Suggested Next Slice

1. Safety workflows
- Add optional confirmation code flow for unlock actions.
- Add role-based auth if/when `MONITOR_API_KEY` becomes mandatory.

10. High Country HA prod cutover prep
- Confirmed HC prod HA endpoint is `192.168.4.115:8123`; legacy `192.168.4.139` is being decommissioned.
- Stored the prod HC HA token out-of-band at `/Users/rpias/dev/vscode-dev-env/.notes_access/secrets/highcountry_prod_ha_token.txt`.
- Do not commit raw HA tokens to git; keep them only in secure local notes and CT104 `/opt/safety-monitor/.env`.
