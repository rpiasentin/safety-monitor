# Safety Monitor Backlog Status

Last updated: 2026-03-04 (night pass)

## Delivered In This Pass

1. Hubitat device lifecycle hardening
- Added auto-pruning for `hubitat_devices` records when devices disappear from Hubitat.
- Kept prune safety guard: no mass delete on empty/invalid upstream payloads.

2. Critical decision traceability
- Added persistent `system_events` table in SQLite.
- Added event helpers in `db.py`.
- Added dedicated page: `/decisions`.
- Added API endpoint: `GET /api/system/decisions`.
- Wired key operator/system actions into event logging.

3. Property lock controls
- Added lock state extraction from Hubitat feed.
- Added property-card lock indicators.
- Added action endpoints:
  - `POST /api/property/{property_id}/locks/all/{lock|unlock}`
  - `POST /api/property/{property_id}/locks/{device_id}/{lock|unlock}`
- Added dashboard controls for lock/unlock all and per-lock actions.

4. Property smoke/CO visibility
- Added smoke/CO state extraction from Hubitat feed.
- Added smoke/CO status panel to each property card.

5. Mobile usability pass
- Kept thumb-safe control sizing for new lock actions.
- Updated property safety rows for smaller screens.

6. Home Assistant Tesla resilience
- Added Home Assistant feed-health box support for properties using `ha_api`.
- Added time-bounded stale Tesla/Powerwall fallback when HA feed is temporarily unavailable.
- Added warning text on property cards while stale fallback is active.
- Added decision-log event type: `stale_tesla_fallback_applied`.

7. Smoke/CO escalation + controls
- Added sustained smoke/CO alarm escalation policy (`alerts.smoke.sustain_minutes`).
- Added smoke alarm alert type with persistent active alerts until clear/ack.
- Added per-sensor smoke controls on property cards:
  - acknowledge until clear
  - mute for configurable minutes
  - unmute
- Added audit events for smoke actions and alarm lifecycle.

8. Decisions page scale improvements
- Added cursor-based pagination for `/decisions`.
- Added cursor support for `GET /api/system/decisions` via `cursor` query param + `X-Next-Cursor` header.
- Added incident export endpoint:
  - `GET /api/system/decisions/export?format=csv|json`

## Suggested Next Slice

1. Safety workflows
- Add optional confirmation code flow for unlock actions.
- Add role-based auth if/when `MONITOR_API_KEY` becomes mandatory.
