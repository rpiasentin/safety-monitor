# Safety Monitor Backlog Status

Last updated: 2026-03-03

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

## Suggested Next Slice

1. Safety workflows
- Add optional confirmation code flow for unlock actions.
- Add role-based auth if/when `MONITOR_API_KEY` becomes mandatory.

2. Smoke/CO escalation
- Add alerting policy for sustained smoke/CO alarm states.
- Add mute/acknowledge controls for smoke alarms with audit events.

3. Decisions page improvements
- Add pagination/cursor support for very large logs.
- Add export endpoint for incident review.
