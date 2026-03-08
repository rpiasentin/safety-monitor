"""
Helpers for translating raw Hubitat valve states into water-service semantics.

Some shutoff valves report a raw `closed` state while the property's water
service is actually ON, so UI and alert logic should reason about `Water On`
and `Water Off` instead of raw mechanical position.
"""

from __future__ import annotations


def _norm(value) -> str:
    return str(value or "").strip().lower()


def valve_service_on_raw_state(property_cfg: dict | None, valve_id: str | None = None) -> str:
    """Return which raw valve state means `Water On` for this valve."""
    cfg = property_cfg or {}
    did = str(valve_id or "").strip()

    raw_map = cfg.get("water_valve_service_on_map") or {}
    if isinstance(raw_map, dict) and did:
        mapped = _norm(raw_map.get(did))
        if mapped in {"open", "closed"}:
            return mapped

    fallback = _norm(cfg.get("water_valve_service_on_state") or "open")
    return fallback if fallback in {"open", "closed"} else "open"


def valve_service_state(raw_state: str | None,
                        property_cfg: dict | None = None,
                        valve_id: str | None = None) -> dict:
    """Decorate a raw valve state with water-service meaning."""
    raw = _norm(raw_state)
    water_on_raw = valve_service_on_raw_state(property_cfg, valve_id)
    water_off_raw = "closed" if water_on_raw == "open" else "open"

    out = {
        "raw_state": raw or "unknown",
        "water_on_raw_state": water_on_raw,
        "water_off_raw_state": water_off_raw,
        "service_state": "unknown",
        "status": "unknown",
        "state_label": "Unknown",
        "can_turn_water_on": True,
        "can_turn_water_off": True,
    }

    if raw == water_on_raw:
        out.update({
            "service_state": "on",
            "status": "good",
            "state_label": "Water On",
            "can_turn_water_on": False,
            "can_turn_water_off": True,
        })
        return out

    if raw == water_off_raw:
        out.update({
            "service_state": "off",
            "status": "critical",
            "state_label": "Water Off",
            "can_turn_water_on": True,
            "can_turn_water_off": False,
        })
        return out

    if raw == "opening":
        turning_on = (water_on_raw == "open")
        out.update({
            "service_state": "transition_on" if turning_on else "transition_off",
            "status": "warning",
            "state_label": "Turning Water On" if turning_on else "Turning Water Off",
            "can_turn_water_on": not turning_on,
            "can_turn_water_off": turning_on,
        })
        return out

    if raw == "closing":
        turning_on = (water_on_raw == "closed")
        out.update({
            "service_state": "transition_on" if turning_on else "transition_off",
            "status": "warning",
            "state_label": "Turning Water On" if turning_on else "Turning Water Off",
            "can_turn_water_on": not turning_on,
            "can_turn_water_off": turning_on,
        })
        return out

    if raw:
        out["state_label"] = raw.replace("_", " ").title()
    return out


def water_action_to_raw_command(action: str | None,
                                property_cfg: dict | None = None,
                                valve_id: str | None = None) -> str:
    """Map `on/off` service commands to raw Hubitat open/close commands."""
    cmd = _norm(action)
    if cmd in {"open", "close"}:
        return cmd

    water_on_raw = valve_service_on_raw_state(property_cfg, valve_id)
    water_off_raw = "closed" if water_on_raw == "open" else "open"
    if cmd == "on":
        return water_on_raw
    if cmd == "off":
        return water_off_raw
    return ""
