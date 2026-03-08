"""
Helpers for translating raw Hubitat water-cutoff states into water-service
semantics.

Some sites expose true valves with raw `open`/`closed` states while others,
like High Country, expose the cutoff through a relay with raw `on`/`off`
states. UI and alert logic should reason about `Water On` and `Water Off`
instead of raw mechanical orientation.
"""

from __future__ import annotations


def _norm(value) -> str:
    return str(value or "").strip().lower()


def valve_is_excluded(property_cfg: dict | None, valve_id: str | None) -> bool:
    cfg = property_cfg or {}
    did = str(valve_id or "").strip()
    if not did:
        return False
    excluded = list(cfg.get("water_cutoff_exclude_ids") or []) + list(cfg.get("water_valve_exclude_ids") or [])
    return did in {str(item or "").strip() for item in excluded if str(item or "").strip()}


def _paired_raw_state(raw_state: str) -> str:
    raw = _norm(raw_state)
    pairs = {
        "open": "closed",
        "closed": "open",
        "on": "off",
        "off": "on",
    }
    return pairs.get(raw, "")


def _raw_state_to_command(raw_state: str) -> str:
    raw = _norm(raw_state)
    return {
        "open": "open",
        "closed": "close",
        "on": "on",
        "off": "off",
    }.get(raw, "")


def valve_service_on_raw_state(property_cfg: dict | None, valve_id: str | None = None) -> str:
    """Return which raw device state means `Water On` for this water cutoff."""
    cfg = property_cfg or {}
    did = str(valve_id or "").strip()

    raw_map = cfg.get("water_cutoff_service_on_map") or cfg.get("water_valve_service_on_map") or {}
    if isinstance(raw_map, dict) and did:
        mapped = _norm(raw_map.get(did))
        if mapped in {"open", "closed", "on", "off"}:
            return mapped

    fallback = _norm(
        cfg.get("water_cutoff_service_on_state")
        or cfg.get("water_valve_service_on_state")
        or "open"
    )
    return fallback if fallback in {"open", "closed", "on", "off"} else "open"


def valve_service_state(raw_state: str | None,
                        property_cfg: dict | None = None,
                        valve_id: str | None = None) -> dict:
    """Decorate a raw water-cutoff state with water-service meaning."""
    raw = _norm(raw_state)
    if raw == "close":
        raw = "closed"
    water_on_raw = valve_service_on_raw_state(property_cfg, valve_id)
    water_off_raw = _paired_raw_state(water_on_raw)

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

    if raw in {"opening", "turning_on", "switching_on"}:
        if raw == "opening":
            turning_on = (water_on_raw == "open")
        else:
            turning_on = (water_on_raw == "on")
        out.update({
            "service_state": "transition_on" if turning_on else "transition_off",
            "status": "warning",
            "state_label": "Turning Water On" if turning_on else "Turning Water Off",
            "can_turn_water_on": not turning_on,
            "can_turn_water_off": turning_on,
        })
        return out

    if raw in {"closing", "turning_off", "switching_off"}:
        if raw == "closing":
            turning_on = (water_on_raw == "closed")
        else:
            turning_on = (water_on_raw == "off")
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
    """Map `on/off` service commands to raw Hubitat device commands."""
    cmd = _norm(action)
    if cmd in {"open", "close", "on", "off"}:
        if cmd in {"open", "close"}:
            return cmd
        water_on_raw = valve_service_on_raw_state(property_cfg, valve_id)
        water_off_raw = _paired_raw_state(water_on_raw)
        return _raw_state_to_command(water_on_raw if cmd == "on" else water_off_raw)

    return ""
