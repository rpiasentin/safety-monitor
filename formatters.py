"""Formatting helpers for dashboard display and notifications."""


def fmt_temp(f: float | None, show_unit: bool = True) -> str:
    if f is None:
        return "—"
    s = f"{f:.1f}°F"
    return s if show_unit else f"{f:.1f}"


def fmt_power(watts: float | None) -> str:
    if watts is None:
        return "—"
    if abs(watts) >= 1000:
        return f"{watts/1000:.2f} kW"
    return f"{watts:.0f} W"


def fmt_voltage(v: float | None) -> str:
    return f"{v:.1f} V" if v is not None else "—"


def fmt_pct(v: float | None) -> str:
    return f"{v:.0f}%" if v is not None else "—"


def temp_status(f: float | None, threshold: float = 40,
                critical: float = 32) -> str:
    """Return 'ok' | 'warning' | 'critical' based on °F."""
    if f is None:
        return "unknown"
    if f < critical:
        return "critical"
    if f < threshold:
        return "warning"
    return "ok"


def battery_status(pct: float | None, low: float = 20,
                   critical: float = 10) -> str:
    """Return 'ok' | 'low' | 'critical'."""
    if pct is None:
        return "unknown"
    if pct < critical:
        return "critical"
    if pct < low:
        return "low"
    return "ok"


def soc_color(pct: float | None) -> str:
    """Tailwind colour class for SOC badge."""
    if pct is None:
        return "bg-gray-400"
    if pct < 10:
        return "bg-red-600"
    if pct < 20:
        return "bg-orange-500"
    if pct < 40:
        return "bg-yellow-400"
    return "bg-green-500"


def temp_color(status: str) -> str:
    return {"ok": "bg-green-500",
            "warning": "bg-yellow-400",
            "critical": "bg-red-600",
            "unknown": "bg-gray-400"}.get(status, "bg-gray-400")


def battery_color(status: str) -> str:
    return {"ok": "bg-green-500",
            "low": "bg-yellow-400",
            "critical": "bg-red-600",
            "unknown": "bg-gray-400"}.get(status, "bg-gray-400")


def ago(iso_ts: str | None) -> str:
    """Human-readable 'X min ago' from an ISO UTC timestamp."""
    if not iso_ts:
        return "never"
    from datetime import datetime, timezone
    try:
        ts = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - ts
        secs = int(delta.total_seconds())
        if secs < 90:
            return f"{secs}s ago"
        if secs < 3600:
            return f"{secs//60}m ago"
        if secs < 86400:
            return f"{secs//3600}h ago"
        return f"{secs//86400}d ago"
    except Exception:
        return iso_ts
