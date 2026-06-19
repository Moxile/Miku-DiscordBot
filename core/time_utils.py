from __future__ import annotations
import re
from datetime import timedelta


def parse_duration(text: str) -> timedelta | None:
    """Translate a duration string like '10s', '5m', '1h', '2d' into a timedelta."""
    match = re.fullmatch(r"(\d+)([smhd])", text.lower())
    if not match:
        return None
    value, unit = int(match.group(1)), match.group(2)
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
    return timedelta(**{units[unit]: value})


def humanize_duration(seconds: int, *, short: bool = False) -> str:
    """Render a number of seconds as a human-readable duration.

    short=False -> '1 day, 2 hours'   short=True -> '1d 2h'
    """
    total = max(0, int(seconds))
    parts = []
    for name, abbr, size in (
        ("day", "d", 86400),
        ("hour", "h", 3600),
        ("minute", "m", 60),
        ("second", "s", 1),
    ):
        val = total // size
        total %= size
        if val:
            parts.append(f"{val}{abbr}" if short else f"{val} {name}{'s' if val != 1 else ''}")
    if not parts:
        return "0s" if short else "0 seconds"
    return " ".join(parts) if short else ", ".join(parts)
