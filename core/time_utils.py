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
