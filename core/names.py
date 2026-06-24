from __future__ import annotations


def format_name(member, guild=None, *, fallback=None):
    """Format a member/user for display as "username (servername)".

    Falls back to bare username if no guild is available, and to
    `fallback` if `member` is None (e.g. they left the server).
    """
    if member is None:
        return fallback
    g = guild or getattr(member, "guild", None)
    if g is not None:
        return f"{member.name} ({g.name})"
    return member.name
