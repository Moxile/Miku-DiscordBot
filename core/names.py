from __future__ import annotations


def format_name(member, guild=None, *, fallback=None):
    """Format a member/user for display as "username (display name)".

    Falls back to bare username if there's no display name to show, and to
    `fallback` if `member` is None (e.g. they left the server).
    """
    if member is None:
        return fallback
    display_name = getattr(member, "display_name", None)
    if display_name and display_name != member.name:
        return f"{member.name} ({display_name})"
    return member.name
