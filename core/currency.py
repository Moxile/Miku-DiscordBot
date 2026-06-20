"""Per-guild currency name/emoji.

The currency name and emoji default to the global values in `config.py` but can be
overridden per guild. Configured values live in the `guild_currency` table and are cached
in-memory on the bot (`MikuBot._currency_cache`); `bot.get_currency(guild_id)` is the
synchronous accessor used at every display site.
"""

from __future__ import annotations

import re
from typing import NamedTuple

from discord.ext import commands

from config import MAIN_CURRENCY_EMOJI, CURRENCY_NAME

# Matches a Discord custom emote: <:name:id> or <a:name:id> (animated).
CUSTOM_EMOJI_RE = re.compile(r"<(a)?:([A-Za-z0-9_~]+):(\d+)>")

MAX_EMOJI_LEN = 32


class Currency(NamedTuple):
    name: str
    emoji: str


DEFAULT_CURRENCY = Currency(name=CURRENCY_NAME, emoji=MAIN_CURRENCY_EMOJI)


def validate_emoji(bot: commands.Bot, raw: str) -> str | None:
    """Normalize a user-supplied emoji into a ready-to-display string.

    For a custom emote the bot can render (i.e. from a guild it shares), returns the
    canonical ``<:name:id>`` / ``<a:name:id>`` string. For a unicode emoji, returns it
    as-is (length-capped). Returns ``None`` if the input is empty, too long, or a custom
    emote the bot can't access.
    """
    raw = raw.strip()
    if not raw:
        return None

    match = CUSTOM_EMOJI_RE.fullmatch(raw)
    if match:
        animated = match.group(1) == "a"
        name = match.group(2)
        emoji_id = int(match.group(3))
        if not any(e.id == emoji_id for e in bot.emojis):
            return None
        prefix = "a" if animated else ""
        return f"<{prefix}:{name}:{emoji_id}>"

    if len(raw) > MAX_EMOJI_LEN:
        return None
    return raw
