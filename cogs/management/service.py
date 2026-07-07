from __future__ import annotations

"""Management service: cog/channel/currency management for admin pages."""

from dataclasses import dataclass

import discord

from core.currency import Currency, validate_emoji
from core.errors import UserError


@dataclass
class CurrencyInfo:
    """Current currency settings."""
    name: str
    emoji: str


async def list_disabled_cogs(pool, guild_id: int) -> list[str]:
    """List all disabled cogs in a guild."""
    rows = await pool.fetch(
        "SELECT cog_name FROM disabled_cogs WHERE guild_id = $1",
        guild_id,
    )
    return [r["cog_name"] for r in rows]


async def disable_cog(pool, guild_id: int, cog_name: str, disabled_cache: dict) -> None:
    """Disable a cog in a guild. Raises UserError if invalid."""
    if cog_name == "Management":
        raise UserError("The Management cog cannot be disabled.")
    await pool.execute(
        "INSERT INTO disabled_cogs (guild_id, cog_name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        guild_id, cog_name,
    )
    disabled_cache.pop(guild_id, None)


async def enable_cog(pool, guild_id: int, cog_name: str, disabled_cache: dict) -> None:
    """Re-enable a cog in a guild."""
    await pool.execute(
        "DELETE FROM disabled_cogs WHERE guild_id = $1 AND cog_name = $2",
        guild_id, cog_name,
    )
    disabled_cache.pop(guild_id, None)


async def list_ignored_channels(pool, guild_id: int) -> list[int]:
    """List all ignored channel IDs in a guild."""
    rows = await pool.fetch(
        "SELECT channel_id FROM ignored_channels WHERE guild_id = $1",
        guild_id,
    )
    return [r["channel_id"] for r in rows]


async def ignore_channel(pool, guild_id: int, channel_id: int, ignored_cache: dict) -> None:
    """Ignore a channel in a guild."""
    await pool.execute(
        "INSERT INTO ignored_channels (guild_id, channel_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        guild_id, channel_id,
    )
    ignored_cache.pop(guild_id, None)


async def unignore_channel(pool, guild_id: int, channel_id: int, ignored_cache: dict) -> None:
    """Un-ignore a channel in a guild."""
    await pool.execute(
        "DELETE FROM ignored_channels WHERE guild_id = $1 AND channel_id = $2",
        guild_id, channel_id,
    )
    ignored_cache.pop(guild_id, None)


async def get_currency(pool, guild_id: int) -> CurrencyInfo:
    """Fetch current currency settings for a guild."""
    row = await pool.fetchrow(
        "SELECT name, emoji FROM guild_currency WHERE guild_id = $1",
        guild_id,
    )
    if row:
        return CurrencyInfo(name=row["name"], emoji=row["emoji"])
    return CurrencyInfo(name="Coins", emoji="🪙")


async def set_currency(pool, bot, guild_id: int, emoji_str: str, name: str, currency_cache: dict) -> None:
    """Set currency for a guild. Raises UserError on invalid emoji."""
    stored_emoji = validate_emoji(bot, emoji_str)
    if stored_emoji is None:
        raise UserError("That emoji can't be used. Use a unicode emoji or a custom emote I'm in.")

    name = name.strip()
    if not name:
        raise UserError("Currency name cannot be empty.")

    await pool.execute(
        """INSERT INTO guild_currency (guild_id, name, emoji)
           VALUES ($1, $2, $3)
           ON CONFLICT (guild_id) DO UPDATE SET name = EXCLUDED.name, emoji = EXCLUDED.emoji""",
        guild_id, name, stored_emoji,
    )
    currency_cache[guild_id] = Currency(name, stored_emoji)


async def reset_currency(pool, guild_id: int, currency_cache: dict) -> None:
    """Reset currency to default for a guild."""
    await pool.execute(
        "DELETE FROM guild_currency WHERE guild_id = $1",
        guild_id,
    )
    currency_cache.pop(guild_id, None)
