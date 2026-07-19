from __future__ import annotations

"""Profile rendering shared by .profileinfo (cog.py) and the Miku Menu (ui.py):
one function that fetches everything and returns the embed + chart file."""

from datetime import datetime, timezone

import discord

from cogs.market.chart import render_price_chart
from core.errors import UserError
from core.names import format_name
from core.time_utils import parse_duration, humanize_duration

from . import db as profile_db

# Friendlier labels for the common shorthands; anything else falls back to a humanized duration.
PERIOD_LABELS = {"1d": "Past 24 hours", "7d": "Past 7 days", "30d": "Past 30 days", "90d": "Past 90 days"}


def resolve_period(period: str | None):
    """Turn a raw period argument into (since, history_limit, period_label).
    Raises UserError if it can't be parsed."""
    if period is None:
        return None, profile_db.NET_WORTH_HISTORY_LIMIT, "Recent activity"
    if period.lower() in ("all", "max"):
        return None, None, "All time"
    delta = parse_duration(period.lower())
    if delta is None:
        raise UserError(f"Invalid period `{period}`. Try something like `7d`, `30d`, `90d`, or `all`.")
    since = datetime.now(timezone.utc) - delta
    label = PERIOD_LABELS.get(period.lower(), f"Past {humanize_duration(int(delta.total_seconds()))}")
    return since, None, label


async def build_profile(bot, guild: discord.Guild, member: discord.Member,
                        period: str | None = None) -> tuple[discord.Embed, discord.File]:
    """The full profile card: stats embed + wallet+bank history chart."""
    since, history_limit, period_label = resolve_period(period)
    guild_id, user_id = guild.id, member.id
    cur = bot.get_currency(guild_id)

    async with bot.pool.acquire() as conn:
        wallet, bank = await profile_db.get_balance(conn, guild_id, user_id)
        gambling = await profile_db.get_gambling_totals(conn, guild_id, user_id)
        portfolio_value, holding_count = await profile_db.get_portfolio_value(conn, guild_id, user_id)
        inventory = await profile_db.get_inventory_totals(conn, guild_id, user_id)
        harem = await profile_db.get_harem_value(conn, guild_id, user_id)
        points = await profile_db.get_net_worth_points(
            conn, guild_id, user_id, wallet, bank, since=since, limit=history_limit,
        )

    net_worth = wallet + bank + portfolio_value + harem["total"]
    name = format_name(member, guild)

    embed = discord.Embed(
        title=f"{name}'s Profile",
        color=discord.Color.from_rgb(108, 92, 231),
    )
    embed.set_thumbnail(url=member.display_avatar.url)

    embed.add_field(name="Wallet", value=f"{wallet:,}{cur.emoji}")
    embed.add_field(name="Bank", value=f"{bank:,}{cur.emoji}")
    embed.add_field(name="Net Worth", value=f"{net_worth:,}{cur.emoji}")

    won, lost = gambling["won"], gambling["lost"]
    net_gambling = won - lost
    sign = "+" if net_gambling >= 0 else ""
    embed.add_field(
        name="Gambling",
        value=(f"Wagered: {gambling['wagered']:,}{cur.emoji}\n"
               f"Games played: {gambling['games']:,}\n"
               f"Net: {sign}{net_gambling:,}{cur.emoji}"),
        inline=True,
    )
    embed.add_field(
        name="Holdings",
        value=(f"Portfolio: {portfolio_value:,}{cur.emoji} ({holding_count} stocks)\n"
               f"Inventory: {inventory['distinct_items']} items\n"
               f"Harem: {harem['count']} waifus ({harem['total']:,}{cur.emoji})"),
        inline=True,
    )
    buf = render_price_chart(f"{name}'s Wallet + Bank", points, period_label=period_label)
    file = discord.File(buf, filename="profile.png")
    embed.set_image(url="attachment://profile.png")
    return embed, file
