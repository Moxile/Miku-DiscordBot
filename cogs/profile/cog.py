from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands

from core.names import format_name
from core.time_utils import parse_duration, humanize_duration
from cogs.market.chart import render_price_chart

from . import db as profile_db

# Friendlier labels for the common shorthands; anything else falls back to a humanized duration.
PERIOD_LABELS = {"1d": "Past 24 hours", "7d": "Past 7 days", "30d": "Past 30 days", "90d": "Past 90 days"}


class Profile(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    @commands.command(name="profileinfo", aliases=["pinfo", "pi"], extras={"example": ".profileinfo @user 30d"})
    async def profileinfo(self, ctx: commands.Context, member: Optional[discord.Member] = None, period: str = None):
        """Show a player's economic profile: balances, gambling stats, holdings, and a
        wallet+bank history graph.

        `period` widens (or narrows) the graph's time window, e.g. `7d`, `30d`, `90d`, `all`.
        Without it, the graph shows the last 60 balance-changing transactions.
        Usage: .profileinfo [@user] [period]"""
        member = member or ctx.author
        guild_id, user_id = ctx.guild.id, member.id
        cur = self.bot.get_currency(guild_id)

        since = None
        history_limit = profile_db.NET_WORTH_HISTORY_LIMIT
        period_label = "Recent activity"
        if period is not None:
            history_limit = profile_db.NET_WORTH_HISTORY_LIMIT_WINDOWED
            if period.lower() in ("all", "max"):
                period_label = "All time"
            else:
                delta = parse_duration(period.lower())
                if delta is None:
                    await ctx.send(f"Invalid period `{period}`. Try something like `7d`, `30d`, `90d`, or `all`.")
                    return
                since = datetime.now(timezone.utc) - delta
                period_label = PERIOD_LABELS.get(period.lower(),
                                                  f"Past {humanize_duration(int(delta.total_seconds()))}")

        async with self.pool.acquire() as conn:
            wallet, bank = await profile_db.get_balance(conn, guild_id, user_id)
            gambling = await profile_db.get_gambling_totals(conn, guild_id, user_id)
            portfolio_value, holding_count = await profile_db.get_portfolio_value(conn, guild_id, user_id)
            inventory = await profile_db.get_inventory_totals(conn, guild_id, user_id)
            harem = await profile_db.get_harem_value(conn, guild_id, user_id)
            points = await profile_db.get_net_worth_points(
                conn, guild_id, user_id, wallet, bank, since=since, limit=history_limit,
            )

        net_worth = wallet + bank + portfolio_value + harem["total"]
        name = format_name(member, ctx.guild)

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

        await ctx.send(embed=embed, file=file)
