from __future__ import annotations

"""Leaderboard logic shared by the .lb command (cog.py) and the Miku Menu
(ui.py): mode dispatch, exclusion filtering, and the page-embed renderer."""

import math

import discord

from cogs.leaderboard import db
from core.errors import UserError
from core.names import format_name

PER_PAGE = 10

# mode key -> (query, title). "emoji" is special-cased (needs the config row).
_MODES = {
    "net": (db.lb_net, "Net Worth Leaderboard"),
    "wallet": (db.lb_wallet, "Wallet Leaderboard"),
    "bank": (db.lb_bank, "Bank Leaderboard"),
    "port": (db.lb_portfolio, "Portfolio Leaderboard"),
    "waifu": (db.lb_waifu, "Harem Value Leaderboard"),
}

MODE_LABELS = {
    "net": "Net Worth",
    "wallet": "Wallet",
    "bank": "Bank",
    "port": "Portfolio",
    "waifu": "Harem Value",
    "emoji": "Reactions",
}


async def get_leaderboard(pool, guild_id: int, mode: str) -> tuple[str, list, str | None]:
    """(title, ranked rows, score_label) for a mode key.

    Rows are filtered to non-excluded members with a positive score.
    score_label is None for currency-scored boards (callers show the guild's
    currency emoji) and the tracked emoji for the reaction board.
    """
    score_label = None
    if mode == "emoji":
        config = await db.get_reaction_config(pool, guild_id)
        if config is None:
            raise UserError("No reaction emoji has been set. An admin can set one with `.lbemoji <emoji>`.")
        rows = await db.lb_reactions(pool, guild_id)
        score_label = config["emoji_display"]
        title = f"{config['emoji_display']} Reaction Leaderboard"
    else:
        query, title = _MODES[mode]
        rows = await query(pool, guild_id)

    excluded = await db.excluded_ids(pool, guild_id)
    rows = [r for r in rows if r["user_id"] not in excluded and (r["score"] or 0) > 0]
    return title, rows, score_label


def max_page(rows: list) -> int:
    return max(0, math.ceil(len(rows) / PER_PAGE) - 1)


def build_lb_embed(guild: discord.Guild, title: str, rows: list, page: int,
                   invoker_id: int, score_label: str) -> discord.Embed:
    """One leaderboard page, PER_PAGE entries starting at page*PER_PAGE."""
    embed = discord.Embed(title=title, color=discord.Color.from_rgb(255, 215, 0))
    if not rows:
        embed.description = "No data yet."
        return embed

    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    start = page * PER_PAGE
    lines = []
    for rank, row in enumerate(rows[start:start + PER_PAGE], start + 1):
        uid = row["user_id"]
        member = guild.get_member(uid)
        name = format_name(member, guild, fallback=f"User {uid}")
        prefix = medals.get(rank, f"`{rank}.`")
        lines.append(f"{prefix} **{name}** — {score_label} {row['score']:,}")
    embed.description = "\n".join(lines)

    invoker_rank = next((i for i, r in enumerate(rows, 1) if r["user_id"] == invoker_id), None)
    footer = f"Page {page + 1}/{max_page(rows) + 1} • {len(rows)} ranked"
    footer += f" • You are #{invoker_rank}" if invoker_rank else " • You are unranked"
    embed.set_footer(text=footer)
    return embed
