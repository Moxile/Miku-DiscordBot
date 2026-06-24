from __future__ import annotations

from typing import Dict, List

import asyncpg
import discord

from config import LICHESS_VARIANTS
from core.names import format_name

VARIANT_EMOJI: Dict[str, str] = {
    "bullet":     "🔫",
    "blitz":      "⚡",
    "rapid":      "🕐",
    "atomic":     "💥",
    "antichess":  "♟️",
    "crazyhouse": "🏠",
}

STYLES: Dict[str, discord.Color] = {
    "default": discord.Color.from_rgb(108, 92, 231),
    "gold":    discord.Color.gold(),
    "chess":   discord.Color.dark_teal(),
}


def build_profile_embed(
    member: discord.Member,
    account_row: asyncpg.Record,
    ratings_rows: List[asyncpg.Record],
    style: str = "default",
) -> discord.Embed:
    color = STYLES.get(style, STYLES["default"])
    ratings_by_variant: Dict[str, asyncpg.Record] = {r["variant"]: r for r in ratings_rows}

    embed = discord.Embed(
        title=f"{format_name(member)}'s Chess Profile",
        color=color,
    )
    embed.set_thumbnail(url=member.display_avatar.url)

    lichess_url = f"https://lichess.org/@/{account_row['lichess_username']}"
    embed.add_field(
        name="Lichess",
        value=f"[{account_row['lichess_username']}]({lichess_url})",
        inline=False,
    )

    lines = []
    for v in LICHESS_VARIANTS:
        key = v["key"]
        name = v["name"]
        emoji = VARIANT_EMOJI.get(key, "")
        row = ratings_by_variant.get(key)
        if row and row["games"] > 0:
            prov = "?" if row["prov"] else ""
            lines.append(f"{emoji} **{name}**: {row['rating']}{prov} ({row['games']} games)")
        else:
            lines.append(f"{emoji} **{name}**: —")

    embed.add_field(name="Ratings", value="\n".join(lines), inline=False)

    if account_row["last_synced_at"]:
        embed.set_footer(
            text=f"Last synced: {account_row['last_synced_at'].strftime('%Y-%m-%d %H:%M UTC')}"
        )

    return embed
