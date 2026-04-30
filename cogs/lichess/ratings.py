from __future__ import annotations

from typing import Dict, List, Optional

import asyncpg
import discord

from config import LICHESS_RATING_ROLE_DEFAULTS, RATING_SEPARATOR_ROLE
from cogs.lichess.db import get_all_rating_roles


def compute_tier(rating: int, min_rating: int, step: int, max_rating: int) -> Optional[int]:
    if rating < min_rating:
        return None
    tier = ((rating - min_rating) // step) * step + min_rating
    return min(tier, max_rating)


async def sync_member(
    guild: discord.Guild,
    member: discord.Member,
    ratings_rows: List[asyncpg.Record],
    pool: asyncpg.Pool,
) -> None:
    if not guild.me.guild_permissions.manage_roles:
        return

    ratings_by_variant: Dict[str, asyncpg.Record] = {r["variant"]: r for r in ratings_rows}

    async with pool.acquire() as conn:
        all_role_rows = await get_all_rating_roles(conn, guild.id)
        config_rows = await conn.fetch(
            "SELECT * FROM lichess_rating_role_config WHERE guild_id = $1", guild.id
        )

    configs: Dict[str, dict] = {}
    for row in config_rows:
        configs[row["variant"]] = {
            "min": row["min_rating"],
            "step": row["step"],
            "max": row["max_rating"],
            "enabled": row["enabled"],
        }

    roles_by_variant: Dict[str, Dict[int, int]] = {}
    for row in all_role_rows:
        roles_by_variant.setdefault(row["variant"], {})[row["tier"]] = row["role_id"]

    separator_role = discord.utils.get(guild.roles, name=RATING_SEPARATOR_ROLE)
    to_add: List[discord.Role] = []
    to_remove: List[discord.Role] = []
    has_any_rating_role = False

    for variant, tier_map in roles_by_variant.items():
        defaults = LICHESS_RATING_ROLE_DEFAULTS.get(
            variant, {"min": 2000, "step": 100, "max": 2700, "enabled": True}
        )
        cfg = configs.get(variant, defaults)
        min_r, step, max_r, enabled = cfg["min"], cfg["step"], cfg["max"], cfg["enabled"]

        target_role_id: Optional[int] = None
        rating_row = ratings_by_variant.get(variant)
        if enabled and rating_row is not None and not rating_row["prov"]:
            tier = compute_tier(rating_row["rating"], min_r, step, max_r)
            if tier is not None:
                target_role_id = tier_map.get(tier)

        for tier, role_id in tier_map.items():
            role = guild.get_role(role_id)
            if role is None or role >= guild.me.top_role:
                continue
            if role_id == target_role_id:
                has_any_rating_role = True
                if role not in member.roles:
                    to_add.append(role)
            else:
                if role in member.roles:
                    to_remove.append(role)

    if separator_role and separator_role < guild.me.top_role:
        if has_any_rating_role and separator_role not in member.roles:
            to_add.append(separator_role)
        elif not has_any_rating_role and separator_role in member.roles:
            to_remove.append(separator_role)

    try:
        if to_remove:
            await member.remove_roles(*to_remove, reason="Lichess rating role sync")
        if to_add:
            await member.add_roles(*to_add, reason="Lichess rating role sync")
    except discord.Forbidden:
        pass
