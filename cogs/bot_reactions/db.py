from __future__ import annotations

import asyncpg

from core.db import Conn


async def add_reaction(
    conn: Conn,
    guild_id: int,
    trigger: str,
    response: str,
    role_id: int | None,
    created_by: int,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """INSERT INTO bot_reactions (guild_id, trigger, response, role_id, created_by)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (guild_id, trigger) DO NOTHING
           RETURNING *""",
        guild_id, trigger, response, role_id, created_by,
    )


async def remove_reaction(conn: Conn, guild_id: int, trigger: str) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """DELETE FROM bot_reactions
           WHERE guild_id = $1 AND trigger = $2
           RETURNING *""",
        guild_id, trigger,
    )


async def list_reactions(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        """SELECT * FROM bot_reactions
           WHERE guild_id = $1
           ORDER BY trigger""",
        guild_id,
    )
