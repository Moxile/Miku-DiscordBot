import asyncpg

from core.db import Conn


async def add_reaction_role(conn: Conn, guild_id: int, channel_id: int, message_id: int,
                             emoji: str, is_custom: bool, role_id: int, created_by: int) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO reaction_roles (guild_id, channel_id, message_id, emoji, is_custom, role_id, created_by)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (guild_id, message_id, emoji) DO NOTHING
           RETURNING *""",
        guild_id, channel_id, message_id, emoji, is_custom, role_id, created_by,
    )


async def get_reaction_role(conn: Conn, guild_id: int, message_id: int, emoji: str) -> asyncpg.Record:
    return await conn.fetchrow(
        """SELECT * FROM reaction_roles
           WHERE guild_id = $1 AND message_id = $2 AND emoji = $3""",
        guild_id, message_id, emoji,
    )


async def remove_reaction_role(conn: Conn, guild_id: int, message_id: int, emoji: str) -> asyncpg.Record:
    return await conn.fetchrow(
        """DELETE FROM reaction_roles
           WHERE guild_id = $1 AND message_id = $2 AND emoji = $3
           RETURNING *""",
        guild_id, message_id, emoji,
    )


async def list_reaction_roles_for_message(conn: Conn, guild_id: int, message_id: int) -> list:
    return await conn.fetch(
        """SELECT * FROM reaction_roles
           WHERE guild_id = $1 AND message_id = $2
           ORDER BY created_at""",
        guild_id, message_id,
    )


async def list_reaction_roles_for_guild(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        """SELECT * FROM reaction_roles
           WHERE guild_id = $1
           ORDER BY message_id, created_at""",
        guild_id,
    )


async def clear_reaction_roles_for_message(conn: Conn, guild_id: int, message_id: int) -> int:
    result = await conn.execute(
        "DELETE FROM reaction_roles WHERE guild_id = $1 AND message_id = $2",
        guild_id, message_id,
    )
    try:
        return int(result.split()[-1])
    except (ValueError, IndexError):
        return 0


async def delete_reaction_roles_for_role(conn: Conn, guild_id: int, role_id: int):
    await conn.execute(
        "DELETE FROM reaction_roles WHERE guild_id = $1 AND role_id = $2",
        guild_id, role_id,
    )


async def set_default_role(conn: Conn, guild_id: int, channel_id: int, message_id: int,
                            role_id: int, created_by: int) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO reaction_role_defaults (guild_id, channel_id, message_id, role_id, created_by)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (guild_id, message_id) DO UPDATE
               SET role_id = EXCLUDED.role_id, created_by = EXCLUDED.created_by, created_at = NOW()
           RETURNING *""",
        guild_id, channel_id, message_id, role_id, created_by,
    )


async def get_default_role(conn: Conn, guild_id: int, message_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "SELECT * FROM reaction_role_defaults WHERE guild_id = $1 AND message_id = $2",
        guild_id, message_id,
    )


async def remove_default_role(conn: Conn, guild_id: int, message_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "DELETE FROM reaction_role_defaults WHERE guild_id = $1 AND message_id = $2 RETURNING *",
        guild_id, message_id,
    )


async def delete_default_role_for_role(conn: Conn, guild_id: int, role_id: int):
    await conn.execute(
        "DELETE FROM reaction_role_defaults WHERE guild_id = $1 AND role_id = $2",
        guild_id, role_id,
    )


async def clear_default_role_for_message(conn: Conn, guild_id: int, message_id: int):
    await conn.execute(
        "DELETE FROM reaction_role_defaults WHERE guild_id = $1 AND message_id = $2",
        guild_id, message_id,
    )


async def get_defaults_for_guild(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        "SELECT * FROM reaction_role_defaults WHERE guild_id = $1 ORDER BY message_id",
        guild_id,
    )
