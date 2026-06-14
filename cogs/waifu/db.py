import asyncpg

from core.db import Conn


async def ensure_waifu(conn: Conn, guild_id: int, user_id: int) -> asyncpg.Record:
    await conn.execute(
        "INSERT INTO waifus (guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        guild_id, user_id,
    )
    return await conn.fetchrow(
        "SELECT * FROM waifus WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def get_waifu(conn: Conn, guild_id: int, user_id: int) -> asyncpg.Record:
    return await conn.fetchrow(
        "SELECT * FROM waifus WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def get_harem(conn: Conn, guild_id: int, owner_id: int) -> list:
    return await conn.fetch(
        "SELECT * FROM waifus WHERE guild_id = $1 AND owner_id = $2 ORDER BY value DESC",
        guild_id, owner_id,
    )


async def set_waifu_owner(conn: Conn, guild_id: int, user_id: int,
                           new_owner_id: int, new_value: int):
    await conn.execute(
        """UPDATE waifus SET owner_id = $3, value = $4, last_bought_at = NOW()
           WHERE guild_id = $1 AND user_id = $2""",
        guild_id, user_id, new_owner_id, new_value,
    )


async def set_engagement(conn: Conn, guild_id: int, user_id: int):
    """Set engaged_since to NOW() if not already set."""
    await conn.execute(
        """UPDATE waifus SET engaged_since = NOW()
           WHERE guild_id = $1 AND user_id = $2 AND engaged_since IS NULL""",
        guild_id, user_id,
    )


async def set_marriage(conn: Conn, guild_id: int, user_a: int, user_b: int):
    """Marry two users: set spouse_id on both."""
    await conn.execute(
        "UPDATE waifus SET spouse_id = $3 WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_a, user_b,
    )
    await conn.execute(
        "UPDATE waifus SET spouse_id = $3 WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_b, user_a,
    )


async def dissolve_marriage(conn: Conn, guild_id: int, user_a: int, user_b: int):
    """Divorce: clear spouse_id and engaged_since on both."""
    for uid in (user_a, user_b):
        await conn.execute(
            "UPDATE waifus SET spouse_id = NULL, engaged_since = NULL WHERE guild_id = $1 AND user_id = $2",
            guild_id, uid,
        )


async def remove_member_waifus(conn: Conn, guild_id: int, user_id: int):
    """Clean up waifu data when a member leaves/is removed from the guild.

    Releases the waifus they owned (so they leave the harem leaderboard and become
    claimable again), dissolves any marriage they were in, then deletes the row
    representing them as a claimable waifu.
    """
    await conn.execute(
        "UPDATE waifus SET owner_id = NULL WHERE guild_id = $1 AND owner_id = $2",
        guild_id, user_id,
    )
    await conn.execute(
        "UPDATE waifus SET spouse_id = NULL, engaged_since = NULL "
        "WHERE guild_id = $1 AND spouse_id = $2",
        guild_id, user_id,
    )
    await conn.execute(
        "DELETE FROM waifus WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def decay_waifu_values(conn: Conn, base_value: int, decay_rate: float):
    """Decay all waifu values above base toward base by decay_rate percent of the excess."""
    await conn.execute(
        """UPDATE waifus
           SET value = GREATEST($1, value - FLOOR((value - $1) * $2)::BIGINT)
           WHERE value > $1
             AND last_bought_at IS NOT NULL
             AND last_bought_at < NOW() - INTERVAL '24 hours'""",
        base_value, decay_rate,
    )
