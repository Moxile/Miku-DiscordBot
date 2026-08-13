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
    await _clear_stale_engagement(conn, guild_id, user_id)
    await conn.execute(
        """UPDATE waifus SET owner_id = $3, value = $4, last_bought_at = NOW()
           WHERE guild_id = $1 AND user_id = $2""",
        guild_id, user_id, new_owner_id, new_value,
    )


async def _clear_stale_engagement(conn: Conn, guild_id: int, user_id: int):
    """user_id's owner_id is about to change, which breaks mutual ownership with
    whoever they're currently engaged to. There's no separate "engaged to" column —
    engagement is only ever inferred from mutual owner_id — so clear engaged_since
    on both sides now, before it goes stale and misattributes to the new owner."""
    row = await get_waifu(conn, guild_id, user_id)
    if not row or row["engaged_since"] is None:
        return
    partner_id = row["owner_id"]
    if partner_id is None:
        await conn.execute(
            "UPDATE waifus SET engaged_since = NULL WHERE guild_id = $1 AND user_id = $2",
            guild_id, user_id,
        )
    else:
        await conn.execute(
            "UPDATE waifus SET engaged_since = NULL WHERE guild_id = $1 AND user_id IN ($2, $3)",
            guild_id, user_id, partner_id,
        )


async def set_engagement(conn: Conn, guild_id: int, user_id: int):
    """Set engaged_since to NOW() if not already set."""
    await conn.execute(
        """UPDATE waifus SET engaged_since = NOW()
           WHERE guild_id = $1 AND user_id = $2 AND engaged_since IS NULL""",
        guild_id, user_id,
    )


async def engage_if_mutual(conn: Conn, guild_id: int, user_a: int, user_b: int) -> bool:
    """If user_a and user_b now own each other (and neither is married), mark both
    engaged. Returns True if they are engaged as a result. Safe to call on either
    the buy or the gift path."""
    row_a = await get_waifu(conn, guild_id, user_a)
    row_b = await get_waifu(conn, guild_id, user_b)
    mutual = (
        row_a and row_b
        and row_a["owner_id"] == user_b
        and row_b["owner_id"] == user_a
        and row_a["spouse_id"] is None
        and row_b["spouse_id"] is None
    )
    if not mutual:
        return False
    await set_engagement(conn, guild_id, user_a)
    await set_engagement(conn, guild_id, user_b)
    return True


async def set_gifted(conn: Conn, guild_id: int, user_id: int):
    """Record that this waifu was just gifted money by their owner (pauses decay)."""
    await conn.execute(
        "UPDATE waifus SET last_gifted_at = NOW() WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def record_beg(conn: Conn, guild_id: int, user_id: int):
    """Record that this waifu was just begged from by their owner (starts the cooldown)."""
    await conn.execute(
        "UPDATE waifus SET last_begged_at = NOW() WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def set_marriage(conn: Conn, guild_id: int, user_a: int, user_b: int,
                        value_step: float) -> tuple:
    """Marry two users: set spouse_id on both, and step each one's value up by
    value_step — the same jump a normal buy gives. Returns (new_value_a, new_value_b)."""
    row_a = await get_waifu(conn, guild_id, user_a)
    row_b = await get_waifu(conn, guild_id, user_b)
    new_value_a = int(row_a["value"] * value_step)
    new_value_b = int(row_b["value"] * value_step)
    await conn.execute(
        "UPDATE waifus SET spouse_id = $3, value = $4 WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_a, user_b, new_value_a,
    )
    await conn.execute(
        "UPDATE waifus SET spouse_id = $3, value = $4 WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_b, user_a, new_value_b,
    )
    return new_value_a, new_value_b


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
    """Decay waifu values above base toward base by decay_rate percent of the excess.

    A waifu is spared while there has been recent activity: either they were bought
    in the last 24h, or their owner gifted them money in the last 24h. Owners keep a
    waifu's value up by gifting them daily (see the money_gift listener / WAIFU_GIFT_RATE).
    """
    await conn.execute(
        """UPDATE waifus
           SET value = GREATEST($1, value - FLOOR((value - $1) * $2)::BIGINT)
           WHERE value > $1
             AND (last_bought_at IS NULL OR last_bought_at < NOW() - INTERVAL '24 hours')
             AND (last_gifted_at IS NULL OR last_gifted_at < NOW() - INTERVAL '24 hours')""",
        base_value, decay_rate,
    )
