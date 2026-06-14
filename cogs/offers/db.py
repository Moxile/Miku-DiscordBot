import asyncpg

from core.db import Conn


async def create_offer(conn: Conn, guild_id: int, channel_id: int, host_id: int,
                        description: str, odds: float, min_stake: int, max_stake: int,
                        pool: int):
    return await conn.fetchrow(
        """INSERT INTO offers (guild_id, channel_id, host_id, description, odds,
                                min_stake, max_stake, pool, pool_remaining)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8) RETURNING *""",
        guild_id, channel_id, host_id, description, odds, min_stake, max_stake, pool,
    )


async def get_offer(conn: Conn, offer_id: int):
    return await conn.fetchrow("SELECT * FROM offers WHERE id = $1", offer_id)


async def lock_offer(conn: asyncpg.Connection, offer_id: int):
    """Row-lock the offer for a transaction."""
    return await conn.fetchrow("SELECT * FROM offers WHERE id = $1 FOR UPDATE", offer_id)


async def get_active_offers(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM offers WHERE guild_id = $1 AND status = 'open' ORDER BY id",
        guild_id,
    )


async def get_offer_takes(conn: Conn, offer_id: int):
    return await conn.fetch(
        "SELECT * FROM offer_takes WHERE offer_id = $1 ORDER BY placed_at",
        offer_id,
    )


async def add_offer_take(conn: Conn, offer_id: int, user_id: int, stake: int, liability: int):
    return await conn.fetchrow(
        """INSERT INTO offer_takes (offer_id, user_id, stake, liability)
           VALUES ($1, $2, $3, $4) RETURNING *""",
        offer_id, user_id, stake, liability,
    )


async def decrement_offer_pool(conn: Conn, offer_id: int, amount: int):
    return await conn.fetchrow(
        """UPDATE offers SET pool_remaining = pool_remaining - $2
           WHERE id = $1 RETURNING pool_remaining""",
        offer_id, amount,
    )


async def set_offer_status(conn: Conn, offer_id: int, status: str):
    await conn.execute(
        "UPDATE offers SET status = $2, closed_at = NOW() WHERE id = $1",
        offer_id, status,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a member's offer takes when they leave/are removed from the guild.

    offer_takes has no guild_id, so scope through the parent offer. Offers they hosted
    (offers.host_id) are left intact since other members may have taken them.
    """
    await conn.execute(
        """DELETE FROM offer_takes
           WHERE user_id = $2
             AND offer_id IN (SELECT id FROM offers WHERE guild_id = $1)""",
        guild_id, user_id,
    )
