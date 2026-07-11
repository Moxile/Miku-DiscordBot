import asyncpg

from core.db import Conn


async def create_bet(conn: Conn, guild_id: int, channel_id: int, host_id: int,
                     description: str, odds: float, min_stake: int, max_stake: int,
                     pool: int, is_multi: bool = False):
    return await conn.fetchrow(
        """INSERT INTO bets (guild_id, channel_id, host_id, description, odds,
                             min_stake, max_stake, pool, pool_remaining, is_multi)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9) RETURNING *""",
        guild_id, channel_id, host_id, description, odds, min_stake, max_stake, pool, is_multi,
    )


async def add_bet_option(conn: Conn, bet_id: int, idx: int, label: str, odds: float):
    return await conn.fetchrow(
        """INSERT INTO bet_options (bet_id, idx, label, odds)
           VALUES ($1, $2, $3, $4) RETURNING *""",
        bet_id, idx, label, odds,
    )


async def get_bet_options(conn: Conn, bet_id: int):
    return await conn.fetch(
        "SELECT * FROM bet_options WHERE bet_id = $1 ORDER BY idx",
        bet_id,
    )


async def get_bet_option_by_idx(conn: Conn, bet_id: int, idx: int):
    return await conn.fetchrow(
        "SELECT * FROM bet_options WHERE bet_id = $1 AND idx = $2",
        bet_id, idx,
    )


async def get_user_take(conn: Conn, bet_id: int, user_id: int):
    """A user's existing take on this bet, if any (multi-option bets allow only one)."""
    return await conn.fetchrow(
        "SELECT * FROM bet_takes WHERE bet_id = $1 AND user_id = $2",
        bet_id, user_id,
    )


async def get_bet(conn: Conn, bet_id: int):
    return await conn.fetchrow("SELECT * FROM bets WHERE id = $1", bet_id)


async def lock_bet(conn: asyncpg.Connection, bet_id: int):
    """Row-lock the bet for a transaction."""
    return await conn.fetchrow("SELECT * FROM bets WHERE id = $1 FOR UPDATE", bet_id)


async def get_active_bets(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM bets WHERE guild_id = $1 AND status = 'open' ORDER BY id",
        guild_id,
    )


async def get_bet_takes(conn: Conn, bet_id: int):
    return await conn.fetch(
        "SELECT * FROM bet_takes WHERE bet_id = $1 ORDER BY placed_at",
        bet_id,
    )


async def add_bet_take(conn: Conn, bet_id: int, user_id: int, stake: int, liability: int,
                       option_id: int = None):
    return await conn.fetchrow(
        """INSERT INTO bet_takes (bet_id, option_id, user_id, stake, liability)
           VALUES ($1, $2, $3, $4, $5) RETURNING *""",
        bet_id, option_id, user_id, stake, liability,
    )


async def decrement_bet_pool(conn: Conn, bet_id: int, amount: int):
    return await conn.fetchrow(
        """UPDATE bets SET pool_remaining = pool_remaining - $2
           WHERE id = $1 RETURNING pool_remaining""",
        bet_id, amount,
    )


async def set_bet_status(conn: Conn, bet_id: int, status: str):
    await conn.execute(
        "UPDATE bets SET status = $2, closed_at = NOW() WHERE id = $1",
        bet_id, status,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a member's bet takes when they leave/are removed from the guild.

    bet_takes has no guild_id, so scope through the parent bet. Bets they hosted
    (bets.host_id) are left intact since other members may have taken them.
    """
    await conn.execute(
        """DELETE FROM bet_takes
           WHERE user_id = $2
             AND bet_id IN (SELECT id FROM bets WHERE guild_id = $1)""",
        guild_id, user_id,
    )
