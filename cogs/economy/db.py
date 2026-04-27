import asyncpg

from core.db import Conn


async def ensure_wallet(conn: Conn, guild_id: int, user_id: int) -> asyncpg.Record:
    """Returns and ensures a user has a wallet (creates if not exists)."""
    await conn.execute(
        "INSERT INTO balances (guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        guild_id, user_id,
    )
    return await conn.fetchrow(
        "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def update_wallet(conn: Conn, guild_id: int, user_id: int, amount: int) -> int:
    row = await conn.fetchrow(
        "UPDATE balances SET wallet = wallet + $3 WHERE guild_id = $1 AND user_id = $2 RETURNING wallet",
        guild_id, user_id, amount,
    )
    return row["wallet"]


async def update_bank(conn: Conn, guild_id: int, user_id: int, amount: int) -> int:
    row = await conn.fetchrow(
        "UPDATE balances SET bank = bank + $3 WHERE guild_id = $1 AND user_id = $2 RETURNING bank",
        guild_id, user_id, amount,
    )
    return row["bank"]


async def add_transaction(conn: Conn, guild_id: int, user_id: int, amount: int, tx_type: str, description: str = None):
    await conn.execute(
        "INSERT INTO transactions (guild_id, user_id, amount, tx_type, description) VALUES ($1, $2, $3, $4, $5)",
        guild_id, user_id, amount, tx_type, description,
    )


async def lock_wallet(conn: asyncpg.Connection, guild_id: int, user_id: int) -> asyncpg.Record:
    """Lock and return the user's balance row. Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2 FOR UPDATE",
        guild_id, user_id,
    )
