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


async def get_shop_purchases(conn: Conn, guild_id: int) -> list:
    """All shop purchases for a guild, newest first (who/what/price/when live in one row)."""
    return await conn.fetch(
        """SELECT user_id, amount, description, created_at FROM transactions
           WHERE guild_id = $1 AND tx_type = 'shop_buy'
           ORDER BY created_at DESC""",
        guild_id,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete all economy data for a member who left/was removed from the guild.

    Deleting the balance cascades to transactions (see economy.schema MIGRATIONS).
    """
    await conn.execute(
        "DELETE FROM locked_users WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
    await conn.execute(
        "DELETE FROM cooldowns WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
    await conn.execute(
        "DELETE FROM balances WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def set_salary_role(conn: Conn, guild_id: int, role_id: int, interval_seconds: int, amount: int):
    """Bind (or rebind) a role to a recurring salary collectable via .collect."""
    await conn.execute(
        """INSERT INTO salary_roles (guild_id, role_id, interval_seconds, amount)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, role_id)
           DO UPDATE SET interval_seconds = EXCLUDED.interval_seconds, amount = EXCLUDED.amount""",
        guild_id, role_id, interval_seconds, amount,
    )


async def remove_salary_role(conn: Conn, guild_id: int, role_id: int) -> str:
    """Remove a role's salary binding. Returns the asyncpg status string (e.g. 'DELETE 1')."""
    return await conn.execute(
        "DELETE FROM salary_roles WHERE guild_id = $1 AND role_id = $2",
        guild_id, role_id,
    )


async def list_salary_roles(conn: Conn, guild_id: int) -> list:
    """All salary bindings for a guild."""
    return await conn.fetch(
        "SELECT role_id, interval_seconds, amount FROM salary_roles WHERE guild_id = $1",
        guild_id,
    )


async def get_salary_roles_for(conn: Conn, guild_id: int, role_ids: list) -> list:
    """Salary bindings whose role is among role_ids (the roles a member holds)."""
    return await conn.fetch(
        "SELECT role_id, interval_seconds, amount FROM salary_roles WHERE guild_id = $1 AND role_id = ANY($2)",
        guild_id, role_ids,
    )


async def lock_wallet(conn: asyncpg.Connection, guild_id: int, user_id: int) -> asyncpg.Record:
    """Lock and return the user's balance row. Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2 FOR UPDATE",
        guild_id, user_id,
    )
