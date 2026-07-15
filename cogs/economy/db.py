from __future__ import annotations

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
        "DELETE FROM crime_jails WHERE guild_id = $1 AND user_id = $2",
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


# ── Crime jail (prisoner role on a failed .crime) ──

async def get_jail_config(conn: Conn, guild_id: int) -> tuple[int | None, int]:
    """(prisoner_role_id, jail_duration_seconds) for a guild.

    role_id is None when no prisoner role is bound (jail disabled); the duration
    falls back to DEFAULT_JAIL_DURATION when unset."""
    from config import DEFAULT_JAIL_DURATION
    rows = await conn.fetch(
        "SELECT key, value FROM guild_settings WHERE guild_id = $1 AND key = ANY($2)",
        guild_id, ["crime_jail_role", "crime_jail_duration"],
    )
    settings = {r["key"]: r["value"] for r in rows}
    role_id = settings.get("crime_jail_role")
    duration = settings.get("crime_jail_duration")
    return (int(role_id) if role_id else None,
            int(duration) if duration else DEFAULT_JAIL_DURATION)


async def add_jail(conn: Conn, guild_id: int, user_id: int, role_id: int, release_at):
    """Record (or restart) a member's jail sentence. Re-jailing resets the timer."""
    await conn.execute(
        """INSERT INTO crime_jails (guild_id, user_id, role_id, release_at)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, user_id)
           DO UPDATE SET role_id = EXCLUDED.role_id, release_at = EXCLUDED.release_at""",
        guild_id, user_id, role_id, release_at,
    )


async def get_due_jails(conn: Conn, now) -> list:
    """Jail rows whose sentence has expired (release_at <= now) — ready to be freed."""
    return await conn.fetch(
        "SELECT guild_id, user_id, role_id FROM crime_jails WHERE release_at <= $1",
        now,
    )


async def remove_jail(conn: Conn, guild_id: int, user_id: int):
    await conn.execute(
        "DELETE FROM crime_jails WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


async def lock_wallet(conn: asyncpg.Connection, guild_id: int, user_id: int) -> asyncpg.Record:
    """Lock and return the user's balance row. Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2 FOR UPDATE",
        guild_id, user_id,
    )
