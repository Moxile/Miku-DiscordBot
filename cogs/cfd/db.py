from __future__ import annotations

import asyncpg

from core.db import Conn


# ── Opening / reading positions ──

async def create_position(conn: Conn, guild_id: int, user_id: int, symbol: str, direction: str,
                          notional: int, leverage: int, entry_price: float, margin: int,
                          liquidation_price: float) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO cfd_positions
               (guild_id, user_id, symbol, direction, notional, leverage,
                entry_price, margin, liquidation_price)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
           RETURNING *""",
        guild_id, user_id, symbol, direction, notional, leverage,
        entry_price, margin, liquidation_price,
    )


async def get_open_position(conn: Conn, guild_id: int, user_id: int, position_id: int):
    """A user's open position by id joined with the stock name, or None."""
    return await conn.fetchrow(
        """SELECT p.*, s.name
           FROM cfd_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.id = $1 AND p.guild_id = $2 AND p.user_id = $3 AND p.status = 'open'""",
        position_id, guild_id, user_id,
    )


async def lock_position(conn: asyncpg.Connection, position_id: int):
    """Lock and return a position row. Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM cfd_positions WHERE id = $1 FOR UPDATE",
        position_id,
    )


async def get_user_open_positions(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        """SELECT p.*, s.name
           FROM cfd_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.guild_id = $1 AND p.user_id = $2 AND p.status = 'open'
           ORDER BY p.opened_at""",
        guild_id, user_id,
    )


async def get_all_open_positions(conn: Conn):
    """Every open position across all guilds — the background loop's working set."""
    return await conn.fetch(
        """SELECT p.*, s.name
           FROM cfd_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.status = 'open'
           ORDER BY p.symbol""",
    )


async def get_open_positions_for_symbol(conn: asyncpg.Connection, guild_id: int, symbol: str):
    """Open positions on one guild's symbol, locked FOR UPDATE — used to force-close
    on delisting. Must be called within a transaction."""
    return await conn.fetch(
        """SELECT * FROM cfd_positions
           WHERE guild_id = $1 AND symbol = $2 AND status = 'open'
           FOR UPDATE""",
        guild_id, symbol,
    )


async def get_position_history(conn: Conn, guild_id: int, user_id: int, limit: int = 50):
    """A user's settled positions (closed or liquidated), newest first."""
    return await conn.fetch(
        """SELECT p.*, s.name
           FROM cfd_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.guild_id = $1 AND p.user_id = $2 AND p.status <> 'open'
           ORDER BY p.closed_at DESC
           LIMIT $3""",
        guild_id, user_id, limit,
    )


# ── Mutating positions ──

async def settle_position(conn: Conn, position_id: int, close_price: float,
                          realized_pl: int, status: str):
    """Mark a position closed or liquidated with its outcome."""
    await conn.execute(
        """UPDATE cfd_positions
           SET status = $2, close_price = $3, realized_pl = $4, closed_at = NOW()
           WHERE id = $1""",
        position_id, status, close_price, realized_pl,
    )


async def update_financing(conn: Conn, position_id: int, financing_accrued: int, last_financed_at):
    await conn.execute(
        """UPDATE cfd_positions
           SET financing_accrued = $2, last_financed_at = $3
           WHERE id = $1""",
        position_id, financing_accrued, last_financed_at,
    )


async def set_margin_call_sent(conn: Conn, position_id: int, sent: bool):
    await conn.execute(
        "UPDATE cfd_positions SET margin_call_sent = $2 WHERE id = $1",
        position_id, sent,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a leaving member's CFD positions. Open positions simply vanish, mirroring
    how RealStocks discards a leaving member's holdings (the locked margin is not refunded)."""
    await conn.execute(
        "DELETE FROM cfd_positions WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
