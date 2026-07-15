from __future__ import annotations

import asyncpg

from core.db import Conn


# ── Opening / reading positions ──

async def create_position(conn: Conn, guild_id: int, user_id: int, symbol: str, opt_type: str,
                          strike: float, expiry, contracts: int, multiplier: int,
                          entry_spot: float, iv: float, premium_paid: int) -> asyncpg.Record:
    return await conn.fetchrow(
        """INSERT INTO option_positions
               (guild_id, user_id, symbol, opt_type, strike, expiry, contracts,
                multiplier, entry_spot, iv, premium_paid)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
           RETURNING *""",
        guild_id, user_id, symbol, opt_type, strike, expiry, contracts,
        multiplier, entry_spot, iv, premium_paid,
    )


async def get_open_position(conn: Conn, guild_id: int, user_id: int, position_id: int):
    return await conn.fetchrow(
        """SELECT p.*, s.name, s.lot_size
           FROM option_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.id = $1 AND p.guild_id = $2 AND p.user_id = $3 AND p.status = 'open'""",
        position_id, guild_id, user_id,
    )


async def lock_position(conn: asyncpg.Connection, position_id: int):
    """Lock and return a position row. Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM option_positions WHERE id = $1 FOR UPDATE",
        position_id,
    )


async def get_user_open_positions(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        """SELECT p.*, s.name, s.lot_size
           FROM option_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.guild_id = $1 AND p.user_id = $2 AND p.status = 'open'
           ORDER BY p.expiry""",
        guild_id, user_id,
    )


async def get_expired_open_positions(conn: Conn, now):
    """Open positions at or past their expiry — the settlement loop's working set."""
    return await conn.fetch(
        """SELECT p.*, s.name, s.lot_size
           FROM option_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.status = 'open' AND p.expiry <= $1
           ORDER BY p.symbol""",
        now,
    )


async def get_open_positions_for_symbol(conn: asyncpg.Connection, guild_id: int, symbol: str):
    """Open positions on one guild's symbol, locked FOR UPDATE — used to force-settle
    on delisting. Must be called within a transaction."""
    return await conn.fetch(
        """SELECT * FROM option_positions
           WHERE guild_id = $1 AND symbol = $2 AND status = 'open'
           FOR UPDATE""",
        guild_id, symbol,
    )


async def get_position_history(conn: Conn, guild_id: int, user_id: int, limit: int = 50):
    return await conn.fetch(
        """SELECT p.*, s.name
           FROM option_positions p
           JOIN real_symbols s ON s.symbol = p.symbol
           WHERE p.guild_id = $1 AND p.user_id = $2 AND p.status <> 'open'
           ORDER BY p.closed_at DESC
           LIMIT $3""",
        guild_id, user_id, limit,
    )


# ── Mutating positions ──

async def settle_position(conn: Conn, position_id: int, settle_spot: float,
                          payout: int, realized_pl: int, status: str):
    await conn.execute(
        """UPDATE option_positions
           SET status = $2, settle_spot = $3, payout = $4, realized_pl = $5, closed_at = NOW()
           WHERE id = $1""",
        position_id, status, settle_spot, payout, realized_pl,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a leaving member's option positions (open contracts simply vanish,
    mirroring how RealStocks discards a leaving member's holdings)."""
    await conn.execute(
        "DELETE FROM option_positions WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
