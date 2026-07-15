from __future__ import annotations

import asyncpg

from core.db import Conn


# ── Symbols (global — shared lot size and price history across guilds) ──

async def get_symbol(conn: Conn, symbol: str):
    return await conn.fetchrow("SELECT * FROM real_symbols WHERE symbol = $1", symbol)


async def create_symbol(conn: Conn, symbol: str, name: str, lot_size: int):
    return await conn.fetchrow(
        """INSERT INTO real_symbols (symbol, name, lot_size) VALUES ($1, $2, $3)
           ON CONFLICT (symbol) DO NOTHING
           RETURNING *""",
        symbol, name, lot_size,
    )


async def update_symbol_profile(conn: Conn, symbol: str, industry: str | None, domain: str | None,
                                market_cap: float | None, eps: float | None):
    await conn.execute(
        """UPDATE real_symbols
           SET industry = $2, domain = $3, market_cap = $4, eps = $5, profile_updated_at = NOW()
           WHERE symbol = $1""",
        symbol, industry, domain, market_cap, eps,
    )


async def get_stale_profile_symbols(conn: Conn, older_than) -> list[str]:
    """Enabled symbols whose fundamentals were never fetched or predate `older_than`."""
    rows = await conn.fetch(
        """SELECT DISTINCT s.symbol
           FROM real_symbols s
           JOIN guild_real_stocks g ON g.symbol = s.symbol
           WHERE s.profile_updated_at IS NULL OR s.profile_updated_at < $1
           ORDER BY s.symbol""",
        older_than,
    )
    return [r["symbol"] for r in rows]


# ── Per-guild enablement ──

async def enable_stock(conn: Conn, guild_id: int, symbol: str, enabled_by: int):
    await conn.execute(
        """INSERT INTO guild_real_stocks (guild_id, symbol, enabled_by) VALUES ($1, $2, $3)
           ON CONFLICT (guild_id, symbol) DO NOTHING""",
        guild_id, symbol, enabled_by,
    )


async def disable_stock(conn: Conn, guild_id: int, symbol: str):
    """Remove a symbol from a guild (cascades to that guild's holdings and trades)."""
    return await conn.execute(
        "DELETE FROM guild_real_stocks WHERE guild_id = $1 AND symbol = $2",
        guild_id, symbol,
    )


async def get_guild_stock(conn: Conn, guild_id: int, symbol: str):
    """The guild's enablement row joined with the global symbol data, or None."""
    return await conn.fetchrow(
        """SELECT g.*, s.name, s.lot_size, s.added_at, s.industry, s.domain, s.market_cap, s.eps
           FROM guild_real_stocks g
           JOIN real_symbols s ON s.symbol = g.symbol
           WHERE g.guild_id = $1 AND g.symbol = $2""",
        guild_id, symbol,
    )


async def list_guild_stocks(conn: Conn, guild_id: int):
    return await conn.fetch(
        """SELECT g.*, s.name, s.lot_size, s.added_at, s.industry, s.domain, s.market_cap, s.eps
           FROM guild_real_stocks g
           JOIN real_symbols s ON s.symbol = g.symbol
           WHERE g.guild_id = $1
           ORDER BY g.enabled_at""",
        guild_id,
    )


async def distinct_enabled_symbols(conn: Conn) -> list[str]:
    """Symbols enabled in at least one guild — the refresh task's working set."""
    rows = await conn.fetch("SELECT DISTINCT symbol FROM guild_real_stocks ORDER BY symbol")
    return [r["symbol"] for r in rows]


# ── Holdings ──

async def get_holding(conn: Conn, guild_id: int, user_id: int, symbol: str) -> int:
    row = await conn.fetchrow(
        "SELECT quantity FROM real_holdings WHERE guild_id = $1 AND user_id = $2 AND symbol = $3",
        guild_id, user_id, symbol,
    )
    return row["quantity"] if row else 0


async def lock_holding(conn: asyncpg.Connection, guild_id: int, user_id: int, symbol: str) -> int:
    """Lock and return the holding quantity. Must be called within a transaction."""
    row = await conn.fetchrow(
        "SELECT quantity FROM real_holdings WHERE guild_id = $1 AND user_id = $2 AND symbol = $3 FOR UPDATE",
        guild_id, user_id, symbol,
    )
    return row["quantity"] if row else 0


async def update_holding(conn: Conn, guild_id: int, user_id: int, symbol: str, quantity_change: int):
    if quantity_change > 0:
        await conn.execute(
            """INSERT INTO real_holdings (guild_id, user_id, symbol, quantity)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (guild_id, user_id, symbol)
               DO UPDATE SET quantity = real_holdings.quantity + $4""",
            guild_id, user_id, symbol, quantity_change,
        )
        return

    result = await conn.execute(
        """UPDATE real_holdings
           SET quantity = quantity + $4
           WHERE guild_id = $1 AND user_id = $2 AND symbol = $3 AND quantity + $4 >= 0""",
        guild_id, user_id, symbol, quantity_change,
    )
    if result == "UPDATE 0":
        raise ValueError("Insufficient units (would go negative)")


async def get_user_holdings(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        """SELECT h.symbol, h.quantity, s.name, s.lot_size
           FROM real_holdings h
           JOIN real_symbols s ON s.symbol = h.symbol
           WHERE h.guild_id = $1 AND h.user_id = $2 AND h.quantity > 0
           ORDER BY h.symbol""",
        guild_id, user_id,
    )


async def get_symbol_holders(conn: Conn, guild_id: int, symbol: str):
    return await conn.fetch(
        "SELECT user_id, quantity FROM real_holdings WHERE guild_id = $1 AND symbol = $2 AND quantity > 0",
        guild_id, symbol,
    )


async def remove_member_data(conn: Conn, guild_id: int, user_id: int):
    """Delete a leaving member's real-stock holdings and trades (the units simply vanish)."""
    await conn.execute(
        "DELETE FROM real_trades WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
    await conn.execute(
        "DELETE FROM real_holdings WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )


# ── Trades ──

async def add_trade(conn: Conn, guild_id: int, user_id: int, symbol: str,
                    side: str, quantity: int, price: int):
    await conn.execute(
        """INSERT INTO real_trades (guild_id, user_id, symbol, side, quantity, price)
           VALUES ($1, $2, $3, $4, $5, $6)""",
        guild_id, user_id, symbol, side, quantity, price,
    )


async def get_trades(conn: Conn, guild_id: int, user_id: int):
    """All of a user's real-stock trades, oldest first — the ordering hold-time
    calculations in service.py depend on."""
    return await conn.fetch(
        """SELECT t.*, s.name
           FROM real_trades t
           JOIN real_symbols s ON s.symbol = t.symbol
           WHERE t.guild_id = $1 AND t.user_id = $2
           ORDER BY t.traded_at ASC, t.id ASC""",
        guild_id, user_id,
    )


async def get_avg_buy_price(conn: Conn, guild_id: int, user_id: int, symbol: str) -> int:
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(quantity * price), 0) AS total_cost,
                  COALESCE(SUM(quantity), 0) AS total_qty
           FROM real_trades
           WHERE guild_id = $1 AND user_id = $2 AND symbol = $3 AND side = 'buy'""",
        guild_id, user_id, symbol,
    )
    if row["total_qty"] == 0:
        return 0
    return row["total_cost"] // row["total_qty"]


# ── Price history (global per symbol — feeds the .stockinfo chart) ──

PRICE_HISTORY_LIMIT = 250


async def record_price(conn: Conn, symbol: str, price: int, *, only_if_changed: bool = True):
    """Append a price point; by default skipped when it equals the latest recorded one."""
    if only_if_changed:
        last = await get_last_recorded_price(conn, symbol)
        if last == price:
            return
    await conn.execute(
        "INSERT INTO real_price_history (symbol, price) VALUES ($1, $2)",
        symbol, price,
    )


async def get_last_recorded_price(conn: Conn, symbol: str):
    row = await conn.fetchrow(
        "SELECT price FROM real_price_history WHERE symbol = $1 ORDER BY recorded_at DESC, id DESC LIMIT 1",
        symbol,
    )
    return row["price"] if row else None


async def get_price_history(conn: Conn, symbol: str, limit: int = PRICE_HISTORY_LIMIT):
    """Up to `limit` most recent points as (price, recorded_at), oldest first."""
    rows = await conn.fetch(
        """SELECT price, recorded_at FROM real_price_history
           WHERE symbol = $1 ORDER BY recorded_at DESC, id DESC LIMIT $2""",
        symbol, limit,
    )
    return list(reversed(rows))


async def get_price_history_since(conn: Conn, symbol: str, since, limit: int = PRICE_HISTORY_LIMIT):
    rows = await conn.fetch(
        """SELECT price, recorded_at FROM real_price_history
           WHERE symbol = $1 AND recorded_at >= $2
           ORDER BY recorded_at DESC, id DESC LIMIT $3""",
        symbol, since, limit,
    )
    return list(reversed(rows))


async def get_last_price_before(conn: Conn, symbol: str, before):
    """Most recent recorded price strictly before `before` — anchors windowed charts."""
    row = await conn.fetchrow(
        """SELECT price FROM real_price_history
           WHERE symbol = $1 AND recorded_at < $2
           ORDER BY recorded_at DESC, id DESC LIMIT 1""",
        symbol, before,
    )
    return row["price"] if row else None
