import asyncpg

from config import REVENUE_OUTER_EXP
from core.db import Conn


async def lock_company(conn: asyncpg.Connection, guild_id: int, stock_channel_id: int) -> asyncpg.Record:
    """Lock and return the company row (for IPO share updates). Must be called within a transaction."""
    return await conn.fetchrow(
        "SELECT * FROM companies WHERE guild_id = $1 AND stock_channel_id = $2 FOR UPDATE",
        guild_id, stock_channel_id,
    )


async def get_company(conn: Conn, guild_id: int, stock_channel_id: int):
    return await conn.fetchrow(
        "SELECT * FROM companies WHERE guild_id = $1 AND stock_channel_id = $2",
        guild_id, stock_channel_id,
    )


async def list_companies(conn: Conn, guild_id: int):
    return await conn.fetch(
        "SELECT * FROM companies WHERE guild_id = $1 ORDER BY listed_at",
        guild_id,
    )


async def create_company(conn: Conn, guild_id: int, stock_channel_id: int, name: str, listed_by: int,
                          total_shares: int = 100, ipo_price: int = 100):
    await conn.execute(
        """INSERT INTO companies (guild_id, stock_channel_id, name, total_shares, available_ipo_shares, ipo_price, listed_by)
           VALUES ($1, $2, $3, $4, $4, $5, $6)""",
        guild_id, stock_channel_id, name, total_shares, ipo_price, listed_by,
    )


async def delete_company(conn: Conn, guild_id: int, stock_channel_id: int):
    """Delete a company and all related data (cascades to portfolios, orders, trades, etc.)."""
    return await conn.fetchrow(
        "DELETE FROM companies WHERE guild_id = $1 AND stock_channel_id = $2 RETURNING name",
        guild_id, stock_channel_id,
    )


async def get_portfolio(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        "SELECT * FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND quantity > 0",
        guild_id, user_id,
    )


async def get_holding(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int):
    row = await conn.fetchrow(
        "SELECT quantity FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3",
        guild_id, user_id, stock_channel_id,
    )
    return row["quantity"] if row else 0


async def lock_holding(conn: asyncpg.Connection, guild_id: int, user_id: int, stock_channel_id: int) -> int:
    """Lock and return the portfolio quantity for a user. Must be called within a transaction."""
    row = await conn.fetchrow(
        "SELECT quantity FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3 FOR UPDATE",
        guild_id, user_id, stock_channel_id,
    )
    return row["quantity"] if row else 0


async def update_holding(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int, quantity_change: int):
    await conn.execute(
        """INSERT INTO portfolios (guild_id, user_id, stock_channel_id, quantity)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, user_id, stock_channel_id)
           DO UPDATE SET quantity = portfolios.quantity + $4""",
        guild_id, user_id, stock_channel_id, quantity_change,
    )


async def get_open_orders(conn: Conn, guild_id: int, stock_channel_id: int, side: str = None):
    if side:
        order = "ASC" if side == "sell" else "DESC"
        return await conn.fetch(
            f"SELECT * FROM orders WHERE guild_id = $1 AND stock_channel_id = $2 AND side = $3 AND remaining > 0 ORDER BY price {order}, created_at ASC",
            guild_id, stock_channel_id, side,
        )
    return await conn.fetch(
        "SELECT * FROM orders WHERE guild_id = $1 AND stock_channel_id = $2 AND remaining > 0 ORDER BY price DESC, created_at ASC",
        guild_id, stock_channel_id,
    )


async def get_open_orders_locked(conn: asyncpg.Connection, guild_id: int, stock_channel_id: int, side: str):
    """Get open orders with FOR UPDATE lock. Must be called within a transaction."""
    order = "ASC" if side == "sell" else "DESC"
    return await conn.fetch(
        f"SELECT * FROM orders WHERE guild_id = $1 AND stock_channel_id = $2 AND side = $3 AND remaining > 0 ORDER BY price {order}, created_at ASC FOR UPDATE",
        guild_id, stock_channel_id, side,
    )


async def get_user_orders(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetch(
        "SELECT * FROM orders WHERE guild_id = $1 AND user_id = $2 AND remaining > 0 ORDER BY created_at DESC",
        guild_id, user_id,
    )


async def get_escrowed_shares(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int):
    """Returns the total shares locked in open sell orders for a user on a stock."""
    row = await conn.fetchrow(
        "SELECT COALESCE(SUM(remaining), 0) AS total FROM orders "
        "WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3 AND side = 'sell' AND remaining > 0",
        guild_id, user_id, stock_channel_id,
    )
    return row["total"]


async def create_order(conn: Conn, guild_id: int, stock_channel_id: int, user_id: int, side: str, quantity: int, price: int):
    return await conn.fetchrow(
        """INSERT INTO orders (guild_id, stock_channel_id, user_id, side, quantity, remaining, price)
           VALUES ($1, $2, $3, $4, $5, $5, $6) RETURNING id""",
        guild_id, stock_channel_id, user_id, side, quantity, price,
    )


async def cancel_order(conn: Conn, guild_id: int, order_id: int, user_id: int):
    return await conn.fetchrow(
        "DELETE FROM orders WHERE id = $1 AND guild_id = $2 AND user_id = $3 AND remaining > 0 RETURNING *",
        order_id, guild_id, user_id,
    )


async def add_trade(conn: Conn, guild_id: int, stock_channel_id: int, buyer_id: int, seller_id: int,
                     quantity: int, price: int, trade_type: str = "market"):
    await conn.execute(
        """INSERT INTO trade_history (guild_id, stock_channel_id, buyer_id, seller_id, quantity, price, trade_type)
           VALUES ($1, $2, $3, $4, $5, $6, $7)""",
        guild_id, stock_channel_id, buyer_id, seller_id, quantity, price, trade_type,
    )


async def get_last_trade_price(conn: Conn, guild_id: int, stock_channel_id: int):
    row = await conn.fetchrow(
        "SELECT price FROM trade_history WHERE guild_id = $1 AND stock_channel_id = $2 ORDER BY traded_at DESC LIMIT 1",
        guild_id, stock_channel_id,
    )
    return row["price"] if row else None


async def upsert_char_count(conn: Conn, guild_id: int, stock_channel_id: int,
                            user_id: int, activity_date, char_count: int):
    """Increment character count for a user in a company channel for a given date."""
    await conn.execute(
        """INSERT INTO channel_activity (guild_id, stock_channel_id, user_id, activity_date, char_count)
           VALUES ($1, $2, $3, $4, $5)
           ON CONFLICT (guild_id, stock_channel_id, user_id, activity_date)
           DO UPDATE SET char_count = channel_activity.char_count + $5""",
        guild_id, stock_channel_id, user_id, activity_date, char_count,
    )


async def compute_daily_revenue(conn: Conn, guild_id: int, stock_channel_id: int,
                                 activity_date, revenue_multiplier: int) -> int:
    """Compute daily revenue from char counts and store it. Returns the computed revenue."""
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(SQRT(SQRT(char_count))), 0) AS raw_sum
           FROM channel_activity
           WHERE guild_id = $1 AND stock_channel_id = $2 AND activity_date = $3""",
        guild_id, stock_channel_id, activity_date,
    )
    revenue = int(row["raw_sum"] ** REVENUE_OUTER_EXP * revenue_multiplier)
    await conn.execute(
        """INSERT INTO company_revenue (guild_id, stock_channel_id, revenue_date, revenue)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, stock_channel_id, revenue_date)
           DO UPDATE SET revenue = $4""",
        guild_id, stock_channel_id, activity_date, revenue,
    )
    return revenue


async def get_weekly_revenue(conn: Conn, guild_id: int, stock_channel_id: int,
                              week_start, week_end) -> list:
    """Get daily revenue records for a date range (inclusive)."""
    return await conn.fetch(
        """SELECT revenue_date, revenue FROM company_revenue
           WHERE guild_id = $1 AND stock_channel_id = $2
             AND revenue_date >= $3 AND revenue_date <= $4
           ORDER BY revenue_date""",
        guild_id, stock_channel_id, week_start, week_end,
    )


async def get_weekly_revenue_total(conn: Conn, guild_id: int, stock_channel_id: int,
                                    week_start, week_end) -> int:
    """Sum of revenue for a date range."""
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(revenue), 0) AS total FROM company_revenue
           WHERE guild_id = $1 AND stock_channel_id = $2
             AND revenue_date >= $3 AND revenue_date <= $4""",
        guild_id, stock_channel_id, week_start, week_end,
    )
    return row["total"]


async def update_treasury(conn: Conn, guild_id: int, stock_channel_id: int, amount: int) -> int:
    """Change company treasury by amount. Returns new treasury value."""
    row = await conn.fetchrow(
        """UPDATE companies SET treasury = treasury + $3
           WHERE guild_id = $1 AND stock_channel_id = $2
           RETURNING treasury""",
        guild_id, stock_channel_id, amount,
    )
    return row["treasury"]


async def set_company_level(conn: Conn, guild_id: int, stock_channel_id: int,
                             level: int, new_multiplier: int, treasury_cost: int):
    """Level up a company: deduct treasury, set new level and multiplier."""
    await conn.execute(
        """UPDATE companies
           SET company_level = $3, revenue_multiplier = $4, treasury = treasury - $5
           WHERE guild_id = $1 AND stock_channel_id = $2""",
        guild_id, stock_channel_id, level, new_multiplier, treasury_cost,
    )


async def get_shareholders(conn: Conn, guild_id: int, stock_channel_id: int):
    """Get all shareholders with quantity > 0 for a company."""
    return await conn.fetch(
        """SELECT user_id, quantity FROM portfolios
           WHERE guild_id = $1 AND stock_channel_id = $2 AND quantity > 0""",
        guild_id, stock_channel_id,
    )


async def reset_all_orders(conn: Conn, guild_id: int) -> tuple[int, int, int]:
    """Cancel all open orders guild-wide, refunding escrowed funds for buy orders.

    Sell-order shares are never deducted from portfolios (escrow is virtual),
    so no share restitution is needed for those.

    Returns (buy_count, sell_count, total_refunded).
    """
    buy_orders = await conn.fetch(
        "SELECT user_id, remaining, price FROM orders WHERE guild_id = $1 AND side = 'buy' AND remaining > 0",
        guild_id,
    )

    refunds: dict[int, int] = {}
    for order in buy_orders:
        uid = order["user_id"]
        refunds[uid] = refunds.get(uid, 0) + order["remaining"] * order["price"]

    for uid, amount in refunds.items():
        await conn.execute(
            "UPDATE wallets SET wallet = wallet + $3 WHERE guild_id = $1 AND user_id = $2",
            guild_id, uid, amount,
        )

    sell_count = await conn.fetchval(
        "SELECT COUNT(*) FROM orders WHERE guild_id = $1 AND side = 'sell' AND remaining > 0",
        guild_id,
    )
    await conn.execute(
        "DELETE FROM orders WHERE guild_id = $1 AND remaining > 0",
        guild_id,
    )

    return len(buy_orders), sell_count, sum(refunds.values())


async def get_avg_buy_price(conn: Conn, guild_id: int, user_id: int, stock_channel_id: int):
    """Compute average buy price for a user's stock from trade history."""
    row = await conn.fetchrow(
        """SELECT COALESCE(SUM(quantity * price), 0) AS total_cost,
                  COALESCE(SUM(quantity), 0) AS total_qty
           FROM trade_history
           WHERE guild_id = $1 AND buyer_id = $2 AND stock_channel_id = $3""",
        guild_id, user_id, stock_channel_id,
    )
    if row["total_qty"] == 0:
        return 0
    return row["total_cost"] // row["total_qty"]
