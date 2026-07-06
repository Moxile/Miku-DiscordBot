from __future__ import annotations

from core.db import Conn


async def excluded_ids(conn: Conn, guild_id: int) -> set:
    rows = await conn.fetch(
        "SELECT user_id FROM lb_excluded WHERE guild_id = $1", guild_id,
    )
    return {r["user_id"] for r in rows}


async def lb_wallet(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        """SELECT user_id, wallet AS score
           FROM balances WHERE guild_id = $1
           ORDER BY wallet DESC""",
        guild_id,
    )


async def lb_bank(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        """SELECT user_id, bank AS score
           FROM balances WHERE guild_id = $1
           ORDER BY bank DESC""",
        guild_id,
    )


async def lb_portfolio(conn: Conn, guild_id: int) -> list:
    """Portfolio value = SUM(quantity * last_trade_price per company, fallback to ipo_price)."""
    return await conn.fetch(
        """WITH last_prices AS (
               SELECT DISTINCT ON (guild_id, stock_channel_id)
                      guild_id, stock_channel_id, price
               FROM trade_history
               ORDER BY guild_id, stock_channel_id, traded_at DESC
           ),
           prices AS (
               SELECT c.guild_id, c.stock_channel_id,
                      COALESCE(lp.price, c.ipo_price) AS price
               FROM companies c
               LEFT JOIN last_prices lp
                      ON lp.guild_id = c.guild_id
                     AND lp.stock_channel_id = c.stock_channel_id
               WHERE c.guild_id = $1
           )
           SELECT p.user_id, COALESCE(SUM(p.quantity * pr.price), 0) AS score
           FROM portfolios p
           JOIN prices pr ON pr.guild_id = p.guild_id AND pr.stock_channel_id = p.stock_channel_id
           WHERE p.guild_id = $1 AND p.quantity > 0
           GROUP BY p.user_id
           ORDER BY score DESC""",
        guild_id,
    )


async def lb_waifu(conn: Conn, guild_id: int) -> list:
    """Harem value = SUM of owned waifu values per owner."""
    return await conn.fetch(
        """SELECT owner_id AS user_id, SUM(value) AS score
           FROM waifus
           WHERE guild_id = $1 AND owner_id IS NOT NULL
           GROUP BY owner_id
           ORDER BY score DESC""",
        guild_id,
    )


async def lb_net(conn: Conn, guild_id: int) -> list:
    """Net worth = wallet + bank + portfolio value + harem value."""
    return await conn.fetch(
        """WITH last_prices AS (
               SELECT DISTINCT ON (guild_id, stock_channel_id)
                      guild_id, stock_channel_id, price
               FROM trade_history
               ORDER BY guild_id, stock_channel_id, traded_at DESC
           ),
           prices AS (
               SELECT c.guild_id, c.stock_channel_id,
                      COALESCE(lp.price, c.ipo_price) AS price
               FROM companies c
               LEFT JOIN last_prices lp
                      ON lp.guild_id = c.guild_id
                     AND lp.stock_channel_id = c.stock_channel_id
               WHERE c.guild_id = $1
           ),
           port_value AS (
               SELECT p.user_id, COALESCE(SUM(p.quantity * pr.price), 0) AS port
               FROM portfolios p
               JOIN prices pr ON pr.guild_id = p.guild_id AND pr.stock_channel_id = p.stock_channel_id
               WHERE p.guild_id = $1 AND p.quantity > 0
               GROUP BY p.user_id
           ),
           harem_value AS (
               SELECT owner_id AS user_id, COALESCE(SUM(value), 0) AS harem
               FROM waifus
               WHERE guild_id = $1 AND owner_id IS NOT NULL
               GROUP BY owner_id
           )
           SELECT b.user_id,
                  (b.wallet + b.bank
                   + COALESCE(pv.port, 0)
                   + COALESCE(hv.harem, 0)) AS score
           FROM balances b
           LEFT JOIN port_value pv ON pv.user_id = b.user_id
           LEFT JOIN harem_value hv ON hv.user_id = b.user_id
           WHERE b.guild_id = $1
           ORDER BY score DESC""",
        guild_id,
    )


async def get_reaction_config(conn: Conn, guild_id: int):
    return await conn.fetchrow(
        "SELECT emoji_key, is_custom, emoji_display FROM reaction_lb_config WHERE guild_id = $1",
        guild_id,
    )


async def lb_reactions(conn: Conn, guild_id: int) -> list:
    return await conn.fetch(
        """SELECT user_id, count AS score
           FROM reaction_lb_counts
           WHERE guild_id = $1 AND count > 0
           ORDER BY count DESC""",
        guild_id,
    )
