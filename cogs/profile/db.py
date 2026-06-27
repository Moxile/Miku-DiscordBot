from __future__ import annotations

from datetime import datetime, timezone

from core.db import Conn

# tx_types logged by cogs/gambling/cog.py — used to aggregate wagered/won/lost.
GAMBLING_TX_TYPES = [
    "rps_win", "rps_loss",
    "blackjack_win", "blackjack_loss",
    "higherlower_win", "higherlower_loss",
    "betflip",
    "roulette_win", "roulette_loss",
    "russian_roulette_win", "russian_roulette_loss",
]

# deposit/withdraw only shuffle money between wallet and bank — net worth is unchanged,
# so they're excluded when reconstructing wallet+bank history from the transaction log.
NET_WORTH_EXCLUDED_TX_TYPES = ("deposit", "withdraw")

NET_WORTH_HISTORY_LIMIT = 60
NET_WORTH_HISTORY_LIMIT_WINDOWED = 250


async def get_balance(conn: Conn, guild_id: int, user_id: int) -> tuple[int, int]:
    row = await conn.fetchrow(
        "SELECT wallet, bank FROM balances WHERE guild_id = $1 AND user_id = $2",
        guild_id, user_id,
    )
    return (row["wallet"], row["bank"]) if row else (0, 0)


async def get_gambling_totals(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetchrow(
        """SELECT COALESCE(SUM(amount) FILTER (WHERE amount > 0), 0) AS won,
                  COALESCE(SUM(-amount) FILTER (WHERE amount < 0), 0) AS lost,
                  COALESCE(SUM(ABS(amount)), 0) AS wagered,
                  COUNT(*) AS games
           FROM transactions
           WHERE guild_id = $1 AND user_id = $2 AND tx_type = ANY($3)""",
        guild_id, user_id, GAMBLING_TX_TYPES,
    )


async def get_net_worth_points(conn: Conn, guild_id: int, user_id: int, wallet: int, bank: int,
                                since: datetime = None, limit: int = NET_WORTH_HISTORY_LIMIT) -> list:
    """Reconstruct (timestamp, wallet+bank) points from the transaction log.

    Takes the most recent `limit` entries on/after `since` (or the most recent `limit` overall
    if `since` is None). Anchored to the current wallet+bank so the last point always matches
    the live balance — see NET_WORTH_EXCLUDED_TX_TYPES for why deposit/withdraw are skipped.
    """
    if since is not None:
        rows = await conn.fetch(
            """SELECT amount, created_at FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type NOT IN ('deposit', 'withdraw')
                 AND created_at >= $4
               ORDER BY created_at DESC, id DESC
               LIMIT $3""",
            guild_id, user_id, limit, since,
        )
    else:
        rows = await conn.fetch(
            """SELECT amount, created_at FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type NOT IN ('deposit', 'withdraw')
               ORDER BY created_at DESC, id DESC
               LIMIT $3""",
            guild_id, user_id, limit,
        )
    if not rows:
        return [(datetime.now(timezone.utc), wallet + bank)]

    rows = list(reversed(rows))
    running = (wallet + bank) - sum(r["amount"] for r in rows)
    points = []
    for r in rows:
        running += r["amount"]
        points.append((r["created_at"], running))
    return points


async def get_portfolio_value(conn: Conn, guild_id: int, user_id: int) -> tuple[int, int]:
    """Total value of a user's stock holdings (last trade price, falling back to IPO price), and how many."""
    holdings = await conn.fetch(
        """SELECT p.quantity, c.ipo_price,
                  (SELECT price FROM trade_history th
                   WHERE th.guild_id = p.guild_id AND th.stock_channel_id = p.stock_channel_id
                   ORDER BY th.traded_at DESC LIMIT 1) AS last_price
           FROM portfolios p
           JOIN companies c ON c.guild_id = p.guild_id AND c.stock_channel_id = p.stock_channel_id
           WHERE p.guild_id = $1 AND p.user_id = $2 AND p.quantity > 0""",
        guild_id, user_id,
    )
    total = sum(h["quantity"] * (h["last_price"] or h["ipo_price"]) for h in holdings)
    return total, len(holdings)


async def get_inventory_totals(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetchrow(
        """SELECT COALESCE(SUM(quantity), 0) AS total_items, COUNT(*) AS distinct_items
           FROM inventory WHERE guild_id = $1 AND user_id = $2 AND quantity > 0""",
        guild_id, user_id,
    )


async def get_harem_value(conn: Conn, guild_id: int, user_id: int):
    return await conn.fetchrow(
        """SELECT COALESCE(SUM(value), 0) AS total, COUNT(*) AS count
           FROM waifus WHERE guild_id = $1 AND owner_id = $2""",
        guild_id, user_id,
    )
