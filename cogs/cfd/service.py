from __future__ import annotations

"""CFD (contract-for-difference) trading logic shared by the text commands
(cog.py), the Miku Menu (ui.py), and the liquidation/financing background loop.

A CFD is a leveraged, cash-settled directional bet on one of the real stocks
from cogs/realstocks. Opening locks ``notional / leverage`` coins of margin
(sent to the void); closing pays back that margin plus profit or minus loss,
floored at zero. `quotes` is RealStocks' shared QuoteService — callers pass it in.

Pricing math (all P/L in coins, which are 1:1 with USD):

    sign    = +1 for long, -1 for short
    pl(p)   = sign * notional * (p / entry - 1)
    equity  = margin + pl(p) - financing_accrued

A position is liquidated once ``equity <= CFD_MAINTENANCE_MARGIN * margin`` —
the maintenance fraction is a buffer the house keeps so a price that gaps
straight through the zero-equity level between ticks never leaves negative
equity for the void to absorb. Solving equity == maintenance for the fee-free
case gives the stored liquidation price:

    frac  = (1 - maintenance) * margin / notional   (== (1 - maintenance) / leverage)
    long  : entry * (1 - frac)
    short : entry * (1 + frac)
"""

import datetime
import math
from dataclasses import dataclass

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.realstocks.db import get_guild_stock
from cogs.realstocks.quotes import QuoteError
from cogs.realstocks.service import fetch_quote
from cogs.cfd.db import (
    create_position, get_open_position, lock_position, get_user_open_positions,
    get_all_open_positions, get_open_positions_for_symbol, get_position_history,
    settle_position, update_financing, set_margin_call_sent,
)
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError
from core.time_utils import humanize_duration
from config import (
    CFD_MAX_LEVERAGE, CFD_MIN_MARGIN, CFD_MAX_NOTIONAL,
    CFD_MAINTENANCE_MARGIN, CFD_MARGIN_CALL_MARGIN, CFD_FINANCING_RATE, REALSTOCK_MAX_QUOTE_AGE,
)

DAY_SECONDS = 86400


# ── Pricing helpers ──

def direction_sign(direction: str) -> int:
    return 1 if direction == "long" else -1


def liquidation_price(entry: float, direction: str, margin: int, notional: int) -> float:
    """Fee-free price at which equity falls to the maintenance margin (see module docstring)."""
    frac = (1 - CFD_MAINTENANCE_MARGIN) * margin / notional
    return entry * (1 - direction_sign(direction) * frac)


def position_pl(entry: float, price: float, direction: str, notional: int) -> float:
    """Unrealized P/L in coins at `price` (unrounded)."""
    return direction_sign(direction) * notional * (price / entry - 1)


def position_equity(pos, price: float) -> float:
    """Margin + unrealized P/L - accrued financing, in coins (unrounded)."""
    pl = position_pl(pos["entry_price"], price, pos["direction"], pos["notional"])
    return pos["margin"] + pl - pos["financing_accrued"]


def max_leverage_for(notional: int) -> int:
    """Highest leverage allowed for a position of this notional, bounded by the
    global cap and the rule that margin (notional / leverage) stays >= CFD_MIN_MARGIN."""
    return max(1, min(CFD_MAX_LEVERAGE, notional // CFD_MIN_MARGIN))


def daily_financing_fee(notional: int) -> int:
    """Whole-coin overnight fee charged per 24h a position is held open."""
    return math.ceil(notional * CFD_FINANCING_RATE)


# ── Trade guards (mirrors realstocks.service, kept local so CFD stands alone) ──

async def _ensure_can_trade(pool, guild_id: int, user_id: int, channel_id: int):
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from using the economy.")
    required = await get_required_channel(pool, guild_id, "trading_channel")
    if required is not None and channel_id != required:
        raise UserError(f"This can only be used in <#{required}>.")


def _ensure_tradeable_quote(quote):
    """Refuse to open/close against a stale price — the market is closed or the feed
    hasn't ticked, exactly the window where a leveraged entry could be arbitraged."""
    age = quote.age_seconds()
    if age > REALSTOCK_MAX_QUOTE_AGE:
        raise UserError(
            "CFD trading is paused for this stock — the market looks closed or the price "
            f"hasn't updated in {humanize_duration(int(age), short=True)}. Try again once "
            "it's trading."
        )


async def _require_stock(pool, guild_id: int, symbol: str):
    stock = await get_guild_stock(pool, guild_id, symbol)
    if not stock:
        raise UserError("This stock is not enabled here. See `.realstocks` for what is.")
    return stock


# ── Open ──

@dataclass
class CFDOpenResult:
    position_id: int
    stock_name: str
    symbol: str
    direction: str
    notional: int
    leverage: int
    margin: int
    entry_price: float
    liquidation_price: float


async def open_position(pool, quotes, guild_id: int, user_id: int, symbol: str,
                        direction: str, notional: int, leverage: int,
                        channel_id: int) -> CFDOpenResult:
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    if direction not in ("long", "short"):
        raise UserError("Direction must be `long` or `short`.")
    if notional <= 0:
        raise UserError("Notional must be positive.")
    if notional > CFD_MAX_NOTIONAL:
        raise UserError(f"Notional can't exceed {CFD_MAX_NOTIONAL:,} per position.")
    if leverage < 1 or leverage > CFD_MAX_LEVERAGE:
        raise UserError(f"Leverage must be between 1 and {CFD_MAX_LEVERAGE}.")

    margin = notional // leverage
    if margin < CFD_MIN_MARGIN:
        allowed = max_leverage_for(notional)
        raise UserError(
            f"Margin would be only {margin:,} (minimum {CFD_MIN_MARGIN:,}). "
            f"At this notional the most leverage you can use is {allowed}x."
        )

    stock = await _require_stock(pool, guild_id, symbol)
    quote = await fetch_quote(quotes, symbol, fresh=True)
    _ensure_tradeable_quote(quote)
    entry = quote.price
    liq = liquidation_price(entry, direction, margin, notional)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild_id, user_id)
            wallet = await lock_wallet(conn, guild_id, user_id)
            if wallet["wallet"] < margin:
                raise UserError(f"You need {margin:,} margin but only have {wallet['wallet']:,}.")
            await update_wallet(conn, guild_id, user_id, -margin)
            row = await create_position(conn, guild_id, user_id, symbol, direction,
                                        notional, leverage, entry, margin, liq)
            await add_transaction(conn, guild_id, user_id, -margin, "cfd_open",
                                  f"Opened {leverage}x {direction} CFD on {symbol} "
                                  f"({notional:,} notional)")

    return CFDOpenResult(position_id=row["id"], stock_name=stock["name"], symbol=symbol,
                         direction=direction, notional=notional, leverage=leverage,
                         margin=margin, entry_price=entry, liquidation_price=liq)


# ── Close ──

@dataclass
class CFDCloseResult:
    stock_name: str
    symbol: str
    direction: str
    notional: int
    leverage: int
    margin: int
    entry_price: float
    close_price: float
    financing: int
    payout: int
    realized_pl: int


async def close_position(pool, quotes, guild_id: int, user_id: int, position_id: int,
                         channel_id: int) -> CFDCloseResult:
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)

    pos = await get_open_position(pool, guild_id, user_id, position_id)
    if not pos:
        raise UserError(f"You have no open CFD position #{position_id}.")

    # Fetch the fresh quote outside the transaction so no row lock is held during I/O.
    quote = await fetch_quote(quotes, pos["symbol"], fresh=True)
    _ensure_tradeable_quote(quote)
    price = quote.price

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await lock_position(conn, position_id)
            if not row or row["status"] != "open" or row["user_id"] != user_id \
                    or row["guild_id"] != guild_id:
                raise UserError(f"You have no open CFD position #{position_id}.")

            pl = round(position_pl(row["entry_price"], price, row["direction"], row["notional"]))
            financing = row["financing_accrued"]
            payout = max(0, row["margin"] + pl - financing)
            realized = payout - row["margin"]

            await ensure_wallet(conn, guild_id, user_id)
            await update_wallet(conn, guild_id, user_id, payout)
            await add_transaction(conn, guild_id, user_id, payout, "cfd_close",
                                  f"Closed {row['leverage']}x {row['direction']} CFD on "
                                  f"{row['symbol']} (P/L {realized:+,})")
            await settle_position(conn, position_id, price, realized, "closed")

    return CFDCloseResult(stock_name=pos["name"], symbol=pos["symbol"], direction=pos["direction"],
                          notional=pos["notional"], leverage=pos["leverage"], margin=pos["margin"],
                          entry_price=pos["entry_price"], close_price=price, financing=financing,
                          payout=payout, realized_pl=realized)


# ── Delisting: force-close every open position on a symbol ──

async def force_close_symbol(conn, guild_id: int, symbol: str, price: float) -> tuple[int, int]:
    """Close all of a guild's open CFD positions on `symbol` at `price`, refunding equity.

    Runs inside an existing transaction (called from RealStocks.removestock before the
    stock's row — and thus these positions — is cascade-deleted). Returns
    (positions_closed, total_paid_out)."""
    positions = await get_open_positions_for_symbol(conn, guild_id, symbol)
    total_paid = 0
    for row in positions:
        pl = round(position_pl(row["entry_price"], price, row["direction"], row["notional"]))
        payout = max(0, row["margin"] + pl - row["financing_accrued"])
        realized = payout - row["margin"]
        if payout > 0:
            await ensure_wallet(conn, guild_id, row["user_id"])
            await update_wallet(conn, guild_id, row["user_id"], payout)
            await add_transaction(conn, guild_id, row["user_id"], payout, "cfd_close",
                                  f"Force-closed {row['direction']} CFD on {symbol} (delisted)")
        await settle_position(conn, row["id"], price, realized, "closed")
        total_paid += payout
    return len(positions), total_paid


# ── Background loop: financing accrual + liquidation ──

async def all_open_positions(pool):
    """Every open position across all guilds — the loop's working set (thin passthrough)."""
    return await get_all_open_positions(pool)


@dataclass
class SettleResult:
    liquidated: bool
    margin_call: bool
    guild_id: int = None
    user_id: int = None
    symbol: str = None
    direction: str = None
    margin: int = None
    equity: float = None
    price: float = None


async def settle_or_accrue(pool, pos, price: float) -> SettleResult:
    """Process one open position against a fresh `price`: accrue any due overnight
    financing, then liquidate if equity has fallen to the maintenance margin. If
    equity has fallen to the (higher) margin-call level instead, flags it for a
    warning DM — once per drop, resetting if equity recovers above that level.

    Re-locks the row inside its own transaction, so a concurrent user-initiated
    close is handled safely (the row will no longer be 'open')."""
    now = datetime.datetime.now(datetime.timezone.utc)
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await lock_position(conn, pos["id"])
            if not row or row["status"] != "open":
                return SettleResult(liquidated=False, margin_call=False)

            financing = row["financing_accrued"]
            last_financed = row["last_financed_at"]
            elapsed_days = int((now - last_financed).total_seconds() // DAY_SECONDS)
            if elapsed_days >= 1:
                financing += elapsed_days * daily_financing_fee(row["notional"])
                last_financed = last_financed + datetime.timedelta(days=elapsed_days)

            pl = position_pl(row["entry_price"], price, row["direction"], row["notional"])
            equity = row["margin"] + pl - financing
            if equity <= CFD_MAINTENANCE_MARGIN * row["margin"]:
                # Liquidated: the house keeps the remaining margin, payout is zero.
                await settle_position(conn, row["id"], price, -row["margin"], "liquidated")
                return SettleResult(liquidated=True, margin_call=False)

            margin_call = False
            if equity <= CFD_MARGIN_CALL_MARGIN * row["margin"]:
                if not row["margin_call_sent"]:
                    margin_call = True
                    await set_margin_call_sent(conn, row["id"], True)
            elif row["margin_call_sent"]:
                await set_margin_call_sent(conn, row["id"], False)

            if financing != row["financing_accrued"]:
                await update_financing(conn, row["id"], financing, last_financed)

            return SettleResult(liquidated=False, margin_call=margin_call,
                                guild_id=row["guild_id"], user_id=row["user_id"],
                                symbol=row["symbol"], direction=row["direction"],
                                margin=row["margin"], equity=equity, price=price)


# ── Read models for the position / history views ──

async def list_open_positions(pool, quotes, guild_id: int, user_id: int) -> list[dict]:
    """A user's open positions with live mark price, unrealized P/L, and equity.

    Each row: id, symbol, name, direction, notional, leverage, margin, entry_price,
    liquidation_price, financing (accrued), price (mark, None if unavailable),
    pl (None if no mark), equity (None if no mark)."""
    positions = await get_user_open_positions(pool, guild_id, user_id)
    rows = []
    for p in positions:
        price = pl = equity = None
        try:
            quote = await quotes.get_quote(p["symbol"])
            price = quote.price
            pl = round(position_pl(p["entry_price"], price, p["direction"], p["notional"]))
            equity = round(position_equity(p, price))
        except QuoteError:
            pass
        rows.append({
            "id": p["id"], "symbol": p["symbol"], "name": p["name"],
            "direction": p["direction"], "notional": p["notional"], "leverage": p["leverage"],
            "margin": p["margin"], "entry_price": p["entry_price"],
            "liquidation_price": p["liquidation_price"], "financing": p["financing_accrued"],
            "price": price, "pl": pl, "equity": equity,
        })
    return rows


async def position_history(pool, guild_id: int, user_id: int) -> list[dict]:
    """A user's settled positions, newest first. Each row: symbol, name, direction,
    notional, leverage, entry_price, close_price, realized_pl, status, hold_seconds."""
    rows = await get_position_history(pool, guild_id, user_id)
    out = []
    for p in rows:
        hold = (p["closed_at"] - p["opened_at"]).total_seconds() if p["closed_at"] else 0
        out.append({
            "symbol": p["symbol"], "name": p["name"], "direction": p["direction"],
            "notional": p["notional"], "leverage": p["leverage"],
            "entry_price": p["entry_price"], "close_price": p["close_price"],
            "realized_pl": p["realized_pl"], "status": p["status"], "hold_seconds": hold,
        })
    return out
