from __future__ import annotations

"""Options trading logic shared by the text commands (cog.py), the Miku Menu
(ui.py), and the expiry-settlement background loop.

Options are European, buy-only, and cash-settled against the void. Buying pays a
Black-Scholes premium up front (see pricing.py); the position then either settles
at expiry for its intrinsic value, or the holder closes early for its current fair
value. `quotes` is RealStocks' shared QuoteService — callers pass it in.

Coin amounts scale from per-share USD prices by the contract multiplier and count:
    premium_cost = ceil(bs_price  * multiplier * contracts)   # buyer pays, rounded up
    close_payout = floor(bs_value * multiplier * contracts)   # house keeps the fraction
    expiry_payoff = floor(intrinsic * multiplier * contracts)
"""

import datetime
import math
from dataclasses import dataclass

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.realstocks.db import get_guild_stock, get_last_recorded_price
from cogs.realstocks.quotes import QuoteError
from cogs.realstocks.service import fetch_quote
from cogs.options import pricing
from cogs.options.db import (
    create_position, get_open_position, lock_position, get_user_open_positions,
    get_expired_open_positions, get_open_positions_for_symbol, get_position_history,
    settle_position,
)
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError
from core.time_utils import humanize_duration
from config import (
    OPTION_CONTRACT_MULTIPLIER, OPTION_MIN_DAYS, OPTION_MAX_DAYS, OPTION_MAX_CONTRACTS,
    OPTION_RISK_FREE_RATE, REALSTOCK_MAX_QUOTE_AGE,
)


# ── Trade guards (mirrors realstocks/cfd, kept local so options stands alone) ──

async def _ensure_can_trade(pool, guild_id: int, user_id: int, channel_id: int):
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from using the economy.")
    required = await get_required_channel(pool, guild_id, "trading_channel")
    if required is not None and channel_id != required:
        raise UserError(f"This can only be used in <#{required}>.")


def _ensure_tradeable_quote(quote):
    age = quote.age_seconds()
    if age > REALSTOCK_MAX_QUOTE_AGE:
        raise UserError(
            "Options trading is paused for this stock — the market looks closed or the price "
            f"hasn't updated in {humanize_duration(int(age), short=True)}. Try again once "
            "it's trading."
        )


async def _require_stock(pool, guild_id: int, symbol: str):
    stock = await get_guild_stock(pool, guild_id, symbol)
    if not stock:
        raise UserError("This stock is not enabled here. See `.realstocks` for what is.")
    return stock


async def _resolve_spot(quotes, pool, symbol: str, lot_size: int) -> float | None:
    """Best-available underlying price in USD for settlement, when a fresh trade quote
    isn't required: the live (cached) quote if reachable, else the last recorded chart
    price converted back from its lot-scaled unit price."""
    try:
        quote = await quotes.get_quote(symbol)
        return quote.price
    except QuoteError:
        last_unit = await get_last_recorded_price(pool, symbol)
        if last_unit and lot_size:
            return last_unit / lot_size
        return None


# ── Quote preview ──

@dataclass
class OptionQuote:
    symbol: str
    stock_name: str
    opt_type: str
    strike: float
    days: int
    spot: float
    iv: float
    premium_per_contract: int
    contracts: int
    total_cost: int


async def quote_premium(pool, quotes, guild_id: int, symbol: str, opt_type: str,
                        strike: float, days: int, contracts: int = 1) -> OptionQuote:
    """Price a prospective option without buying it."""
    _validate_terms(opt_type, strike, days, contracts)
    stock = await _require_stock(pool, guild_id, symbol)
    quote = await fetch_quote(quotes, symbol, fresh=True)
    _ensure_tradeable_quote(quote)
    iv = await pricing.estimate_iv(pool, symbol)
    per_share = pricing.black_scholes(opt_type, quote.price, strike, days, iv, OPTION_RISK_FREE_RATE)
    per_contract = max(1, math.ceil(per_share * OPTION_CONTRACT_MULTIPLIER))
    total = max(1, math.ceil(per_share * OPTION_CONTRACT_MULTIPLIER * contracts))
    return OptionQuote(symbol=symbol, stock_name=stock["name"], opt_type=opt_type, strike=strike,
                       days=days, spot=quote.price, iv=iv, premium_per_contract=per_contract,
                       contracts=contracts, total_cost=total)


def _validate_terms(opt_type: str, strike: float, days: int, contracts: int):
    if opt_type not in ("call", "put"):
        raise UserError("Option type must be `call` or `put`.")
    if strike <= 0:
        raise UserError("Strike must be positive.")
    if contracts <= 0:
        raise UserError("Contracts must be positive.")
    if contracts > OPTION_MAX_CONTRACTS:
        raise UserError(f"You can buy at most {OPTION_MAX_CONTRACTS:,} contracts per position.")
    if days < OPTION_MIN_DAYS or days > OPTION_MAX_DAYS:
        raise UserError(f"Expiry must be between {OPTION_MIN_DAYS} and {OPTION_MAX_DAYS} days out.")


# ── Buy ──

@dataclass
class OptionBuyResult:
    position_id: int
    stock_name: str
    symbol: str
    opt_type: str
    strike: float
    contracts: int
    multiplier: int
    spot: float
    iv: float
    total_cost: int
    expiry: datetime.datetime


async def buy(pool, quotes, guild_id: int, user_id: int, symbol: str, opt_type: str,
              strike: float, days: int, contracts: int, channel_id: int) -> OptionBuyResult:
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    _validate_terms(opt_type, strike, days, contracts)
    stock = await _require_stock(pool, guild_id, symbol)

    quote = await fetch_quote(quotes, symbol, fresh=True)
    _ensure_tradeable_quote(quote)
    spot = quote.price
    iv = await pricing.estimate_iv(pool, symbol)
    per_share = pricing.black_scholes(opt_type, spot, strike, days, iv, OPTION_RISK_FREE_RATE)
    total_cost = max(1, math.ceil(per_share * OPTION_CONTRACT_MULTIPLIER * contracts))
    expiry = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild_id, user_id)
            wallet = await lock_wallet(conn, guild_id, user_id)
            if wallet["wallet"] < total_cost:
                raise UserError(f"That premium costs {total_cost:,} but you only have {wallet['wallet']:,}.")
            await update_wallet(conn, guild_id, user_id, -total_cost)
            row = await create_position(conn, guild_id, user_id, symbol, opt_type, strike, expiry,
                                        contracts, OPTION_CONTRACT_MULTIPLIER, spot, iv, total_cost)
            await add_transaction(conn, guild_id, user_id, -total_cost, "option_buy",
                                  f"Bought {contracts}x {symbol} {strike:g} {opt_type} exp {days}d")

    return OptionBuyResult(position_id=row["id"], stock_name=stock["name"], symbol=symbol,
                           opt_type=opt_type, strike=strike, contracts=contracts,
                           multiplier=OPTION_CONTRACT_MULTIPLIER, spot=spot, iv=iv,
                           total_cost=total_cost, expiry=expiry)


# ── Early close (sell back at fair value) ──

@dataclass
class OptionCloseResult:
    stock_name: str
    symbol: str
    opt_type: str
    strike: float
    contracts: int
    spot: float
    premium_paid: int
    payout: int
    realized_pl: int


async def close(pool, quotes, guild_id: int, user_id: int, position_id: int,
                channel_id: int) -> OptionCloseResult:
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)

    pos = await get_open_position(pool, guild_id, user_id, position_id)
    if not pos:
        raise UserError(f"You have no open option position #{position_id}.")

    quote = await fetch_quote(quotes, pos["symbol"], fresh=True)
    _ensure_tradeable_quote(quote)
    spot = quote.price
    days_left = pricing.days_until(pos["expiry"])
    per_share = pricing.black_scholes(pos["opt_type"], spot, pos["strike"], days_left,
                                      pos["iv"], OPTION_RISK_FREE_RATE)
    payout = max(0, math.floor(per_share * pos["multiplier"] * pos["contracts"]))

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await lock_position(conn, position_id)
            if not row or row["status"] != "open" or row["user_id"] != user_id \
                    or row["guild_id"] != guild_id:
                raise UserError(f"You have no open option position #{position_id}.")
            realized = payout - row["premium_paid"]
            await ensure_wallet(conn, guild_id, user_id)
            await update_wallet(conn, guild_id, user_id, payout)
            await add_transaction(conn, guild_id, user_id, payout, "option_close",
                                  f"Closed {row['contracts']}x {row['symbol']} "
                                  f"{row['strike']:g} {row['opt_type']} (P/L {realized:+,})")
            await settle_position(conn, position_id, spot, payout, realized, "closed")

    return OptionCloseResult(stock_name=pos["name"], symbol=pos["symbol"], opt_type=pos["opt_type"],
                             strike=pos["strike"], contracts=pos["contracts"], spot=spot,
                             premium_paid=pos["premium_paid"], payout=payout, realized_pl=realized)


# ── Expiry settlement (background loop) ──

async def settle_expired(pool, quotes) -> None:
    """Settle every open position that has reached expiry, at its intrinsic value.

    In-the-money options pay out and are marked 'exercised'; worthless ones are
    marked 'expired'. Uses the best-available spot (options settle even when the
    market is closed); positions whose price can't be resolved at all are left for
    the next cycle."""
    now = datetime.datetime.now(datetime.timezone.utc)
    positions = await get_expired_open_positions(pool, now)
    spot_cache: dict[str, float | None] = {}
    for pos in positions:
        symbol = pos["symbol"]
        if symbol not in spot_cache:
            spot_cache[symbol] = await _resolve_spot(quotes, pool, symbol, pos["lot_size"])
        spot = spot_cache[symbol]
        if spot is None:
            continue
        await _settle_one_at_expiry(pool, pos["id"], spot)


async def _settle_one_at_expiry(pool, position_id: int, spot: float) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await lock_position(conn, position_id)
            if not row or row["status"] != "open":
                return
            intrinsic = pricing.intrinsic_value(row["opt_type"], spot, row["strike"])
            payoff = max(0, math.floor(intrinsic * row["multiplier"] * row["contracts"]))
            realized = payoff - row["premium_paid"]
            status = "exercised" if payoff > 0 else "expired"
            if payoff > 0:
                await ensure_wallet(conn, row["guild_id"], row["user_id"])
                await update_wallet(conn, row["guild_id"], row["user_id"], payoff)
                await add_transaction(conn, row["guild_id"], row["user_id"], payoff, "option_settle",
                                      f"Exercised {row['contracts']}x {row['symbol']} "
                                      f"{row['strike']:g} {row['opt_type']} at expiry")
            await settle_position(conn, position_id, spot, payoff, realized, status)


# ── Delisting: force-settle every open position on a symbol ──

async def force_settle_symbol(conn, guild_id: int, symbol: str, spot: float) -> tuple[int, int]:
    """Settle all of a guild's open options on `symbol` at intrinsic value against `spot`.

    Runs inside RealStocks.removestock's transaction, before the cascade delete.
    Returns (positions_settled, total_paid_out)."""
    positions = await get_open_positions_for_symbol(conn, guild_id, symbol)
    total_paid = 0
    for row in positions:
        intrinsic = pricing.intrinsic_value(row["opt_type"], spot, row["strike"])
        payoff = max(0, math.floor(intrinsic * row["multiplier"] * row["contracts"]))
        realized = payoff - row["premium_paid"]
        status = "exercised" if payoff > 0 else "expired"
        if payoff > 0:
            await ensure_wallet(conn, guild_id, row["user_id"])
            await update_wallet(conn, guild_id, row["user_id"], payoff)
            await add_transaction(conn, guild_id, row["user_id"], payoff, "option_settle",
                                  f"Force-settled {row['contracts']}x {symbol} "
                                  f"{row['strike']:g} {row['opt_type']} (delisted)")
        await settle_position(conn, row["id"], spot, payoff, realized, status)
        total_paid += payoff
    return len(positions), total_paid


# ── Read models ──

async def list_open_positions(pool, quotes, guild_id: int, user_id: int) -> list[dict]:
    """A user's open options with current fair value and unrealized P/L.

    Each row: id, symbol, name, opt_type, strike, contracts, multiplier, entry_spot,
    premium_paid, expiry, days_left, spot (None if unavailable), value (mark, None if
    no spot), pl (None if no spot)."""
    positions = await get_user_open_positions(pool, guild_id, user_id)
    rows = []
    for p in positions:
        days_left = pricing.days_until(p["expiry"])
        spot = value = pl = None
        try:
            quote = await quotes.get_quote(p["symbol"])
            spot = quote.price
            per_share = pricing.black_scholes(p["opt_type"], spot, p["strike"], days_left,
                                              p["iv"], OPTION_RISK_FREE_RATE)
            value = max(0, math.floor(per_share * p["multiplier"] * p["contracts"]))
            pl = value - p["premium_paid"]
        except QuoteError:
            pass
        rows.append({
            "id": p["id"], "symbol": p["symbol"], "name": p["name"], "opt_type": p["opt_type"],
            "strike": p["strike"], "contracts": p["contracts"], "multiplier": p["multiplier"],
            "entry_spot": p["entry_spot"], "premium_paid": p["premium_paid"], "expiry": p["expiry"],
            "days_left": days_left, "spot": spot, "value": value, "pl": pl,
        })
    return rows


async def position_history(pool, guild_id: int, user_id: int) -> list[dict]:
    """A user's settled options, newest first. Each row: symbol, name, opt_type, strike,
    contracts, entry_spot, settle_spot, premium_paid, payout, realized_pl, status."""
    rows = await get_position_history(pool, guild_id, user_id)
    return [{
        "symbol": p["symbol"], "name": p["name"], "opt_type": p["opt_type"], "strike": p["strike"],
        "contracts": p["contracts"], "entry_spot": p["entry_spot"], "settle_spot": p["settle_spot"],
        "premium_paid": p["premium_paid"], "payout": p["payout"], "realized_pl": p["realized_pl"],
        "status": p["status"],
    } for p in rows]
