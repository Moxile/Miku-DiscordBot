from __future__ import annotations

"""Real-stock trading logic shared by the text commands (cog.py) and the Miku
Menu (ui.py). `quotes` is the cog's QuoteService — callers pass it in."""

import asyncio
import datetime
from dataclasses import dataclass

import discord

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.market.chart import render_price_chart
from cogs.realstocks.db import (
    get_guild_stock, lock_holding, update_holding,
    add_trade, record_price, get_trades,
    get_price_history, get_price_history_since, get_last_price_before,
    list_guild_stocks,
)
from cogs.realstocks.quotes import QuoteError, UnknownSymbolError, unit_buy_price, unit_sell_price, unit_mid_price
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError

# Time windows offered by the price-chart buttons: key -> (chart subtitle, days)
CHART_WINDOWS = {
    "daily": ("Past 24 hours", 1),
    "weekly": ("Past 7 days", 7),
    "monthly": ("Past 30 days", 30),
}


async def fetch_quote(quotes, symbol: str, *, fresh: bool = False):
    """Get a quote or raise UserError with the friendly message.

    `fresh=True` bypasses the quote cache entirely (max_age=0) — required for buy/sell
    so a trade always executes at the true live price rather than a price up to
    REALSTOCK_QUOTE_TTL seconds old, which would otherwise let someone trade against
    a price they know is already stale."""
    if not quotes.configured:
        raise UserError("Real-stock trading is not configured (missing `FINNHUB_API_KEY`).")
    try:
        return await quotes.get_quote(symbol, max_age=0 if fresh else None)
    except UnknownSymbolError:
        raise UserError(f"Unknown ticker: **{symbol}**.")
    except QuoteError as e:
        raise UserError(str(e))


async def _ensure_can_trade(pool, guild_id: int, user_id: int, channel_id: int):
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from using the economy.")
    required = await get_required_channel(pool, guild_id, "trading_channel")
    if required is not None and channel_id != required:
        raise UserError(f"This can only be used in <#{required}>.")


async def _require_stock(pool, guild_id: int, symbol: str):
    stock = await get_guild_stock(pool, guild_id, symbol)
    if not stock:
        raise UserError("This stock is not enabled here. See `.realstocks` for what is.")
    return stock


async def render_window(pool, symbol_row, key: str):
    """Render the recorded-price chart for a window key ('daily'/'weekly'/'monthly'/'all').

    Returns a discord.File named ``price_<key>.png``, or None when nothing is recorded yet.
    Windowed views anchor the line at the last recorded price before the window so the
    chart spans the whole period even when the price barely moved.
    """
    symbol = symbol_row["symbol"]
    now = datetime.datetime.now(datetime.timezone.utc)
    if key == "all":
        history = await get_price_history(pool, symbol)
        if not history:
            return None
        points = [(r["recorded_at"], r["price"]) for r in history]
        period_label = "Since listing"
    else:
        period_label, days = CHART_WINDOWS[key]
        cutoff = max(now - datetime.timedelta(days=days), symbol_row["added_at"])
        rows = await get_price_history_since(pool, symbol, cutoff)
        anchor = await get_last_price_before(pool, symbol, cutoff)
        if anchor is None and not rows:
            return None
        if anchor is None:
            anchor = rows[0]["price"]
        points = [(cutoff, anchor)] + [(r["recorded_at"], r["price"]) for r in rows]
        if len(points) == 1:  # nothing recorded in the window — flat line across it
            points.append((now, anchor))

    title = f"{symbol_row['name']} ({symbol})"
    loop = asyncio.get_running_loop()
    buf = await loop.run_in_executor(None, render_price_chart, title, points, period_label)
    return discord.File(buf, filename=f"price_{key}.png")


@dataclass
class RealTradeResult:
    stock_name: str
    symbol: str
    quantity: int
    unit_price: int
    total: int
    lot_size: int


async def buy(pool, quotes, guild_id: int, user_id: int, symbol: str,
              quantity: int, channel_id: int) -> RealTradeResult:
    """Buy units at the live price. Coins go to the void (price-taker model)."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    if quantity <= 0:
        raise UserError("Quantity must be positive.")
    stock = await _require_stock(pool, guild_id, symbol)

    quote = await fetch_quote(quotes, symbol, fresh=True)
    unit_price = unit_buy_price(quote.price, stock["lot_size"])
    total = unit_price * quantity

    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild_id, user_id)
            wallet = await lock_wallet(conn, guild_id, user_id)
            if wallet["wallet"] < total:
                raise UserError(f"You need {total:,} but only have {wallet['wallet']:,}.")
            await update_wallet(conn, guild_id, user_id, -total)
            await update_holding(conn, guild_id, user_id, symbol, quantity)
            await add_trade(conn, guild_id, user_id, symbol, "buy", quantity, unit_price)
            await add_transaction(conn, guild_id, user_id, -total, "realstock_buy",
                                  f"Bought {quantity}x {symbol} units")

    return RealTradeResult(stock_name=stock["name"], symbol=symbol, quantity=quantity,
                           unit_price=unit_price, total=total, lot_size=stock["lot_size"])


async def sell(pool, quotes, guild_id: int, user_id: int, symbol: str,
               quantity: int, channel_id: int) -> RealTradeResult:
    """Sell units at the live price."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    if quantity <= 0:
        raise UserError("Quantity must be positive.")
    stock = await _require_stock(pool, guild_id, symbol)

    quote = await fetch_quote(quotes, symbol, fresh=True)
    unit_price = unit_sell_price(quote.price, stock["lot_size"])
    total = unit_price * quantity

    async with pool.acquire() as conn:
        async with conn.transaction():
            held = await lock_holding(conn, guild_id, user_id, symbol)
            if held < quantity:
                raise UserError(f"You only hold {held:,} unit(s) of **{symbol}**.")
            await update_holding(conn, guild_id, user_id, symbol, -quantity)
            await ensure_wallet(conn, guild_id, user_id)
            await update_wallet(conn, guild_id, user_id, total)
            await add_trade(conn, guild_id, user_id, symbol, "sell", quantity, unit_price)
            await add_transaction(conn, guild_id, user_id, total, "realstock_sell",
                                  f"Sold {quantity}x {symbol} units")

    return RealTradeResult(stock_name=stock["name"], symbol=symbol, quantity=quantity,
                           unit_price=unit_price, total=total, lot_size=stock["lot_size"])


async def trade_history(pool, guild_id: int, user_id: int) -> list[dict]:
    """A user's trades, newest first, each with a `hold_seconds` for sells — the time
    since that user's most recent buy of the same symbol before the sell. Approximates
    per-lot hold time without full FIFO accounting, which is plenty for spotting
    flips (buy-then-sell-fast) at a glance."""
    rows = await get_trades(pool, guild_id, user_id)  # oldest first
    last_buy_at: dict[str, datetime.datetime] = {}
    history = []
    for t in rows:
        hold_seconds = None
        if t["side"] == "buy":
            last_buy_at[t["symbol"]] = t["traded_at"]
        else:
            bought_at = last_buy_at.get(t["symbol"])
            if bought_at is not None:
                hold_seconds = (t["traded_at"] - bought_at).total_seconds()
        history.append({
            "symbol": t["symbol"], "name": t["name"], "side": t["side"],
            "quantity": t["quantity"], "price": t["price"], "traded_at": t["traded_at"],
            "hold_seconds": hold_seconds,
        })
    history.reverse()
    return history


SORT_LABELS = {"name": "Name (A→Z)", "gainers": "Top Gainers", "losers": "Top Losers"}


async def list_stock_rows(pool, quotes, guild_id: int, sort: str = "name") -> list[dict]:
    """Every enabled stock with its live quote, sorted for browsing. Each row is
    ``{"stock": <db row>, "quote": Quote|None, "pct": float|None, "unit": int|None, "ok": bool}``
    — ``ok`` is False when the quote couldn't be fetched (price fields are then None)."""
    stocks = await list_guild_stocks(pool, guild_id)
    rows = []
    for s in stocks:
        try:
            quote = await quotes.get_quote(s["symbol"])
            pct = (quote.price - quote.prev_close) / quote.prev_close * 100 if quote.prev_close else 0.0
            unit = unit_mid_price(quote.price, s["lot_size"])
            rows.append({"stock": s, "quote": quote, "pct": pct, "unit": unit, "ok": True})
        except QuoteError:
            rows.append({"stock": s, "quote": None, "pct": None, "unit": None, "ok": False})
    return sort_stock_rows(rows, sort)


def sort_stock_rows(rows: list[dict], sort: str) -> list[dict]:
    """Re-sort rows already built by list_stock_rows — no re-fetching involved."""
    if sort == "gainers":
        rows.sort(key=lambda r: r["pct"] if r["ok"] else float("-inf"), reverse=True)
    elif sort == "losers":
        rows.sort(key=lambda r: r["pct"] if r["ok"] else float("inf"))
    else:
        rows.sort(key=lambda r: r["stock"]["name"].lower())
    return rows


def format_market_cap(cap_millions: float | None) -> str:
    """Finnhub reports market cap in millions of USD."""
    if not cap_millions:
        return "—"
    if cap_millions >= 1_000_000:
        return f"${cap_millions / 1_000_000:,.2f}T"
    if cap_millions >= 1_000:
        return f"${cap_millions / 1_000:,.2f}B"
    return f"${cap_millions:,.0f}M"


def company_info_lines(stock) -> list[str]:
    """Cached fundamentals for a stock row (sector, website, market cap, EPS) as
    embed-ready lines. Empty when nothing has been fetched yet."""
    lines = []
    if stock["industry"]:
        lines.append(f"Sector: {stock['industry']}")
    if stock["domain"]:
        lines.append(f"Website: [{stock['domain']}](https://{stock['domain']})")
    if stock["market_cap"]:
        lines.append(f"Market Cap: {format_market_cap(stock['market_cap'])}")
    if stock["eps"] is not None:
        lines.append(f"EPS (TTM): {stock['eps']:.2f}")
    return lines


async def record_live_price(pool, quotes, stock) -> None:
    """Refresh the chart-recorded price from a live quote (best effort)."""
    try:
        quote = await quotes.get_quote(stock["symbol"])
    except QuoteError:
        return
    await record_price(pool, stock["symbol"], unit_mid_price(quote.price, stock["lot_size"]))
