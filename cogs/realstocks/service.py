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
    get_guild_stock, get_symbol, lock_holding, update_holding,
    add_trade, record_price, get_trades,
    get_price_history, get_price_history_since, get_last_price_before,
    list_guild_stocks,
)
from cogs.realstocks.quotes import QuoteError, UnknownSymbolError, unit_buy_price, unit_sell_price, unit_mid_price
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError
from core.time_utils import humanize_duration
from config import REALSTOCK_MAX_QUOTE_AGE

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


def _ensure_tradeable_quote(quote):
    """Refuse trades against a stale price. The quote's exchange timestamp only advances
    while the stock is actually trading, so a large age means the market is closed,
    pre-open, halted, or the feed hasn't ticked yet — exactly the windows where the shown
    price lags the real one and can be arbitraged (e.g. buying at the pre-open price right
    before it jumps at the open)."""
    age = quote.age_seconds()
    if age > REALSTOCK_MAX_QUOTE_AGE:
        raise UserError(
            "Trading is paused for this stock — the market looks closed or the price "
            f"hasn't updated in {humanize_duration(int(age), short=True)}. Try again once "
            "it's trading."
        )


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
    _ensure_tradeable_quote(quote)
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
    _ensure_tradeable_quote(quote)
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


async def trade_history(pool, quotes, guild_id: int, user_id: int) -> list[dict]:
    """A user's trades, FIFO-matched per symbol into one row per lot rather than
    separate buy/sell rows — so buying 100x NVDA and later selling that same 100
    is one row with its hold time and P/L, and buying 50+50 then selling 100 at
    once produces two rows (one per original buy), since those are genuinely
    different holding periods.

    Still-open lots (bought but not yet sold) get their own row too, with
    hold_seconds counted up to now and `pl` as unrealized P/L against the current
    quote (None if a quote isn't available). Rows are newest-first by whichever
    event is most recent: sold_at for closed lots, bought_at for open ones.

    Each row: symbol, name, quantity, buy_price, bought_at, sell_price (None if
    still held), sold_at (None if still held), hold_seconds, pl."""
    trades = await get_trades(pool, guild_id, user_id)  # oldest first
    open_lots: dict[str, list[dict]] = {}
    names: dict[str, str] = {}
    closed = []
    for t in trades:
        symbol = t["symbol"]
        names[symbol] = t["name"]
        lots = open_lots.setdefault(symbol, [])
        if t["side"] == "buy":
            lots.append({"quantity": t["quantity"], "price": t["price"], "bought_at": t["traded_at"]})
            continue
        remaining = t["quantity"]
        while remaining > 0 and lots:
            lot = lots[0]
            take = min(remaining, lot["quantity"])
            closed.append({
                "symbol": symbol, "name": t["name"], "quantity": take,
                "buy_price": lot["price"], "bought_at": lot["bought_at"],
                "sell_price": t["price"], "sold_at": t["traded_at"],
                "hold_seconds": (t["traded_at"] - lot["bought_at"]).total_seconds(),
                "pl": take * (t["price"] - lot["price"]),
            })
            lot["quantity"] -= take
            remaining -= take
            if lot["quantity"] == 0:
                lots.pop(0)

    open_rows = []
    now = datetime.datetime.now(datetime.timezone.utc)
    for symbol, lots in open_lots.items():
        if not lots:
            continue
        current_price = None
        sym_row = await get_symbol(pool, symbol)
        if sym_row:
            try:
                quote = await quotes.get_quote(symbol)
                current_price = unit_sell_price(quote.price, sym_row["lot_size"])
            except QuoteError:
                pass
        for lot in lots:
            pl = lot["quantity"] * (current_price - lot["price"]) if current_price is not None else None
            open_rows.append({
                "symbol": symbol, "name": names[symbol], "quantity": lot["quantity"],
                "buy_price": lot["price"], "bought_at": lot["bought_at"],
                "sell_price": None, "sold_at": None,
                "hold_seconds": (now - lot["bought_at"]).total_seconds(),
                "pl": pl,
            })

    history = closed + open_rows
    history.sort(key=lambda r: r["sold_at"] or r["bought_at"], reverse=True)
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
