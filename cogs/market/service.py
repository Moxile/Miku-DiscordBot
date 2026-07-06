from __future__ import annotations

"""Market trading logic shared by the text commands (cog.py) and the Miku Menu
(ui.py): market buy/sell, limit orders, order cancellation, stock gifts, and
the read-only exchange/portfolio overviews + price-chart rendering.

All functions raise core.errors.UserError with the user-facing message for any
failure; successful results come back as small dataclasses the callers format.
"""

import asyncio
import datetime
from dataclasses import dataclass, field

import discord

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.market.chart import render_price_chart
from cogs.market.db import (
    get_company, list_companies,
    get_portfolio, lock_holding, update_holding,
    get_open_orders, get_open_orders_locked, get_user_orders, cancel_order,
    create_order, get_escrowed_shares,
    add_trade, get_last_trade_price, get_price_history, PRICE_HISTORY_LIMIT,
    get_price_history_since, get_last_trade_price_before,
    lock_company, update_treasury, get_avg_buy_price,
)
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError
from core.money import parse_amount
from core.names import format_name

# Time windows offered by the price-chart buttons: key -> (button label, chart subtitle, days)
CHART_WINDOWS = {
    "daily": ("Daily", "Past 24 hours", 1),
    "weekly": ("Weekly", "Past 7 days", 7),
    "monthly": ("Monthly", "Past 30 days", 30),
}


async def _ensure_can_trade(pool, guild_id: int, user_id: int, channel_id: int):
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from using the economy.")
    required = await get_required_channel(pool, guild_id, "trading_channel")
    if required is not None and channel_id != required:
        raise UserError(f"This can only be used in <#{required}>.")


async def _require_company(pool, guild_id: int, stock_channel_id: int):
    company = await get_company(pool, guild_id, stock_channel_id)
    if not company:
        raise UserError("This channel is not a listed company.")
    return company


def _require_positive(quantity: int):
    if quantity <= 0:
        raise UserError("Quantity must be positive.")


# ── Charts ──

async def render_window(pool, guild_id: int, stock_channel_id: int, company, key: str):
    """Render the price chart for a window key ('daily'/'weekly'/'monthly'/'all').

    Returns a discord.File named ``price_<key>.png``, or None when there is nothing to draw.
    Windowed views anchor the line at the last price before the window (falling back to the
    IPO price) so the chart spans the whole period even with few or no recent trades.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    if key == "all":
        history = await get_price_history(pool, guild_id, stock_channel_id)
        if not history:
            return None
        points = [(r["traded_at"], r["price"]) for r in history]
        if len(history) < PRICE_HISTORY_LIMIT:
            points.insert(0, (company["listed_at"], company["base_ipo_price"]))
        period_label = "All time"
    else:
        _label, period_label, days = CHART_WINDOWS[key]
        cutoff = max(now - datetime.timedelta(days=days), company["listed_at"])
        rows = await get_price_history_since(pool, guild_id, stock_channel_id, cutoff)
        anchor = await get_last_trade_price_before(pool, guild_id, stock_channel_id, cutoff)
        if anchor is None:
            anchor = company["base_ipo_price"]
        points = [(cutoff, anchor)] + [(r["traded_at"], r["price"]) for r in rows]
        if len(points) == 1:  # no trades in the window — draw a flat line across it
            points.append((now, anchor))

    loop = asyncio.get_running_loop()
    buf = await loop.run_in_executor(None, render_price_chart, company["name"], points, period_label)
    return discord.File(buf, filename=f"price_{key}.png")


# ── Read-only overviews ──

async def exchange_overview(pool, guild_id: int) -> list[dict]:
    """One entry per listed company: name, channel, best bid/ask, IPO status."""
    companies = await list_companies(pool, guild_id)
    entries = []
    for c in companies:
        buy_orders = await get_open_orders(pool, guild_id, c["stock_channel_id"], "buy")
        sell_orders = await get_open_orders(pool, guild_id, c["stock_channel_id"], "sell")
        entries.append({
            "name": c["name"],
            "stock_channel_id": c["stock_channel_id"],
            "best_bid": buy_orders[0]["price"] if buy_orders else None,
            "best_ask": sell_orders[0]["price"] if sell_orders else None,
            "available_ipo_shares": c["available_ipo_shares"],
            "total_shares": c["total_shares"],
            "ipo_price": c["ipo_price"],
        })
    return entries


@dataclass
class PortfolioOverview:
    holdings: list = field(default_factory=list)  # dicts: name, quantity, price, value, avg_cost, pl, dividends
    orders: list = field(default_factory=list)    # dicts: id, side, remaining, price, stock_name, stock_channel_id
    total_value: int = 0
    total_pl: int = 0
    total_dividends: int = 0


async def portfolio_overview(pool, guild_id: int, user_id: int) -> PortfolioOverview:
    """Everything the portfolio views show, as plain data."""
    result = PortfolioOverview()
    holdings = await get_portfolio(pool, guild_id, user_id)
    total_cost = 0
    for h in holdings:
        company = await get_company(pool, guild_id, h["stock_channel_id"])
        name = company["name"] if company else str(h["stock_channel_id"])
        last_price = await get_last_trade_price(pool, guild_id, h["stock_channel_id"])
        price = last_price or (company["ipo_price"] if company else 0)
        avg_cost = await get_avg_buy_price(pool, guild_id, user_id, h["stock_channel_id"])
        value = h["quantity"] * price
        cost_basis = h["quantity"] * avg_cost
        div_row = await pool.fetchrow(
            """SELECT COALESCE(SUM(amount), 0) AS total FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'dividend'
                 AND description = $3""",
            guild_id, user_id, f"Dividend from {name}",
        )
        result.holdings.append({
            "name": name,
            "stock_channel_id": h["stock_channel_id"],
            "quantity": h["quantity"],
            "price": price,
            "value": value,
            "avg_cost": avg_cost,
            "pl": value - cost_basis,
            "dividends": div_row["total"],
        })
        result.total_value += value
        total_cost += cost_basis
        result.total_dividends += div_row["total"]
    result.total_pl = result.total_value - total_cost

    for o in await get_user_orders(pool, guild_id, user_id):
        company = await get_company(pool, guild_id, o["stock_channel_id"])
        result.orders.append({
            "id": o["id"],
            "side": o["side"],
            "remaining": o["remaining"],
            "price": o["price"],
            "stock_name": company["name"] if company else str(o["stock_channel_id"]),
            "stock_channel_id": o["stock_channel_id"],
        })
    return result


# ── Trading ──

@dataclass
class TradeResult:
    company_name: str
    quantity: int       # requested
    filled: int         # actually traded
    total: int          # cost (buy) or revenue (sell)

    @property
    def avg_price(self) -> int:
        return self.total // self.filled if self.filled else 0


async def market_buy(pool, guild_id: int, user_id: int, stock_channel_id: int,
                     quantity: int, channel_id: int) -> TradeResult:
    """Buy shares immediately at the best available price (order book + IPO)."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    _require_positive(quantity)
    company = await _require_company(pool, guild_id, stock_channel_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild_id, user_id)
            bought = 0
            total_cost = 0

            company = await lock_company(conn, guild_id, stock_channel_id)
            sell_orders = await get_open_orders_locked(conn, guild_id, stock_channel_id, "sell")
            wallet = await lock_wallet(conn, guild_id, user_id)
            remaining_funds = wallet["wallet"]

            ipo_rem = company["available_ipo_shares"]
            ipo_price = company["ipo_price"]
            order_rems = {o["id"]: o["remaining"] for o in sell_orders}

            while bought < quantity and remaining_funds > 0:
                need = quantity - bought

                # Find cheapest available sell order (skip own orders)
                best_order = None
                for o in sell_orders:
                    if o["user_id"] == user_id:
                        continue
                    if order_rems[o["id"]] > 0:
                        best_order = o
                        break  # already sorted ASC, so first valid is cheapest

                has_ipo = ipo_rem > 0
                has_order = best_order is not None

                if not has_ipo and not has_order:
                    break

                use_ipo = (has_ipo and not has_order) or (has_ipo and has_order and ipo_price <= best_order["price"])

                if use_ipo:
                    fill_qty = min(need, ipo_rem, remaining_funds // ipo_price)
                    if fill_qty <= 0:
                        break
                    cost = fill_qty * ipo_price
                    await update_wallet(conn, guild_id, user_id, -cost)
                    await update_holding(conn, guild_id, user_id, stock_channel_id, fill_qty)
                    await conn.execute(
                        "UPDATE companies SET available_ipo_shares = available_ipo_shares - $3 WHERE guild_id = $1 AND stock_channel_id = $2",
                        guild_id, stock_channel_id, fill_qty,
                    )
                    await add_trade(conn, guild_id, stock_channel_id, user_id, None, fill_qty, ipo_price, "ipo")
                    await update_treasury(conn, guild_id, stock_channel_id, cost)
                    bought += fill_qty
                    total_cost += cost
                    remaining_funds -= cost
                    ipo_rem -= fill_qty
                else:
                    order = best_order
                    fill_qty = min(need, order_rems[order["id"]], remaining_funds // order["price"])
                    if fill_qty <= 0:
                        break
                    seller_qty = await conn.fetchval(
                        "SELECT COALESCE(quantity, 0) FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3 FOR UPDATE",
                        guild_id, order["user_id"], stock_channel_id,
                    )
                    fill_qty = min(fill_qty, seller_qty or 0)
                    if fill_qty <= 0:
                        order_rems[order["id"]] = 0
                        continue
                    cost = fill_qty * order["price"]
                    await update_wallet(conn, guild_id, user_id, -cost)
                    await ensure_wallet(conn, guild_id, order["user_id"])
                    await update_wallet(conn, guild_id, order["user_id"], cost)
                    try:
                        await update_holding(conn, guild_id, order["user_id"], stock_channel_id, -fill_qty)
                    except ValueError:
                        order_rems[order["id"]] = 0
                    await update_holding(conn, guild_id, user_id, stock_channel_id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, guild_id, stock_channel_id, user_id, order["user_id"], fill_qty, order["price"], "market")
                    await add_transaction(conn, guild_id, order["user_id"], cost, "market_sell", f"Sold {fill_qty}x {company['name']}")
                    bought += fill_qty
                    total_cost += cost
                    remaining_funds -= cost
                    order_rems[order["id"]] -= fill_qty

            if bought > 0:
                await add_transaction(conn, guild_id, user_id, -total_cost, "market_buy", f"Bought {bought}x {company['name']}")

    if bought == 0:
        raise UserError(f"Could not buy any shares of **{company['name']}**. No shares available or insufficient funds.")
    return TradeResult(company_name=company["name"], quantity=quantity, filled=bought, total=total_cost)


async def market_sell(pool, guild_id: int, user_id: int, stock_channel_id: int,
                      quantity: int, channel_id: int) -> TradeResult:
    """Sell shares immediately into the best open buy orders."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    _require_positive(quantity)
    company = await _require_company(pool, guild_id, stock_channel_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            holding = await lock_holding(conn, guild_id, user_id, stock_channel_id)
            escrowed = await get_escrowed_shares(conn, guild_id, user_id, stock_channel_id)
            available = holding - escrowed
            if available < quantity:
                raise UserError(f"You only have {available:,} available shares of **{company['name']}** ({holding:,} held, {escrowed:,} in open sell orders).")

            sold = 0
            total_revenue = 0

            buy_orders = await get_open_orders_locked(conn, guild_id, stock_channel_id, "buy")
            for order in buy_orders:
                if sold >= quantity:
                    break
                if order["user_id"] == user_id:
                    continue

                fill_qty = min(quantity - sold, order["remaining"])
                revenue = fill_qty * order["price"]

                await update_wallet(conn, guild_id, user_id, revenue)
                try:
                    await update_holding(conn, guild_id, user_id, stock_channel_id, -fill_qty)
                except ValueError:
                    continue
                await update_holding(conn, guild_id, order["user_id"], stock_channel_id, fill_qty)
                await conn.execute(
                    "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                    order["id"], fill_qty,
                )
                await add_trade(conn, guild_id, stock_channel_id, order["user_id"], user_id, fill_qty, order["price"], "market")
                await add_transaction(conn, guild_id, order["user_id"], -revenue, "market_buy", f"Bought {fill_qty}x {company['name']}")

                sold += fill_qty
                total_revenue += revenue

            if sold > 0:
                await add_transaction(conn, guild_id, user_id, total_revenue, "market_sell", f"Sold {sold}x {company['name']}")

    if sold == 0:
        raise UserError(f"No buy orders available for **{company['name']}**. Place a sell order instead with `sellorder`.")
    return TradeResult(company_name=company["name"], quantity=quantity, filled=sold, total=total_revenue)


@dataclass
class OrderResult:
    company_name: str
    price: int
    filled: int      # shares traded immediately
    total: int       # spent (buy) or earned (sell) on the immediate fills
    remaining: int   # shares left resting on the book
    order_id: int | None = None  # set when remaining > 0


async def place_buy_order(pool, guild_id: int, user_id: int, stock_channel_id: int,
                          quantity: int, raw_price: str, channel_id: int) -> OrderResult:
    """Place a limit buy order; fills immediately against asks/IPO where possible."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    price = parse_amount(raw_price)
    _require_positive(quantity)
    company = await _require_company(pool, guild_id, stock_channel_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            total_cost = quantity * price
            await ensure_wallet(conn, guild_id, user_id)
            wallet = await lock_wallet(conn, guild_id, user_id)
            if wallet["wallet"] < total_cost:
                raise UserError(f"You need {total_cost:,} to place this order but only have {wallet['wallet']:,}.")

            await update_wallet(conn, guild_id, user_id, -total_cost)

            filled = 0
            spent = 0
            company_locked = await lock_company(conn, guild_id, stock_channel_id)
            sell_orders = await get_open_orders_locked(conn, guild_id, stock_channel_id, "sell")
            for order in sell_orders:
                if filled >= quantity:
                    break
                if order["user_id"] == user_id:
                    continue
                if order["price"] > price:
                    break

                fill_qty = min(quantity - filled, order["remaining"])

                # Lock seller's portfolio and cap fill_qty at what they actually hold
                seller_qty = await conn.fetchval(
                    "SELECT COALESCE(quantity, 0) FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3 FOR UPDATE",
                    guild_id, order["user_id"], stock_channel_id,
                )
                fill_qty = min(fill_qty, seller_qty or 0)
                if fill_qty <= 0:
                    continue

                fill_cost = fill_qty * order["price"]

                await ensure_wallet(conn, guild_id, order["user_id"])
                await update_wallet(conn, guild_id, order["user_id"], fill_cost)
                refund = fill_qty * (price - order["price"])
                if refund > 0:
                    await update_wallet(conn, guild_id, user_id, refund)

                try:
                    await update_holding(conn, guild_id, order["user_id"], stock_channel_id, -fill_qty)
                except ValueError:
                    continue
                await update_holding(conn, guild_id, user_id, stock_channel_id, fill_qty)
                await conn.execute(
                    "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                    order["id"], fill_qty,
                )
                await add_trade(conn, guild_id, stock_channel_id, user_id, order["user_id"], fill_qty, order["price"], "limit")
                await add_transaction(conn, guild_id, order["user_id"], fill_cost, "market_sell", f"Sold {fill_qty}x {company['name']} via limit")

                filled += fill_qty
                spent += fill_cost

            # Fill remaining quantity from IPO if price allows
            ipo_rem = company_locked["available_ipo_shares"]
            ipo_price_val = company_locked["ipo_price"]
            if filled < quantity and ipo_rem > 0 and ipo_price_val <= price:
                fill_qty = min(quantity - filled, ipo_rem)
                fill_cost = fill_qty * ipo_price_val
                refund = fill_qty * (price - ipo_price_val)
                if refund > 0:
                    await update_wallet(conn, guild_id, user_id, refund)
                await update_holding(conn, guild_id, user_id, stock_channel_id, fill_qty)
                await conn.execute(
                    "UPDATE companies SET available_ipo_shares = available_ipo_shares - $3 WHERE guild_id = $1 AND stock_channel_id = $2",
                    guild_id, stock_channel_id, fill_qty,
                )
                await add_trade(conn, guild_id, stock_channel_id, user_id, None, fill_qty, ipo_price_val, "ipo")
                await update_treasury(conn, guild_id, stock_channel_id, fill_cost)
                filled += fill_qty
                spent += fill_cost

            remaining = quantity - filled
            order_id = None
            if remaining > 0:
                row = await create_order(conn, guild_id, stock_channel_id, user_id, "buy", remaining, price)
                order_id = row["id"]
            if filled > 0:
                await add_transaction(conn, guild_id, user_id, -spent, "market_buy", f"Bought {filled}x {company['name']} via limit")

    return OrderResult(company_name=company["name"], price=price, filled=filled,
                       total=spent, remaining=remaining, order_id=order_id)


async def place_sell_order(pool, guild_id: int, user_id: int, stock_channel_id: int,
                           quantity: int, raw_price: str, channel_id: int) -> OrderResult:
    """Place a limit sell order; fills immediately against bids where possible."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    price = parse_amount(raw_price)
    _require_positive(quantity)
    company = await _require_company(pool, guild_id, stock_channel_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            holding = await lock_holding(conn, guild_id, user_id, stock_channel_id)
            escrowed = await get_escrowed_shares(conn, guild_id, user_id, stock_channel_id)
            available = holding - escrowed
            if available < quantity:
                raise UserError(f"You only have {available:,} available shares of **{company['name']}** ({holding:,} held, {escrowed:,} in open sell orders).")

            filled = 0
            revenue = 0
            buy_orders = await get_open_orders_locked(conn, guild_id, stock_channel_id, "buy")
            for order in buy_orders:
                if filled >= quantity:
                    break
                if order["user_id"] == user_id:
                    continue
                if order["price"] < price:
                    break

                fill_qty = min(quantity - filled, order["remaining"])
                fill_revenue = fill_qty * order["price"]

                await update_wallet(conn, guild_id, user_id, fill_revenue)
                try:
                    await update_holding(conn, guild_id, user_id, stock_channel_id, -fill_qty)
                except ValueError:
                    continue
                await update_holding(conn, guild_id, order["user_id"], stock_channel_id, fill_qty)
                await conn.execute(
                    "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                    order["id"], fill_qty,
                )
                await add_trade(conn, guild_id, stock_channel_id, order["user_id"], user_id, fill_qty, order["price"], "limit")
                await add_transaction(conn, guild_id, order["user_id"], -fill_revenue, "market_buy", f"Bought {fill_qty}x {company['name']} via limit")

                filled += fill_qty
                revenue += fill_revenue

            remaining = quantity - filled
            order_id = None
            if remaining > 0:
                row = await create_order(conn, guild_id, stock_channel_id, user_id, "sell", remaining, price)
                order_id = row["id"]
            if filled > 0:
                await add_transaction(conn, guild_id, user_id, revenue, "market_sell", f"Sold {filled}x {company['name']} via limit")

    return OrderResult(company_name=company["name"], price=price, filled=filled,
                       total=revenue, remaining=remaining, order_id=order_id)


async def cancel_user_order(pool, guild_id: int, user_id: int, order_id: int,
                            channel_id: int):
    """Cancel one of the user's open orders. Returns (order_row, refund)."""
    await _ensure_can_trade(pool, guild_id, user_id, channel_id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            order = await cancel_order(conn, guild_id, order_id, user_id)
            if not order:
                raise UserError("Order not found or already filled.")

            refund = 0
            if order["side"] == "buy":
                refund = order["remaining"] * order["price"]
                await update_wallet(conn, guild_id, user_id, refund)
    return order, refund


async def gift_stocks(pool, guild_id: int, giver: discord.Member, recipient: discord.Member,
                      stock_channel_id: int, quantity: int, channel_id: int) -> str:
    """Move shares giver → recipient for free. Returns the company name."""
    await _ensure_can_trade(pool, guild_id, giver.id, channel_id)
    if recipient.bot:
        raise UserError("You cannot gift stocks to a bot.")
    if recipient == giver:
        raise UserError("You cannot gift stocks to yourself.")
    _require_positive(quantity)
    company = await _require_company(pool, guild_id, stock_channel_id)

    async with pool.acquire() as conn:
        async with conn.transaction():
            holding = await lock_holding(conn, guild_id, giver.id, stock_channel_id)
            escrowed = await get_escrowed_shares(conn, guild_id, giver.id, stock_channel_id)
            available = holding - escrowed
            if available < quantity:
                raise UserError(
                    f"You only have {available:,} available shares of **{company['name']}** "
                    f"({holding:,} held, {escrowed:,} in open sell orders)."
                )

            await update_holding(conn, guild_id, giver.id, stock_channel_id, -quantity)
            await update_holding(conn, guild_id, recipient.id, stock_channel_id, quantity)
            await add_transaction(conn, guild_id, giver.id, 0, "gift_send",
                                  f"Gifted {quantity}x {company['name']} to {format_name(recipient)}")
            await add_transaction(conn, guild_id, recipient.id, 0, "gift_receive",
                                  f"Received {quantity}x {company['name']} from {format_name(giver)}")
    return company["name"]
