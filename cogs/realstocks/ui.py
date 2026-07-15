from __future__ import annotations

"""Real-stock pages for the Miku Menu: live-price list → stock detail with
chart and buy/sell, plus the real-stock portfolio."""

import math

import discord

from cogs.realstocks import service
from cogs.realstocks.db import (
    get_avg_buy_price, get_guild_stock, get_holding, get_last_recorded_price,
    get_user_holdings,
)
from cogs.realstocks.quotes import QuoteError, unit_buy_price, unit_sell_price
from core.errors import UserError
from core.names import format_name
from core.ui import Page, QuantityModal

STOCKS_PER_PAGE = 8


def _lot_note(lot_size: int) -> str:
    return "1 share" if lot_size == 1 else f"{lot_size:,} shares"


def _quotes(page: Page):
    cog = page.bot.get_cog("RealStocks")
    if cog is None:
        raise UserError("Real-stock trading is currently unavailable.")
    return cog.quotes


class RealStocksPage(Page):
    """Compact, paginated stock browser — sortable by name / top gainers / top losers.
    The jump-to select only lists the current page, so it always has room for all
    visible stocks even when there are more than Discord's 25-option limit overall."""

    def __init__(self, hub):
        super().__init__(hub)
        self.page_no = 0
        self.sort = "name"
        self._max_page = 0

    async def build(self):
        cur = self.currency
        quotes = _quotes(self)
        rows = await service.list_stock_rows(self.pool, quotes, self.guild.id, self.sort)

        embed = discord.Embed(title="🌐 Real Stock Market", color=discord.Color.blue())
        if not rows:
            embed.description = "No real stocks are enabled here yet. An owner can add one with `.addstock <ticker>`."
            return embed, [self.button("My Real Stocks", self._portfolio, emoji="💼",
                                       style=discord.ButtonStyle.primary, row=1)]

        self._max_page = max(0, math.ceil(len(rows) / STOCKS_PER_PAGE) - 1)
        self.page_no = min(self.page_no, self._max_page)
        page_rows = rows[self.page_no * STOCKS_PER_PAGE:(self.page_no + 1) * STOCKS_PER_PAGE]

        lines = []
        for r in page_rows:
            s = r["stock"]
            if not r["ok"]:
                lines.append(f"**{s['symbol']}** — {s['name']} — price unavailable")
                continue
            arrow = "📈" if r["pct"] >= 0 else "📉"
            lines.append(
                f"**{s['symbol']}** — {s['name']}\n"
                f"{r['unit']:,}{cur.emoji}/unit (${r['quote'].price:,.2f}) {arrow} {r['pct']:+.2f}% today"
            )
        embed.description = "\n".join(lines)
        embed.set_footer(text=f"Page {self.page_no + 1}/{self._max_page + 1} · "
                              f"sorted by {service.SORT_LABELS[self.sort]} · {len(rows)} stocks")

        select = discord.ui.Select(
            placeholder="View a stock…",
            options=[
                discord.SelectOption(label=f"{r['stock']['symbol']} — {r['stock']['name']}"[:100],
                                     value=r["stock"]["symbol"])
                for r in page_rows
            ],
            row=0,
        )
        select.callback = self._pick_stock
        self._select = select

        sort_select = discord.ui.Select(
            placeholder="Sort by…",
            options=[
                discord.SelectOption(label=label, value=key, default=key == self.sort)
                for key, label in service.SORT_LABELS.items()
            ],
            row=1,
        )
        sort_select.callback = self._pick_sort
        self._sort_select = sort_select

        items = [
            select,
            sort_select,
            self.button("◀ Prev", self._prev, row=2, disabled=self.page_no == 0),
            self.button("Next ▶", self._next, row=2, disabled=self.page_no >= self._max_page),
            self.button("My Real Stocks", self._portfolio, emoji="💼",
                        style=discord.ButtonStyle.primary, row=3),
        ]
        return embed, items

    async def _pick_stock(self, interaction: discord.Interaction):
        await self.hub.push(interaction, RealStockPage(self.hub, self._select.values[0]))

    async def _pick_sort(self, interaction: discord.Interaction):
        self.sort = self._sort_select.values[0]
        self.page_no = 0
        await self.hub.refresh(interaction)

    async def _prev(self, interaction: discord.Interaction):
        self.page_no = max(0, self.page_no - 1)
        await self.hub.refresh(interaction)

    async def _next(self, interaction: discord.Interaction):
        self.page_no = min(self._max_page, self.page_no + 1)
        await self.hub.refresh(interaction)

    async def _portfolio(self, interaction: discord.Interaction):
        await self.hub.push(interaction, RealPortfolioPage(self.hub))


class RealStockPage(Page):
    def __init__(self, hub, symbol: str):
        super().__init__(hub)
        self.symbol = symbol
        self.window = "all"

    async def build(self):
        stock = await get_guild_stock(self.pool, self.guild.id, self.symbol)
        if not stock:
            raise UserError("This stock is no longer enabled here.")
        quotes = _quotes(self)
        quote = await service.fetch_quote(quotes, self.symbol)
        await service.record_live_price(self.pool, quotes, stock)

        lot = stock["lot_size"]
        cur = self.currency
        pct = (quote.price - quote.prev_close) / quote.prev_close * 100 if quote.prev_close else 0

        embed = discord.Embed(title=f"{stock['name']} ({self.symbol})", color=discord.Color.blue())
        embed.add_field(name="Real Price", value=f"${quote.price:,.2f} ({pct:+.2f}% today)", inline=True)
        embed.add_field(name="Unit Size", value=_lot_note(lot), inline=True)
        embed.add_field(name="Buy / Sell per Unit",
                        value=f"{unit_buy_price(quote.price, lot):,}{cur.emoji} / "
                              f"{unit_sell_price(quote.price, lot):,}{cur.emoji}",
                        inline=True)

        info_lines = service.company_info_lines(stock)
        if info_lines:
            embed.add_field(name="Company Info", value="\n".join(info_lines), inline=False)

        held = await get_holding(self.pool, self.guild.id, self.user.id, self.symbol)
        if held:
            avg = await get_avg_buy_price(self.pool, self.guild.id, self.user.id, self.symbol)
            value = held * unit_sell_price(quote.price, lot)
            pl = value - held * avg
            pl_str = f"+{pl:,}" if pl >= 0 else f"{pl:,}"
            embed.add_field(name="Your Position",
                            value=f"{held:,} units @ avg {avg:,}{cur.emoji} — value {value:,}{cur.emoji} "
                                  f"(P/L {pl_str}{cur.emoji})",
                            inline=False)

        files = []
        file = await service.render_window(self.pool, stock, self.window)
        if file is not None:
            embed.set_image(url=f"attachment://price_{self.window}.png")
            files.append(file)

        window_select = discord.ui.Select(
            placeholder="Chart window…",
            options=[
                discord.SelectOption(label=label, value=key, default=key == self.window)
                for key, (label, _days) in service.CHART_WINDOWS.items()
            ] + [discord.SelectOption(label="Since listing", value="all", default=self.window == "all")],
            row=0,
        )
        window_select.callback = self._pick_window
        self._window_select = window_select

        items = [
            window_select,
            self.button("Buy", self._buy, emoji="🟢", style=discord.ButtonStyle.success, row=1),
            self.button("Sell", self._sell, emoji="🔴", style=discord.ButtonStyle.danger, row=1),
        ]
        return embed, items, files

    async def _pick_window(self, interaction: discord.Interaction):
        self.window = self._window_select.values[0]
        await self.hub.refresh(interaction)

    async def _buy(self, interaction: discord.Interaction):
        async def _do(modal_interaction, quantity):
            result = await service.buy(self.pool, _quotes(self), self.guild.id, self.user.id,
                                       self.symbol, quantity, self.session.channel_id)
            await self.hub.refresh(modal_interaction, notice=(
                f"🟢 Bought **{result.quantity:,} unit(s) of {result.symbol}** "
                f"@ {result.unit_price:,}{self.currency.emoji} = **{result.total:,}**{self.currency.emoji}."))
        await interaction.response.send_modal(QuantityModal(self.hub, title=f"Buy {self.symbol}", handler=_do))

    async def _sell(self, interaction: discord.Interaction):
        async def _do(modal_interaction, quantity):
            result = await service.sell(self.pool, _quotes(self), self.guild.id, self.user.id,
                                        self.symbol, quantity, self.session.channel_id)
            await self.hub.refresh(modal_interaction, notice=(
                f"🔴 Sold **{result.quantity:,} unit(s) of {result.symbol}** "
                f"@ {result.unit_price:,}{self.currency.emoji} = **{result.total:,}**{self.currency.emoji}."))
        await interaction.response.send_modal(QuantityModal(self.hub, title=f"Sell {self.symbol}", handler=_do))


class RealPortfolioPage(Page):
    async def build(self):
        holdings = await get_user_holdings(self.pool, self.guild.id, self.user.id)
        cur = self.currency
        quotes = _quotes(self)

        embed = discord.Embed(title=f"💼 {format_name(self.user)}'s Real Stocks",
                              color=discord.Color.green())
        if not holdings:
            embed.description = "You hold no real stocks."
            return embed, []

        total_value = 0
        total_cost = 0
        for h in holdings:
            try:
                quote = await quotes.get_quote(h["symbol"])
                price = unit_sell_price(quote.price, h["lot_size"])
            except QuoteError:
                price = await get_last_recorded_price(self.pool, h["symbol"]) or 0
            avg = await get_avg_buy_price(self.pool, self.guild.id, self.user.id, h["symbol"])
            value = h["quantity"] * price
            cost = h["quantity"] * avg
            pl = value - cost
            total_value += value
            total_cost += cost
            pl_str = f"+{pl:,}" if pl >= 0 else f"{pl:,}"
            embed.add_field(
                name=f"{h['name']} ({h['symbol']})",
                value=f"{h['quantity']:,} units @ {price:,}{cur.emoji} = {value:,}{cur.emoji}\n"
                      f"Avg cost: {avg:,}{cur.emoji} | P/L: {pl_str}{cur.emoji}",
                inline=False,
            )
        total_pl = total_value - total_cost
        total_pl_str = f"+{total_pl:,}" if total_pl >= 0 else f"{total_pl:,}"
        embed.add_field(name="Summary",
                        value=f"Total value: {total_value:,}{cur.emoji} | Total P/L: {total_pl_str}{cur.emoji}",
                        inline=False)
        return embed, []
