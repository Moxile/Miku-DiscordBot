from __future__ import annotations

"""Real-stock pages for the Miku Menu: live-price list → stock detail with
chart and buy/sell, plus the real-stock portfolio."""

import discord

from cogs.realstocks import service
from cogs.realstocks.db import (
    get_avg_buy_price, get_guild_stock, get_holding, get_last_recorded_price,
    get_user_holdings, list_guild_stocks,
)
from cogs.realstocks.quotes import QuoteError, unit_buy_price, unit_mid_price, unit_sell_price
from core.errors import UserError
from core.names import format_name
from core.ui import Page, QuantityModal


def _lot_note(lot_size: int) -> str:
    return "1 share" if lot_size == 1 else f"{lot_size:,} shares"


def _quotes(page: Page):
    cog = page.bot.get_cog("RealStocks")
    if cog is None:
        raise UserError("Real-stock trading is currently unavailable.")
    return cog.quotes


class RealStocksPage(Page):
    async def build(self):
        stocks = await list_guild_stocks(self.pool, self.guild.id)
        cur = self.currency
        quotes = _quotes(self)

        embed = discord.Embed(title="🌐 Real Stock Market", color=discord.Color.blue())
        if not stocks:
            embed.description = "No real stocks are enabled here yet. An owner can add one with `.addstock <ticker>`."
            return embed, [self.button("My Real Stocks", self._portfolio, emoji="💼",
                                       style=discord.ButtonStyle.primary, row=1)]

        for s in stocks:
            try:
                quote = await quotes.get_quote(s["symbol"])
            except QuoteError:
                embed.add_field(name=f"{s['name']} ({s['symbol']})", value="Price unavailable", inline=False)
                continue
            lot = s["lot_size"]
            unit = unit_mid_price(quote.price, lot)
            pct = (quote.price - quote.prev_close) / quote.prev_close * 100 if quote.prev_close else 0
            arrow = "📈" if pct >= 0 else "📉"
            embed.add_field(
                name=f"{s['name']} ({s['symbol']})",
                value=f"{unit:,}{cur.emoji}/unit ({_lot_note(lot)}, ${quote.price:,.2f}) "
                      f"{arrow} {pct:+.2f}% today",
                inline=False,
            )

        select = discord.ui.Select(
            placeholder="View a stock…",
            options=[
                discord.SelectOption(label=f"{s['name']} ({s['symbol']})"[:100], value=s["symbol"])
                for s in stocks[:25]
            ],
            row=0,
        )
        select.callback = self._pick_stock
        self._select = select

        items = [
            select,
            self.button("My Real Stocks", self._portfolio, emoji="💼",
                        style=discord.ButtonStyle.primary, row=1),
        ]
        return embed, items

    async def _pick_stock(self, interaction: discord.Interaction):
        await self.hub.push(interaction, RealStockPage(self.hub, self._select.values[0]))

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
