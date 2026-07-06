"""Real-world stocks traded with bot currency, alongside the simulated market.

Prices come from Finnhub (last traded price, shared cache across guilds) and
users trade as price-takers: buys and sells execute instantly against the live
quote, with coins going to / coming from the void — there is no order book,
since against a real-world price the market is effectively infinitely liquid.

Value is 1:1 with USD via per-symbol lot sizes (see quotes.py): one traded
"unit" is lot_size real shares, sized so a unit always costs a sensible whole
number of coins — penny stocks included.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import re

import discord
from discord.ext import commands, tasks

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.market.chart import render_price_chart
from cogs.realstocks.db import (
    get_symbol, create_symbol,
    enable_stock, disable_stock, get_guild_stock, list_guild_stocks, distinct_enabled_symbols,
    get_holding, lock_holding, update_holding, get_user_holdings, get_symbol_holders, remove_member_data,
    add_trade, get_avg_buy_price,
    record_price, get_last_recorded_price,
    get_price_history, get_price_history_since, get_last_price_before, PRICE_HISTORY_LIMIT,
)
from cogs.realstocks.quotes import (
    QuoteService, QuoteError, UnknownSymbolError,
    lot_size_for, unit_buy_price, unit_sell_price, unit_mid_price,
)
from core.checks import require_channel, require_not_locked
from core.confirm import confirm
from core.names import format_name
from config import REALSTOCK_QUOTE_TTL, REALSTOCK_REFRESH_MINUTES

SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,15}$")

# Time windows offered by the price-chart buttons: key -> (chart subtitle, days)
CHART_WINDOWS = {
    "daily": ("Past 24 hours", 1),
    "weekly": ("Past 7 days", 7),
    "monthly": ("Past 30 days", 30),
}


class RealChartView(discord.ui.View):
    """Daily / weekly / monthly / all-time toggles for a real stock's price chart."""

    def __init__(self, cog, symbol_row, embed, *, current="all", timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.symbol_row = symbol_row
        self.embed = embed
        self.current = current
        self.message = None
        self._sync_buttons()

    def _sync_buttons(self):
        self.daily_btn.disabled = self.current == "daily"
        self.weekly_btn.disabled = self.current == "weekly"
        self.monthly_btn.disabled = self.current == "monthly"
        self.all_btn.disabled = self.current == "all"

    async def _switch(self, interaction: discord.Interaction, key: str):
        self.current = key
        self._sync_buttons()
        file = await self.cog._render_window(self.symbol_row, key)
        if file is None:  # nothing to draw for this window
            await interaction.response.defer()
            return
        self.embed.set_image(url=f"attachment://price_{key}.png")
        await interaction.response.edit_message(attachments=[file], embed=self.embed, view=self)

    @discord.ui.button(label="Daily", style=discord.ButtonStyle.secondary)
    async def daily_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "daily")

    @discord.ui.button(label="Weekly", style=discord.ButtonStyle.secondary)
    async def weekly_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "weekly")

    @discord.ui.button(label="Monthly", style=discord.ButtonStyle.secondary)
    async def monthly_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "monthly")

    @discord.ui.button(label="All", style=discord.ButtonStyle.primary)
    async def all_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "all")

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class RealStocks(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.quotes = QuoteService(os.getenv("FINNHUB_API_KEY"), REALSTOCK_QUOTE_TTL)
        self.refresh_prices.start()

    def cog_unload(self):
        self.refresh_prices.cancel()
        asyncio.create_task(self.quotes.close())

    @property
    def pool(self):
        return self.bot.pool

    # ── Background price recording ──

    @tasks.loop(minutes=REALSTOCK_REFRESH_MINUTES)
    async def refresh_prices(self):
        """Refresh quotes for every enabled symbol and record changed prices for the charts."""
        if not self.quotes.configured:
            return
        symbols = await distinct_enabled_symbols(self.pool)
        for sym in symbols:
            try:
                quote = await self.quotes.get_quote(sym, max_age=REALSTOCK_REFRESH_MINUTES * 60 - 10)
            except QuoteError:
                continue  # transient API trouble — the next cycle retries
            row = await get_symbol(self.pool, sym)
            if row:
                await record_price(self.pool, sym, unit_mid_price(quote.price, row["lot_size"]))
            await asyncio.sleep(1.1)  # stay well under Finnhub's 60 calls/min

    @refresh_prices.before_loop
    async def before_refresh(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    # ── Helpers ──

    async def _fetch_quote(self, ctx, symbol: str):
        """Get a quote, or send a friendly error and return None."""
        if not self.quotes.configured:
            await ctx.send("Real-stock trading is not configured (missing `FINNHUB_API_KEY`).")
            return None
        try:
            return await self.quotes.get_quote(symbol)
        except UnknownSymbolError:
            await ctx.send(f"Unknown ticker: **{symbol}**.")
            return None
        except QuoteError as e:
            await ctx.send(str(e))
            return None

    async def _render_window(self, symbol_row, key):
        """Render the recorded-price chart for a window key ('daily'/'weekly'/'monthly'/'all').

        Returns a discord.File named ``price_<key>.png``, or None when nothing is recorded yet.
        Windowed views anchor the line at the last recorded price before the window so the
        chart spans the whole period even when the price barely moved.
        """
        symbol = symbol_row["symbol"]
        now = datetime.datetime.now(datetime.timezone.utc)
        if key == "all":
            history = await get_price_history(self.pool, symbol)
            if not history:
                return None
            points = [(r["recorded_at"], r["price"]) for r in history]
            period_label = "Since listing"
        else:
            period_label, days = CHART_WINDOWS[key]
            cutoff = max(now - datetime.timedelta(days=days), symbol_row["added_at"])
            rows = await get_price_history_since(self.pool, symbol, cutoff)
            anchor = await get_last_price_before(self.pool, symbol, cutoff)
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

    @staticmethod
    def _normalize_symbol(symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        return symbol if SYMBOL_RE.match(symbol) else None

    @staticmethod
    def _lot_note(lot_size: int) -> str:
        return "1 share" if lot_size == 1 else f"{lot_size:,} shares"

    # ── Owner commands ──

    @commands.command()
    @commands.is_owner()
    async def addstock(self, ctx, symbol: str):
        """Enable a real-world stock for this server. Usage: .addstock NVDA"""
        symbol = self._normalize_symbol(symbol)
        if not symbol:
            await ctx.send("That doesn't look like a ticker symbol (letters, digits, `.`, `-`).")
            return

        existing = await get_guild_stock(self.pool, ctx.guild.id, symbol)
        if existing:
            await ctx.send(f"**{symbol}** is already enabled here.")
            return

        quote = await self._fetch_quote(ctx, symbol)
        if quote is None:
            return

        sym_row = await get_symbol(self.pool, symbol)
        if not sym_row:
            name = await self.quotes.lookup_name(symbol) or symbol
            lot = lot_size_for(quote.price)
            await create_symbol(self.pool, symbol, name, lot)
            sym_row = await get_symbol(self.pool, symbol)
            await record_price(self.pool, symbol, unit_mid_price(quote.price, lot),
                               only_if_changed=False)
        await enable_stock(self.pool, ctx.guild.id, symbol, ctx.author.id)

        lot = sym_row["lot_size"]
        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Stock Enabled", color=discord.Color.green())
        embed.add_field(name="Company", value=f"{sym_row['name']} (**{symbol}**)", inline=False)
        embed.add_field(name="Real Price", value=f"${quote.price:,.2f}", inline=True)
        embed.add_field(name="Unit Size", value=self._lot_note(lot), inline=True)
        embed.add_field(name="Price per Unit", value=f"{unit_buy_price(quote.price, lot):,}{cur.emoji}", inline=True)
        embed.set_footer(text=f"Trade with .rbuy {symbol} / .rsell {symbol}")
        await ctx.send(embed=embed)

    @commands.command()
    @commands.is_owner()
    async def removestock(self, ctx, symbol: str):
        """Disable a real-world stock for this server, force-selling all holdings at the
        current price. Usage: .removestock NVDA"""
        symbol = self._normalize_symbol(symbol)
        if not symbol:
            await ctx.send("That doesn't look like a ticker symbol.")
            return
        stock = await get_guild_stock(self.pool, ctx.guild.id, symbol)
        if not stock:
            await ctx.send(f"**{symbol}** is not enabled here.")
            return

        holders = await get_symbol_holders(self.pool, ctx.guild.id, symbol)
        # Payout price: live quote when reachable, else the last recorded chart price.
        payout_price = None
        if self.quotes.configured:
            try:
                quote = await self.quotes.get_quote(symbol)
                payout_price = unit_sell_price(quote.price, stock["lot_size"])
            except QuoteError:
                pass
        if payout_price is None:
            payout_price = await get_last_recorded_price(self.pool, symbol) or 0

        total_units = sum(h["quantity"] for h in holders)
        prompt = (f"Remove **{stock['name']} ({symbol})** from this server? "
                  f"{len(holders)} holder(s) with {total_units:,} unit(s) will be "
                  f"force-sold at {payout_price:,} each.")
        if not await confirm(ctx, prompt):
            return

        cur = self.bot.get_currency(ctx.guild.id)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Re-fetch inside the transaction — holdings may have changed while confirming.
                holders = await get_symbol_holders(conn, ctx.guild.id, symbol)
                for h in holders:
                    payout = h["quantity"] * payout_price
                    if payout > 0:
                        await ensure_wallet(conn, ctx.guild.id, h["user_id"])
                        await update_wallet(conn, ctx.guild.id, h["user_id"], payout)
                        await add_transaction(conn, ctx.guild.id, h["user_id"], payout, "realstock_sell",
                                              f"Forced sale of {h['quantity']}x {symbol} units (delisted)")
                await disable_stock(conn, ctx.guild.id, symbol)

        paid = sum(h["quantity"] for h in holders) * payout_price
        await ctx.send(f"**{symbol}** removed. Paid out {paid:,}{cur.emoji} to {len(holders)} holder(s).")

    # ── Public commands ──

    @commands.command(aliases=['rs'])
    async def realstocks(self, ctx):
        """List the real-world stocks enabled on this server with live prices."""
        stocks = await list_guild_stocks(self.pool, ctx.guild.id)
        if not stocks:
            await ctx.send("No real stocks are enabled here yet. An owner can add one with `.addstock <ticker>`.")
            return

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Real Stock Market", color=discord.Color.blue())
        for s in stocks:
            try:
                quote = await self.quotes.get_quote(s["symbol"])
            except QuoteError:
                embed.add_field(name=f"{s['name']} ({s['symbol']})", value="Price unavailable", inline=False)
                continue
            lot = s["lot_size"]
            unit = unit_mid_price(quote.price, lot)
            pct = (quote.price - quote.prev_close) / quote.prev_close * 100 if quote.prev_close else 0
            arrow = "📈" if pct >= 0 else "📉"
            embed.add_field(
                name=f"{s['name']} ({s['symbol']})",
                value=f"{unit:,}{cur.emoji}/unit ({self._lot_note(lot)}, ${quote.price:,.2f}) "
                      f"{arrow} {pct:+.2f}% today",
                inline=False,
            )
        embed.set_footer(text="Buy with .rbuy <ticker> <qty> · details with .stockinfo <ticker>")
        await ctx.send(embed=embed)

    @commands.command(aliases=['si', 'rinfo'])
    async def stockinfo(self, ctx, symbol: str):
        """Show details and the price chart for an enabled real stock. Usage: .stockinfo NVDA"""
        symbol = self._normalize_symbol(symbol)
        stock = symbol and await get_guild_stock(self.pool, ctx.guild.id, symbol)
        if not stock:
            await ctx.send("This stock is not enabled here. See `.realstocks` for what is.")
            return

        quote = await self._fetch_quote(ctx, symbol)
        if quote is None:
            return
        await record_price(self.pool, symbol, unit_mid_price(quote.price, stock["lot_size"]))

        lot = stock["lot_size"]
        cur = self.bot.get_currency(ctx.guild.id)
        pct = (quote.price - quote.prev_close) / quote.prev_close * 100 if quote.prev_close else 0

        embed = discord.Embed(title=f"{stock['name']} ({symbol})", color=discord.Color.blue())
        embed.add_field(name="Real Price", value=f"${quote.price:,.2f} ({pct:+.2f}% today)", inline=True)
        embed.add_field(name="Unit Size", value=self._lot_note(lot), inline=True)
        embed.add_field(name="Buy / Sell per Unit",
                        value=f"{unit_buy_price(quote.price, lot):,}{cur.emoji} / "
                              f"{unit_sell_price(quote.price, lot):,}{cur.emoji}",
                        inline=True)

        held = await get_holding(self.pool, ctx.guild.id, ctx.author.id, symbol)
        if held:
            avg = await get_avg_buy_price(self.pool, ctx.guild.id, ctx.author.id, symbol)
            value = held * unit_sell_price(quote.price, lot)
            pl = value - held * avg
            pl_str = f"+{pl:,}" if pl >= 0 else f"{pl:,}"
            embed.add_field(name="Your Position",
                            value=f"{held:,} units @ avg {avg:,}{cur.emoji} — value {value:,}{cur.emoji} "
                                  f"(P/L {pl_str}{cur.emoji})",
                            inline=False)

        file = await self._render_window(stock, "all")
        if file is None:
            await ctx.send(embed=embed)
            return
        embed.set_image(url="attachment://price_all.png")
        view = RealChartView(self, stock, embed, current="all")
        view.message = await ctx.send(embed=embed, file=file, view=view)

    @commands.command(aliases=['rp', 'rport'])
    @require_not_locked()
    async def rportfolio(self, ctx, member: discord.Member = None):
        """Show real-stock holdings and P/L. Use with a member mention to see others'."""
        member = member or ctx.author
        holdings = await get_user_holdings(self.pool, ctx.guild.id, member.id)
        if not holdings:
            await ctx.send(f"{format_name(member)} holds no real stocks.")
            return

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title=f"{format_name(member)}'s Real Stocks", color=discord.Color.green())
        total_value = 0
        total_cost = 0
        for h in holdings:
            try:
                quote = await self.quotes.get_quote(h["symbol"])
                price = unit_sell_price(quote.price, h["lot_size"])
            except QuoteError:
                price = await get_last_recorded_price(self.pool, h["symbol"]) or 0
            avg = await get_avg_buy_price(self.pool, ctx.guild.id, member.id, h["symbol"])
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
        await ctx.send(embed=embed)

    @commands.command(aliases=['rb'])
    @require_not_locked()
    @require_channel("trading_channel")
    async def rbuy(self, ctx, symbol: str, quantity: int = 1):
        """Buy units of a real stock at the live price. Usage: .rbuy NVDA 5"""
        if quantity <= 0:
            await ctx.send("Quantity must be positive.")
            return
        symbol = self._normalize_symbol(symbol)
        stock = symbol and await get_guild_stock(self.pool, ctx.guild.id, symbol)
        if not stock:
            await ctx.send("This stock is not enabled here. See `.realstocks` for what is.")
            return

        quote = await self._fetch_quote(ctx, symbol)
        if quote is None:
            return
        unit_price = unit_buy_price(quote.price, stock["lot_size"])
        total = unit_price * quantity

        cur = self.bot.get_currency(ctx.guild.id)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                if wallet["wallet"] < total:
                    await ctx.send(f"You need {total:,}{cur.emoji} but only have {wallet['wallet']:,}{cur.emoji}.")
                    return
                await update_wallet(conn, ctx.guild.id, ctx.author.id, -total)
                await update_holding(conn, ctx.guild.id, ctx.author.id, symbol, quantity)
                await add_trade(conn, ctx.guild.id, ctx.author.id, symbol, "buy", quantity, unit_price)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, -total, "realstock_buy",
                                      f"Bought {quantity}x {symbol} units")

        embed = discord.Embed(title="Stock Buy", color=discord.Color.green())
        embed.add_field(name="Stock", value=f"{stock['name']} ({symbol})", inline=True)
        embed.add_field(name="Bought", value=f"{quantity:,} unit(s) ({self._lot_note(stock['lot_size'])} each)", inline=True)
        embed.add_field(name="Price per Unit", value=f"{unit_price:,}{cur.emoji}", inline=True)
        embed.add_field(name="Total Cost", value=f"{total:,}{cur.emoji}", inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=['rsl'])
    @require_not_locked()
    @require_channel("trading_channel")
    async def rsell(self, ctx, symbol: str, quantity: int = 1):
        """Sell units of a real stock at the live price. Usage: .rsell NVDA 5"""
        if quantity <= 0:
            await ctx.send("Quantity must be positive.")
            return
        symbol = self._normalize_symbol(symbol)
        stock = symbol and await get_guild_stock(self.pool, ctx.guild.id, symbol)
        if not stock:
            await ctx.send("This stock is not enabled here. See `.realstocks` for what is.")
            return

        quote = await self._fetch_quote(ctx, symbol)
        if quote is None:
            return
        unit_price = unit_sell_price(quote.price, stock["lot_size"])
        total = unit_price * quantity

        cur = self.bot.get_currency(ctx.guild.id)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                held = await lock_holding(conn, ctx.guild.id, ctx.author.id, symbol)
                if held < quantity:
                    await ctx.send(f"You only hold {held:,} unit(s) of **{symbol}**.")
                    return
                await update_holding(conn, ctx.guild.id, ctx.author.id, symbol, -quantity)
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                await update_wallet(conn, ctx.guild.id, ctx.author.id, total)
                await add_trade(conn, ctx.guild.id, ctx.author.id, symbol, "sell", quantity, unit_price)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, total, "realstock_sell",
                                      f"Sold {quantity}x {symbol} units")

        embed = discord.Embed(title="Stock Sell", color=discord.Color.red())
        embed.add_field(name="Stock", value=f"{stock['name']} ({symbol})", inline=True)
        embed.add_field(name="Sold", value=f"{quantity:,} unit(s)", inline=True)
        embed.add_field(name="Price per Unit", value=f"{unit_price:,}{cur.emoji}", inline=True)
        embed.add_field(name="Total Received", value=f"{total:,}{cur.emoji}", inline=True)
        await ctx.send(embed=embed)
