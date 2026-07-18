"""Leveraged CFD trading on the real stocks from cogs/realstocks.

Positions are cash-settled against the void, exactly like the spot market:
opening a position locks `notional / leverage` coins of margin, and closing
returns that margin plus profit (or minus loss, floored at zero). A background
loop accrues overnight financing and auto-liquidates positions whose equity
falls to the maintenance margin. See cfd/service.py for the pricing math.

Live quotes come from the RealStocks cog's shared QuoteService — this cog does
not open its own Finnhub client.
"""

from __future__ import annotations

import re

import discord
from discord.ext import commands, tasks

from cogs.cfd import service
from cogs.realstocks.quotes import QuoteError
from core.checks import require_channel, require_not_locked
from core.errors import UserError
from core.names import format_name
from core.time_utils import humanize_duration
from config import CFD_REFRESH_MINUTES

SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,15}$")
POSITIONS_PER_PAGE = 6

DIRECTION_ALIASES = {
    "long": "long", "l": "long", "buy": "long", "b": "long",
    "short": "short", "s": "short", "sell": "short",
}


def _dir_emoji(direction: str) -> str:
    return "🟢" if direction == "long" else "🔴"


class CFDTrading(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.settle_loop.start()

    def cog_unload(self):
        self.settle_loop.cancel()

    @property
    def pool(self):
        return self.bot.pool

    @property
    def quotes(self):
        """RealStocks' shared QuoteService, or raise a friendly error if that cog
        isn't loaded."""
        cog = self.bot.get_cog("RealStocks")
        if cog is None:
            raise UserError("CFD trading is currently unavailable (real stocks are offline).")
        return cog.quotes

    @staticmethod
    def _normalize_symbol(symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        return symbol if SYMBOL_RE.match(symbol) else None

    # ── Background: financing accrual + liquidation ──

    @tasks.loop(minutes=CFD_REFRESH_MINUTES)
    async def settle_loop(self):
        """Accrue overnight financing and liquidate underwater positions. Reuses the
        shared quote cache, so it costs at most one API call per symbol per TTL window."""
        realstocks = self.bot.get_cog("RealStocks")
        if realstocks is None or not realstocks.quotes.configured:
            return
        quotes = realstocks.quotes
        positions = await service.all_open_positions(self.pool)
        # One quote per distinct symbol (cached), then process each position on it.
        prices: dict[str, float] = {}
        for pos in positions:
            symbol = pos["symbol"]
            if symbol not in prices:
                try:
                    quote = await quotes.get_quote(symbol)
                except QuoteError:
                    prices[symbol] = None
                else:
                    prices[symbol] = quote.price
            price = prices[symbol]
            if price is None:
                continue
            result = await service.settle_or_accrue(self.pool, pos, price)
            if result.margin_call:
                await self._send_margin_call_dm(result)

    async def _send_margin_call_dm(self, result: service.SettleResult):
        """Warn a trader by DM once their equity drops to the margin-call level, well
        before liquidation. Silently drops the DM if the user has DMs closed."""
        try:
            user = await self.bot.fetch_user(result.user_id)
        except discord.HTTPException:
            return
        cur = self.bot.get_currency(result.guild_id)
        embed = discord.Embed(
            title="⚠️ Margin Call",
            description=(f"Your {result.direction} {result.symbol} CFD position is running low "
                        f"on equity and is at risk of liquidation."),
            color=discord.Color.orange())
        embed.add_field(name="Equity", value=f"{result.equity:,.0f}{cur.emoji}", inline=True)
        embed.add_field(name="Margin", value=f"{result.margin:,}{cur.emoji}", inline=True)
        embed.set_footer(text="Close the position or let it ride — no further action is required.")
        try:
            await user.send(embed=embed)
        except (discord.Forbidden, discord.HTTPException):
            pass

    @settle_loop.before_loop
    async def before_settle(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                from cogs.cfd.db import remove_member_data
                await remove_member_data(conn, member.guild.id, member.id)

    # ── Commands ──

    @commands.command(aliases=["cfd"])
    @require_not_locked()
    @require_channel("trading_channel")
    async def cfdopen(self, ctx, symbol: str, direction: str, notional: int, leverage: int = 1):
        """Open a leveraged CFD on a real stock. Usage: .cfdopen NVDA long 1000 5

        `direction` is long or short, `notional` is your total exposure in coins, and
        `leverage` sets how much margin you post (notional / leverage)."""
        symbol = self._normalize_symbol(symbol)
        if not symbol:
            await ctx.send("That doesn't look like a ticker symbol.")
            return
        direction = DIRECTION_ALIASES.get(direction.lower())
        if direction is None:
            await ctx.send("Direction must be `long` or `short`.")
            return

        result = await service.open_position(self.pool, self.quotes, ctx.guild.id, ctx.author.id,
                                              symbol, direction, notional, leverage, ctx.channel.id)

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(
            title=f"{_dir_emoji(direction)} CFD Opened — #{result.position_id}",
            color=discord.Color.green() if direction == "long" else discord.Color.red())
        embed.add_field(name="Stock", value=f"{result.stock_name} ({symbol})", inline=True)
        embed.add_field(name="Direction", value=f"{direction.title()} · {result.leverage}x", inline=True)
        embed.add_field(name="Notional", value=f"{result.notional:,}{cur.emoji}", inline=True)
        embed.add_field(name="Margin Locked", value=f"{result.margin:,}{cur.emoji}", inline=True)
        embed.add_field(name="Entry Price", value=f"${result.entry_price:,.2f}", inline=True)
        embed.add_field(name="Liquidation", value=f"${result.liquidation_price:,.2f}", inline=True)
        embed.set_footer(text=f"Close with .cfdclose {result.position_id}")
        await ctx.send(embed=embed)

    @commands.command()
    @require_not_locked()
    @require_channel("trading_channel")
    async def cfdclose(self, ctx, position_id: int):
        """Close one of your open CFD positions at the live price. Usage: .cfdclose 42"""
        result = await service.close_position(self.pool, self.quotes, ctx.guild.id,
                                              ctx.author.id, position_id, ctx.channel.id)

        cur = self.bot.get_currency(ctx.guild.id)
        won = result.realized_pl >= 0
        embed = discord.Embed(title=f"CFD Closed — #{position_id}",
                              color=discord.Color.green() if won else discord.Color.red())
        embed.add_field(name="Stock", value=f"{result.stock_name} ({result.symbol})", inline=True)
        embed.add_field(name="Direction", value=f"{result.direction.title()} · {result.leverage}x", inline=True)
        embed.add_field(name="Entry → Close",
                        value=f"${result.entry_price:,.2f} → ${result.close_price:,.2f}", inline=True)
        if result.financing:
            embed.add_field(name="Financing", value=f"-{result.financing:,}{cur.emoji}", inline=True)
        embed.add_field(name="Payout", value=f"{result.payout:,}{cur.emoji}", inline=True)
        pl_str = f"+{result.realized_pl:,}" if won else f"{result.realized_pl:,}"
        embed.add_field(name="Net P/L", value=f"**{pl_str}**{cur.emoji}", inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=["cfdpos", "cfdportfolio"])
    @require_not_locked()
    async def cfdpositions(self, ctx, member: discord.Member = None):
        """Show open CFD positions with live P/L. Use with a mention to see others'."""
        member = member or ctx.author
        cur = self.bot.get_currency(ctx.guild.id)
        rows = await service.list_open_positions(self.pool, self.quotes, ctx.guild.id, member.id)
        if not rows:
            await ctx.send(f"{format_name(member)} has no open CFD positions.")
            return

        embed = discord.Embed(title=f"📊 {format_name(member)}'s CFD Positions",
                              color=discord.Color.blurple())
        total_equity = 0
        for r in rows:
            head = (f"**#{r['id']}** {_dir_emoji(r['direction'])} {r['direction'].title()} "
                    f"{r['leverage']}x {r['symbol']} — {r['notional']:,}{cur.emoji} notional")
            if r["price"] is None:
                embed.add_field(name=head, value="Mark price unavailable", inline=False)
                continue
            total_equity += r["equity"]
            pl_str = f"+{r['pl']:,}" if r["pl"] >= 0 else f"{r['pl']:,}"
            fin = f" · financing -{r['financing']:,}" if r["financing"] else ""
            embed.add_field(
                name=head,
                value=(f"Entry ${r['entry_price']:,.2f} → mark ${r['price']:,.2f} · "
                       f"liq ${r['liquidation_price']:,.2f}\n"
                       f"Margin {r['margin']:,}{cur.emoji}{fin} · "
                       f"P/L **{pl_str}**{cur.emoji} · equity {r['equity']:,}{cur.emoji}"),
                inline=False)
        embed.set_footer(text=f"Total equity across positions: {total_equity:,}")
        await ctx.send(embed=embed)

    @commands.command(aliases=["cfdhist"])
    async def cfdhistory(self, ctx, member: discord.Member = None):
        """Show a member's settled (closed or liquidated) CFD positions. Usage: .cfdhistory [@member]"""
        member = member or ctx.author
        cur = self.bot.get_currency(ctx.guild.id)
        rows = await service.position_history(self.pool, ctx.guild.id, member.id)
        if not rows:
            await ctx.send(f"{format_name(member)} has no settled CFD positions.")
            return

        embed = discord.Embed(title=f"📜 {format_name(member)}'s CFD History",
                              color=discord.Color.blurple())
        for r in rows[:POSITIONS_PER_PAGE * 2]:
            tag = "💥 liquidated" if r["status"] == "liquidated" else "closed"
            pl_str = f"+{r['realized_pl']:,}" if r["realized_pl"] >= 0 else f"{r['realized_pl']:,}"
            held = humanize_duration(int(r["hold_seconds"]), short=True)
            embed.add_field(
                name=f"{_dir_emoji(r['direction'])} {r['direction'].title()} {r['leverage']}x "
                     f"{r['symbol']} — {tag}",
                value=(f"${r['entry_price']:,.2f} → ${r['close_price']:,.2f} "
                       f"({r['notional']:,}{cur.emoji} notional, held {held})\n"
                       f"P/L: **{pl_str}**{cur.emoji}"),
                inline=False)
        await ctx.send(embed=embed)
