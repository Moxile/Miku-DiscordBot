"""Buy-only European options on the real stocks from cogs/realstocks.

Premiums are priced with Black-Scholes (cogs/options/pricing.py) since Finnhub's
free tier has no option chain. A bought option cash-settles at expiry for its
intrinsic value, or the holder can close it early for its current fair value; a
background loop settles positions once they reach expiry. Live quotes come from
the RealStocks cog's shared QuoteService.
"""

from __future__ import annotations

import re

import discord
from discord.ext import commands, tasks

from cogs.options import service
from core.checks import require_channel, require_not_locked
from core.errors import UserError
from core.names import format_name
from config import OPTION_REFRESH_MINUTES

SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,15}$")

TYPE_ALIASES = {"call": "call", "c": "call", "put": "put", "p": "put"}
STATUS_LABELS = {"exercised": "✅ exercised", "expired": "❌ expired worthless", "closed": "closed early"}


def _type_emoji(opt_type: str) -> str:
    return "📈" if opt_type == "call" else "📉"


class Options(commands.Cog):
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
        cog = self.bot.get_cog("RealStocks")
        if cog is None:
            raise UserError("Options trading is currently unavailable (real stocks are offline).")
        return cog.quotes

    @staticmethod
    def _normalize_symbol(symbol: str) -> str | None:
        symbol = symbol.upper().strip()
        return symbol if SYMBOL_RE.match(symbol) else None

    # ── Background: expiry settlement ──

    @tasks.loop(minutes=OPTION_REFRESH_MINUTES)
    async def settle_loop(self):
        realstocks = self.bot.get_cog("RealStocks")
        if realstocks is None or not realstocks.quotes.configured:
            return
        await service.settle_expired(self.pool, realstocks.quotes)

    @settle_loop.before_loop
    async def before_settle(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                from cogs.options.db import remove_member_data
                await remove_member_data(conn, member.guild.id, member.id)

    # ── Commands ──

    @commands.command(aliases=["optquote", "oquote"])
    @require_not_locked()
    async def optprice(self, ctx, symbol: str, opt_type: str, strike: float, days: int,
                       contracts: int = 1):
        """Preview an option premium without buying. Usage: .optprice NVDA call 200 30"""
        parsed = await self._prepare(ctx, symbol, opt_type)
        if parsed is None:
            return
        symbol, opt_type = parsed
        q = await service.quote_premium(self.pool, self.quotes, ctx.guild.id, symbol,
                                        opt_type, strike, days, contracts)

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title=f"{_type_emoji(opt_type)} {q.stock_name} ({symbol}) "
                                    f"{strike:g} {opt_type.title()}",
                              color=discord.Color.gold())
        embed.add_field(name="Spot", value=f"${q.spot:,.2f}", inline=True)
        embed.add_field(name="Expiry", value=f"{days} day(s)", inline=True)
        embed.add_field(name="Implied Vol", value=f"{q.iv * 100:.0f}%", inline=True)
        embed.add_field(name="Premium / Contract", value=f"{q.premium_per_contract:,}{cur.emoji}", inline=True)
        embed.add_field(name=f"Total ({contracts}x)", value=f"{q.total_cost:,}{cur.emoji}", inline=True)
        embed.set_footer(text=f"Buy with .optbuy {symbol} {opt_type} {strike:g} {days} {contracts}")
        await ctx.send(embed=embed)

    @commands.command(aliases=["obuy"])
    @require_not_locked()
    @require_channel("trading_channel")
    async def optbuy(self, ctx, symbol: str, opt_type: str, strike: float, days: int,
                     contracts: int = 1):
        """Buy a European option on a real stock. Usage: .optbuy NVDA call 200 30 2

        `opt_type` is call or put, `strike` is the strike price in USD, `days` is how
        many days until expiry, and `contracts` is how many to buy (100 shares each)."""
        parsed = await self._prepare(ctx, symbol, opt_type)
        if parsed is None:
            return
        symbol, opt_type = parsed
        result = await service.buy(self.pool, self.quotes, ctx.guild.id, ctx.author.id,
                                   symbol, opt_type, strike, days, contracts, ctx.channel.id)

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title=f"{_type_emoji(opt_type)} Option Bought — #{result.position_id}",
                              color=discord.Color.green())
        embed.add_field(name="Contract",
                        value=f"{result.contracts}x {result.stock_name} ({symbol}) "
                              f"{result.strike:g} {opt_type.title()}", inline=False)
        embed.add_field(name="Spot at Buy", value=f"${result.spot:,.2f}", inline=True)
        embed.add_field(name="Expiry", value=f"<t:{int(result.expiry.timestamp())}:R>", inline=True)
        embed.add_field(name="Implied Vol", value=f"{result.iv * 100:.0f}%", inline=True)
        embed.add_field(name="Premium Paid", value=f"{result.total_cost:,}{cur.emoji}", inline=True)
        embed.set_footer(text=f"Close early with .optclose {result.position_id}")
        await ctx.send(embed=embed)

    @commands.command(aliases=["oclose"])
    @require_not_locked()
    @require_channel("trading_channel")
    async def optclose(self, ctx, position_id: int):
        """Close an open option early at its current fair value. Usage: .optclose 42"""
        result = await service.close(self.pool, self.quotes, ctx.guild.id, ctx.author.id,
                                     position_id, ctx.channel.id)

        cur = self.bot.get_currency(ctx.guild.id)
        won = result.realized_pl >= 0
        embed = discord.Embed(title=f"Option Closed — #{position_id}",
                              color=discord.Color.green() if won else discord.Color.red())
        embed.add_field(name="Contract",
                        value=f"{result.contracts}x {result.symbol} {result.strike:g} "
                              f"{result.opt_type.title()}", inline=False)
        embed.add_field(name="Spot", value=f"${result.spot:,.2f}", inline=True)
        embed.add_field(name="Premium Paid", value=f"{result.premium_paid:,}{cur.emoji}", inline=True)
        embed.add_field(name="Payout", value=f"{result.payout:,}{cur.emoji}", inline=True)
        pl_str = f"+{result.realized_pl:,}" if won else f"{result.realized_pl:,}"
        embed.add_field(name="Net P/L", value=f"**{pl_str}**{cur.emoji}", inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=["opos", "optportfolio"])
    @require_not_locked()
    async def optpositions(self, ctx, member: discord.Member = None):
        """Show open option positions with live value. Use with a mention to see others'."""
        member = member or ctx.author
        cur = self.bot.get_currency(ctx.guild.id)
        rows = await service.list_open_positions(self.pool, self.quotes, ctx.guild.id, member.id)
        if not rows:
            await ctx.send(f"{format_name(member)} has no open option positions.")
            return

        embed = discord.Embed(title=f"🎟️ {format_name(member)}'s Options", color=discord.Color.gold())
        total_value = 0
        for r in rows:
            head = (f"**#{r['id']}** {_type_emoji(r['opt_type'])} {r['contracts']}x {r['symbol']} "
                    f"{r['strike']:g} {r['opt_type'].title()}")
            expiry_txt = f"<t:{int(r['expiry'].timestamp())}:R>"
            if r["value"] is None:
                embed.add_field(name=head, value=f"exp {expiry_txt} · mark unavailable", inline=False)
                continue
            total_value += r["value"]
            pl_str = f"+{r['pl']:,}" if r["pl"] >= 0 else f"{r['pl']:,}"
            embed.add_field(
                name=head,
                value=(f"spot ${r['spot']:,.2f} · exp {expiry_txt}\n"
                       f"Paid {r['premium_paid']:,}{cur.emoji} · value {r['value']:,}{cur.emoji} · "
                       f"P/L **{pl_str}**{cur.emoji}"),
                inline=False)
        embed.set_footer(text=f"Total mark value: {total_value:,}")
        await ctx.send(embed=embed)

    @commands.command(aliases=["ohist", "opthist"])
    async def opthistory(self, ctx, member: discord.Member = None):
        """Show a member's settled option positions. Usage: .opthistory [@member]"""
        member = member or ctx.author
        cur = self.bot.get_currency(ctx.guild.id)
        rows = await service.position_history(self.pool, ctx.guild.id, member.id)
        if not rows:
            await ctx.send(f"{format_name(member)} has no settled option positions.")
            return

        embed = discord.Embed(title=f"📜 {format_name(member)}'s Option History", color=discord.Color.gold())
        for r in rows[:12]:
            tag = STATUS_LABELS.get(r["status"], r["status"])
            pl_str = f"+{r['realized_pl']:,}" if r["realized_pl"] >= 0 else f"{r['realized_pl']:,}"
            settle = f"${r['settle_spot']:,.2f}" if r["settle_spot"] is not None else "—"
            embed.add_field(
                name=f"{_type_emoji(r['opt_type'])} {r['contracts']}x {r['symbol']} "
                     f"{r['strike']:g} {r['opt_type'].title()} — {tag}",
                value=(f"paid {r['premium_paid']:,}{cur.emoji} → payout {r['payout']:,}{cur.emoji} "
                       f"(settle {settle})\nP/L: **{pl_str}**{cur.emoji}"),
                inline=False)
        await ctx.send(embed=embed)

    # ── Helpers ──

    async def _prepare(self, ctx, symbol: str, opt_type: str):
        """Validate and normalize the ticker + option type, sending a friendly error
        and returning None on bad input."""
        norm = self._normalize_symbol(symbol)
        if not norm:
            await ctx.send("That doesn't look like a ticker symbol.")
            return None
        kind = TYPE_ALIASES.get(opt_type.lower())
        if kind is None:
            await ctx.send("Option type must be `call` or `put`.")
            return None
        return norm, kind
