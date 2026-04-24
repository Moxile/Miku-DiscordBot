import asyncio
import datetime
import time
from collections import defaultdict

import discord
from discord.ext import commands, tasks

from cogs.utils.db import (
    ensure_wallet, update_wallet, add_transaction,
    get_company, list_companies, create_company, delete_company,
    get_portfolio, get_holding, update_holding,
    get_open_orders, get_open_orders_locked, get_user_orders, create_order, cancel_order, get_escrowed_shares,
    add_trade, get_last_trade_price,
    lock_wallet, lock_company,
    upsert_char_count, compute_daily_revenue,
    get_weekly_revenue, get_weekly_revenue_total,
    update_treasury, set_company_level, get_shareholders,
    get_avg_buy_price,
)
from cogs.utils.checks import require_channel, WrongChannel, invalidate
from cogs.utils.money import parse_amount, AmountError
from config import (
    MAIN_CURRENCY_EMOJI,
    LEVEL_BASE_THRESHOLD,
    COST_FACTOR,
    DIVIDEND_REVENUE_SHARE,
    LEVEL_UP_TREASURY_CONSUME,
)


class Market(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Character cache for batching messages
        self._char_buffer: dict[tuple, int] = defaultdict(int)
        self._buffer_lock = asyncio.Lock()

        # Cache with company channels to avoid hitting DB on every message
        self._company_channels: dict[int, set[int]] = {}
        self._cache_expiry: dict[int, float] = {}

        # Start background tasks
        self.flush_char_buffer.start()
        self.daily_revenue_task.start()
        self.wednesday_recap_task.start()
        self.sunday_financials_task.start()

    def cog_unload(self):
        self.flush_char_buffer.cancel()
        self.daily_revenue_task.cancel()
        self.wednesday_recap_task.cancel()
        self.sunday_financials_task.cancel()

    @property
    def pool(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), delete_after=10)
        else:
            raise error

    async def _get_company_channels(self, guild_id: int) -> set[int]:
        now = time.monotonic()
        if guild_id in self._company_channels and self._cache_expiry.get(guild_id, 0) > now:
            return self._company_channels[guild_id]
        companies = await list_companies(self.pool, guild_id)
        channels = {c["stock_channel_id"] for c in companies}
        self._company_channels[guild_id] = channels
        self._cache_expiry[guild_id] = now + 300  # 5 min TTL
        return channels

    # ── Activity tracking ──

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return
        if message.channel.id not in await self._get_company_channels(message.guild.id):
            return
        char_count = len(message.content)
        if char_count == 0:
            return
        today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        key = (message.guild.id, message.channel.id, message.author.id, today)
        async with self._buffer_lock:
            self._char_buffer[key] += char_count

    @tasks.loop(seconds=60)
    async def flush_char_buffer(self):
        async with self._buffer_lock:
            if not self._char_buffer:
                return
            buffer = dict(self._char_buffer)
            self._char_buffer.clear()
        for (guild_id, channel_id, user_id, date_str), count in buffer.items():
            activity_date = datetime.date.fromisoformat(date_str)
            await upsert_char_count(self.pool, guild_id, channel_id, user_id, activity_date, count)

    @flush_char_buffer.before_loop
    async def before_flush(self):
        await self.bot.wait_until_ready()

    # ── Scheduled tasks ──

    @tasks.loop(time=datetime.time(hour=0, minute=0, tzinfo=datetime.timezone.utc))
    async def daily_revenue_task(self):
        """Compute yesterday's revenue for all companies."""
        # Flush buffer first to ensure data is in DB
        await self.flush_char_buffer()
        yesterday = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).date()
        for guild in self.bot.guilds:
            companies = await list_companies(self.pool, guild.id)
            for company in companies: 
                await compute_daily_revenue(
                    self.pool, guild.id, company["stock_channel_id"],
                    yesterday, company["revenue_multiplier"],
                )

    @daily_revenue_task.before_loop
    async def before_daily(self):
        await self.bot.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=0, minute=5, tzinfo=datetime.timezone.utc))
    async def wednesday_recap_task(self):
        """Post mid-week revenue recap every Wednesday."""
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.weekday() != 2:  # Wednesday
            return
        monday = (now - datetime.timedelta(days=now.weekday())).date()
        yesterday = (now - datetime.timedelta(days=1)).date()

        for guild in self.bot.guilds:
            companies = await list_companies(self.pool, guild.id)
            for company in companies:
                channel = guild.get_channel(company["stock_channel_id"])
                if not channel:
                    continue
                daily_records = await get_weekly_revenue(
                    self.pool, guild.id, company["stock_channel_id"], monday, yesterday,
                )
                total_so_far = sum(r["revenue"] for r in daily_records)
                lines = [f"  {r['revenue_date'].strftime('%A')}: {r['revenue']}{MAIN_CURRENCY_EMOJI}" for r in daily_records]

                embed = discord.Embed(title=f"{company['name']} - Mid-Week Revenue Recap", color=discord.Color.gold())
                embed.add_field(name="Daily Breakdown", value="\n".join(lines) or "No revenue yet", inline=False)
                embed.add_field(name="Total So Far", value=f"{total_so_far}{MAIN_CURRENCY_EMOJI}", inline=True)
                embed.add_field(name="Treasury", value=f"{company['treasury']}{MAIN_CURRENCY_EMOJI}", inline=True)
                embed.add_field(name="Level", value=str(company["company_level"]), inline=True)
                await channel.send(embed=embed)

    @wednesday_recap_task.before_loop
    async def before_wednesday(self):
        await self.bot.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=0, minute=10, tzinfo=datetime.timezone.utc))
    async def sunday_financials_task(self):
        """Process weekly financials every Sunday."""
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.weekday() != 6:  # Sunday
            return
        monday = (now - datetime.timedelta(days=now.weekday())).date()
        saturday = (now - datetime.timedelta(days=1)).date()

        for guild in self.bot.guilds:
            companies = await list_companies(self.pool, guild.id)
            for comp in companies:
                channel = guild.get_channel(comp["stock_channel_id"])

                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        company = await lock_company(conn, guild.id, comp["stock_channel_id"])
                        weekly_revenue = await get_weekly_revenue_total(
                            conn, guild.id, company["stock_channel_id"], monday, saturday,
                        )
                        # --- OLD MODEL: dividends = 20% of profit, paid only when profit > 0 ---
                        # cost = int(0.05 * company["treasury"])
                        # profit = weekly_revenue - cost
                        #
                        # dividends_paid = 0
                        # dividend_per_share = 0
                        #
                        # if profit > 0:
                        #     dividend_pool = int(0.20 * profit)
                        #     dividend_per_share = dividend_pool // company["total_shares"]
                        #
                        #     if dividend_per_share > 0:
                        #         shareholders = await get_shareholders(conn, guild.id, company["stock_channel_id"])
                        #         for sh in shareholders:
                        #             payout = dividend_per_share * sh["quantity"]
                        #             await ensure_wallet(conn, guild.id, sh["user_id"])
                        #             await update_wallet(conn, guild.id, sh["user_id"], payout)
                        #             await add_transaction(conn, guild.id, sh["user_id"], payout, "dividend",
                        #                                   f"Dividend from {company['name']}")
                        #             dividends_paid += payout
                        #
                        #     # Treasury gets profit minus what was paid out
                        #     # (IPO shares' dividend portion stays in treasury implicitly)
                        #     await update_treasury(conn, guild.id, company["stock_channel_id"], profit - dividends_paid)
                        # else:
                        #     await update_treasury(conn, guild.id, company["stock_channel_id"], profit)

                        # --- NEW MODEL: dividends = 10% of revenue, always paid ---
                        cost = int(0.05 * company["treasury"])
                        dividend_pool = int(DIVIDEND_REVENUE_SHARE * weekly_revenue)
                        dividend_per_share = dividend_pool // company["total_shares"]
                        dividends_paid = 0
                        profit = weekly_revenue - cost  # kept for reporting

                        if dividend_per_share > 0:
                            shareholders = await get_shareholders(conn, guild.id, company["stock_channel_id"])
                            for sh in shareholders:
                                payout = dividend_per_share * sh["quantity"]
                                await ensure_wallet(conn, guild.id, sh["user_id"])
                                await update_wallet(conn, guild.id, sh["user_id"], payout)
                                await add_transaction(conn, guild.id, sh["user_id"], payout, "dividend",
                                                      f"Dividend from {company['name']}")
                                dividends_paid += payout

                        await update_treasury(conn, guild.id, company["stock_channel_id"],
                                              weekly_revenue - dividends_paid - cost)

                        # Level-up check
                        company = await lock_company(conn, guild.id, company["stock_channel_id"])
                        leveled_up = False
                        next_level = company["company_level"] + 1
                        threshold = LEVEL_BASE_THRESHOLD * (2 ** (next_level - 1))

                        if company["treasury"] >= threshold:
                            consume = int(LEVEL_UP_TREASURY_CONSUME * company["treasury"])
                            new_multiplier = company["revenue_multiplier"] * 2
                            await set_company_level(conn, guild.id, company["stock_channel_id"],
                                                     next_level, new_multiplier, consume)
                            leveled_up = True

                if channel:
                    updated = await get_company(self.pool, guild.id, comp["stock_channel_id"])
                    embed = discord.Embed(title=f"{company['name']} - Weekly Financial Summary", color=discord.Color.blue())
                    embed.add_field(name="Weekly Revenue", value=f"{weekly_revenue}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Operating Cost (5%)", value=f"{cost}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Profit", value=f"{profit}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Dividend/Share", value=f"{dividend_per_share}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Total Dividends Paid", value=f"{dividends_paid}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Treasury", value=f"{updated['treasury']}{MAIN_CURRENCY_EMOJI}", inline=True)
                    if leveled_up:
                        embed.add_field(
                            name="LEVEL UP!",
                            value=f"Level {next_level} reached!",
                            inline=False,
                        )
                    await channel.send(embed=embed)

    @sunday_financials_task.before_loop
    async def before_sunday(self):
        await self.bot.wait_until_ready()

    # ── List / browse ──

    @commands.command(aliases=['m', 'stocks', 'ex'])
    async def exchange(self, ctx):
        """List all companies on the exchange with best bid/ask and IPO availability."""
        companies = await list_companies(self.pool, ctx.guild.id)
        if not companies:
            await ctx.send("No companies are listed yet.")
            return

        embed = discord.Embed(title="Stock Exchange", color=discord.Color.blue())
        for c in companies:
            channel = ctx.guild.get_channel(c["stock_channel_id"])
            name = channel.mention if channel else c["name"]
            buy_orders = await get_open_orders(self.pool, ctx.guild.id, c["stock_channel_id"], "buy")
            sell_orders = await get_open_orders(self.pool, ctx.guild.id, c["stock_channel_id"], "sell")
            best_bid = f"{buy_orders[0]['price']}{MAIN_CURRENCY_EMOJI}" if buy_orders else "None"
            best_ask = f"{sell_orders[0]['price']}{MAIN_CURRENCY_EMOJI}" if sell_orders else "None"
            ipo_status = f" | IPO: {c['available_ipo_shares']}/{c['total_shares']} @ {c['ipo_price']}{MAIN_CURRENCY_EMOJI}" if c["available_ipo_shares"] > 0 else ""
            embed.add_field(name=f"{c['name']} ({name})", value=f"Bid: {best_bid} / Ask: {best_ask}{ipo_status}", inline=False)

        await ctx.send(embed=embed)

    @commands.command(aliases=['p', 'port'])
    async def portfolio(self, ctx, member: discord.Member = None):
        """Show your current holdings as well as how those evolve. Use with a member mention to see others portfolio."""
        member = member or ctx.author
        holdings = await get_portfolio(self.pool, ctx.guild.id, member.id)
        orders = await get_user_orders(self.pool, ctx.guild.id, member.id)
        if not holdings and not orders:
            await ctx.send(f"{member.display_name} has no holdings or open orders.")
            return

        embed = discord.Embed(title=f"{member.display_name}'s Portfolio", color=discord.Color.green())
        total_value = 0
        total_cost = 0
        total_divs = 0
        for h in holdings:
            company = await get_company(self.pool, ctx.guild.id, h["stock_channel_id"])
            name = company["name"] if company else str(h["stock_channel_id"])
            last_price = await get_last_trade_price(self.pool, ctx.guild.id, h["stock_channel_id"])
            price = last_price or (company["ipo_price"] if company else 0)
            avg_cost = await get_avg_buy_price(self.pool, ctx.guild.id, member.id, h["stock_channel_id"])
            value = h["quantity"] * price
            cost_basis = h["quantity"] * avg_cost
            pl = value - cost_basis
            total_value += value
            total_cost += cost_basis
            pl_str = f"+{pl}" if pl >= 0 else str(pl)
            # Dividends received for this stock
            div_row = await self.pool.fetchrow(
                """SELECT COALESCE(SUM(amount), 0) AS total FROM transactions
                   WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'dividend'
                     AND description = $3""",
                ctx.guild.id, member.id, f"Dividend from {name}",
            )
            divs = div_row["total"]
            total_divs += divs
            embed.add_field(
                name=name,
                value=f"{h['quantity']} shares @ {price}{MAIN_CURRENCY_EMOJI} = {value}{MAIN_CURRENCY_EMOJI}\n"
                      f"Avg cost: {avg_cost}{MAIN_CURRENCY_EMOJI} | P/L: {pl_str}{MAIN_CURRENCY_EMOJI}\n"
                      f"Dividends received: {divs}{MAIN_CURRENCY_EMOJI}",
                inline=False,
            )
        total_pl = total_value - total_cost
        total_pl_str = f"+{total_pl}" if total_pl >= 0 else str(total_pl)

        if orders:
            order_lines = []
            for o in orders:
                company = await get_company(self.pool, ctx.guild.id, o["stock_channel_id"])
                stock_name = company["name"] if company else str(o["stock_channel_id"])
                side = "BUY" if o["side"] == "buy" else "SELL"
                order_lines.append(f"#{o['id']} {side} {o['remaining']}x {stock_name} @ {o['price']}{MAIN_CURRENCY_EMOJI}")
            embed.add_field(name="Open Orders", value="\n".join(order_lines), inline=False)

        embed.set_footer(text=f"Total value: {total_value}{MAIN_CURRENCY_EMOJI} | Total P/L: {total_pl_str}{MAIN_CURRENCY_EMOJI} | Total dividends: {total_divs}{MAIN_CURRENCY_EMOJI}")
        await ctx.send(embed=embed)

    @commands.command(aliases=['divhist', 'dh'])
    async def dividendhistory(self, ctx, member: discord.Member = None):
        """Show all dividend payouts you (or another member) received.
        Usage: .dividendhistory [@member]"""
        member = member or ctx.author
        rows = await self.pool.fetch(
            """SELECT amount, description, created_at FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'dividend'
               ORDER BY created_at DESC""",
            ctx.guild.id, member.id,
        )
        if not rows:
            await ctx.send(f"{member.display_name} has not received any dividends.")
            return

        total = sum(r["amount"] for r in rows)
        embed = discord.Embed(
            title=f"{member.display_name}'s Dividend History",
            color=discord.Color.blue(),
        )
        # Show most recent 15 entries
        lines = []
        for row in rows[:15]:
            date = row["created_at"].strftime("%Y-%m-%d")
            desc = row["description"] or "Dividend"
            lines.append(f"`{date}` {desc} — **{row['amount']:,}**{MAIN_CURRENCY_EMOJI}")
        embed.description = "\n".join(lines)
        if len(rows) > 15:
            embed.description += f"\n*... and {len(rows) - 15} more*"
        embed.set_footer(text=f"Total dividends received: {total:,}{MAIN_CURRENCY_EMOJI}")
        await ctx.send(embed=embed)

    @commands.command(aliases=['ci', 'cinfo'])
    async def companyinfo(self, ctx, stock: discord.TextChannel):
        """Show detailed info about a company. Use with mentioning a stock channel."""
        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        last_price = await get_last_trade_price(self.pool, ctx.guild.id, stock.id)
        buy_orders = await get_open_orders(self.pool, ctx.guild.id, stock.id, "buy")
        sell_orders = await get_open_orders(self.pool, ctx.guild.id, stock.id, "sell")

        best_bid = f"{buy_orders[0]['price']}{MAIN_CURRENCY_EMOJI}" if buy_orders else "None"
        best_ask = f"{sell_orders[0]['price']}{MAIN_CURRENCY_EMOJI}" if sell_orders else "None"

        embed = discord.Embed(title=f"{company['name']} - Company Info", color=discord.Color.blue())
        embed.add_field(name="Channel", value=stock.mention, inline=True)
        embed.add_field(name="Total Shares", value=str(company["total_shares"]), inline=True)
        embed.add_field(name="IPO Price", value=f"{company['ipo_price']}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="IPO Shares Left", value=str(company["available_ipo_shares"]), inline=True)
        embed.add_field(name="Last Trade", value=f"{last_price}{MAIN_CURRENCY_EMOJI}" if last_price else "No trades yet", inline=True)
        embed.add_field(name="Best Bid / Ask", value=f"{best_bid} / {best_ask}", inline=True)
        embed.add_field(name="Treasury", value=f"{company['treasury']}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Level", value=str(company["company_level"]), inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=['ob'])
    async def orderbook(self, ctx, stock: discord.TextChannel):
        """Shows you the current order book for a stock. Use with mentioning a stock channel."""
        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        buy_orders = await get_open_orders(self.pool, ctx.guild.id, stock.id, "buy")
        sell_orders = await get_open_orders(self.pool, ctx.guild.id, stock.id, "sell")

        embed = discord.Embed(title=f"{company['name']} - Order Book", color=discord.Color.blue())

        sell_lines = []
        if company["available_ipo_shares"] > 0:
            sell_lines.append(f"{company['available_ipo_shares']}x @ {company['ipo_price']}{MAIN_CURRENCY_EMOJI} — IPO")
        if sell_orders:
            for o in reversed(sell_orders[:10]):
                member = ctx.guild.get_member(o["user_id"])
                name = member.display_name if member else str(o["user_id"])
                sell_lines.append(f"{o['remaining']}x @ {o['price']}{MAIN_CURRENCY_EMOJI} — {name}")
        embed.add_field(name="Sell Orders (Asks)", value="\n".join(sell_lines) if sell_lines else "None", inline=False)

        if buy_orders:
            buy_lines = []
            for o in buy_orders[:10]:
                member = ctx.guild.get_member(o["user_id"])
                name = member.display_name if member else str(o["user_id"])
                buy_lines.append(f"{o['remaining']}x @ {o['price']}{MAIN_CURRENCY_EMOJI} — {name}")
            embed.add_field(name="Buy Orders (Bids)", value="\n".join(buy_lines), inline=False)
        else:
            embed.add_field(name="Buy Orders (Bids)", value="None", inline=False)

        await ctx.send(embed=embed)

    # ── Admin ──

    @commands.command()
    @commands.is_owner()
    async def listcompany(self, ctx, stock: discord.TextChannel, name: str, ipo_price: str = "100", total_shares: int = 1000):
        """Admin: list a new company on the market, associating it with a text channel. Optionally use IPO, total shares to adjust the default 100, 1000."""
        try:
            ipo_price = parse_amount(ipo_price)
        except AmountError as e:
            await ctx.send(str(e))
            return

        existing = await get_company(self.pool, ctx.guild.id, stock.id)
        if existing:
            await ctx.send(f"{stock.mention} is already listed as **{existing['name']}**.")
            return

        await create_company(self.pool, ctx.guild.id, stock.id, name, ctx.author.id, total_shares, ipo_price)
        # Invalidate channel cache for this guild
        self._company_channels.pop(ctx.guild.id, None)
        await ctx.send(f"**{name}** has been listed! {total_shares} shares available at {ipo_price}{MAIN_CURRENCY_EMOJI} each via IPO.")

    @commands.command(aliases=['delist'])
    @commands.is_owner()
    async def delistcompany(self, ctx, stock: discord.TextChannel):
        """Admin: delist a company, deleting all shares, orders, and trade history."""

        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        deleted = await delete_company(self.pool, ctx.guild.id, stock.id)
        self._company_channels.pop(ctx.guild.id, None)
        await ctx.send(f"**{deleted['name']}** has been delisted. All shares, orders, and history have been removed.")

    @commands.command()
    @commands.is_owner()
    async def calcrevenue(self, ctx):
        """Admin: Compute today's revenue for all companies."""
        await self.flush_char_buffer()
        today = datetime.datetime.now(datetime.timezone.utc).date()
        companies = await list_companies(self.pool, ctx.guild.id)
        results = []
        for company in companies:
            rev = await compute_daily_revenue(
                self.pool, ctx.guild.id, company["stock_channel_id"],
                today, company["revenue_multiplier"],
            )
            results.append(f"**{company['name']}**: {rev}{MAIN_CURRENCY_EMOJI}")
        await ctx.send("Revenue calculated for today:\n" + "\n".join(results) if results else "No companies listed.")

    @commands.command()
    @commands.is_owner()
    async def forcerecap(self, ctx):
        """Admin: force a revenue recap for the current week."""
        now = datetime.datetime.now(datetime.timezone.utc)
        monday = (now - datetime.timedelta(days=now.weekday())).date()
        today = now.date()
        companies = await list_companies(self.pool, ctx.guild.id)
        for company in companies:
            daily_records = await get_weekly_revenue(
                self.pool, ctx.guild.id, company["stock_channel_id"], monday, today,
            )
            total_so_far = sum(r["revenue"] for r in daily_records)
            lines = [f"  {r['revenue_date'].strftime('%A')}: {r['revenue']}{MAIN_CURRENCY_EMOJI}" for r in daily_records]
            embed = discord.Embed(title=f"{company['name']} - Revenue Recap", color=discord.Color.gold())
            embed.add_field(name="Daily Breakdown", value="\n".join(lines) or "No revenue yet", inline=False)
            embed.add_field(name="Total So Far", value=f"{total_so_far}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Treasury", value=f"{company['treasury']}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Level", value=str(company["company_level"]), inline=True)
            await ctx.send(embed=embed)

    @commands.command()
    @commands.is_owner()
    async def forcefinancials(self, ctx):
        """Admin: trigger the weekly financial processing (treasury update, dividends, level-up)."""
        if not ctx.author.guild_permissions.administrator:
            await ctx.send("You do not have permission to use this command.")
            return
        now = datetime.datetime.now(datetime.timezone.utc)
        monday = (now - datetime.timedelta(days=now.weekday())).date()
        yesterday = (now - datetime.timedelta(days=1)).date()

        companies = await list_companies(self.pool, ctx.guild.id)
        for comp in companies:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    company = await lock_company(conn, ctx.guild.id, comp["stock_channel_id"])
                    weekly_revenue = await get_weekly_revenue_total(
                        conn, ctx.guild.id, company["stock_channel_id"], monday, yesterday,
                    )
                    # --- OLD MODEL: dividends = 20% of profit, paid only when profit > 0 ---
                    # cost = int(COST_FACTOR * company["treasury"])
                    # profit = weekly_revenue - cost
                    # dividends_paid = 0
                    # dividend_per_share = 0
                    #
                    # if profit > 0:
                    #     dividend_pool = int(0.20 * profit)
                    #     dividend_per_share = dividend_pool // company["total_shares"]
                    #     if dividend_per_share > 0:
                    #         shareholders = await get_shareholders(conn, ctx.guild.id, company["stock_channel_id"])
                    #         for sh in shareholders:
                    #             payout = dividend_per_share * sh["quantity"]
                    #             await ensure_wallet(conn, ctx.guild.id, sh["user_id"])
                    #             await update_wallet(conn, ctx.guild.id, sh["user_id"], payout)
                    #             await add_transaction(conn, ctx.guild.id, sh["user_id"], payout, "dividend",
                    #                                   f"Dividend from {company['name']}")
                    #             dividends_paid += payout
                    #     await update_treasury(conn, ctx.guild.id, company["stock_channel_id"], profit - dividends_paid)
                    # else:
                    #     await update_treasury(conn, ctx.guild.id, company["stock_channel_id"], profit)

                    # --- NEW MODEL: dividends = 10% of revenue, always paid ---
                    cost = int(COST_FACTOR * company["treasury"])
                    dividend_pool = int(DIVIDEND_REVENUE_SHARE * weekly_revenue)
                    dividend_per_share = dividend_pool // company["total_shares"]
                    dividends_paid = 0
                    profit = weekly_revenue - cost  # kept for reporting

                    if dividend_per_share > 0:
                        shareholders = await get_shareholders(conn, ctx.guild.id, company["stock_channel_id"])
                        for sh in shareholders:
                            payout = dividend_per_share * sh["quantity"]
                            await ensure_wallet(conn, ctx.guild.id, sh["user_id"])
                            await update_wallet(conn, ctx.guild.id, sh["user_id"], payout)
                            await add_transaction(conn, ctx.guild.id, sh["user_id"], payout, "dividend",
                                                  f"Dividend from {company['name']}")
                            dividends_paid += payout

                    await update_treasury(conn, ctx.guild.id, company["stock_channel_id"],
                                          weekly_revenue - dividends_paid - cost)

                    company = await lock_company(conn, ctx.guild.id, company["stock_channel_id"])
                    leveled_up = False
                    next_level = company["company_level"] + 1
                    threshold = LEVEL_BASE_THRESHOLD * (2 ** (next_level - 1))
                    if company["treasury"] >= threshold:
                        consume = int(LEVEL_UP_TREASURY_CONSUME * company["treasury"])
                        new_multiplier = company["revenue_multiplier"] * 2
                        await set_company_level(conn, ctx.guild.id, company["stock_channel_id"],
                                                 next_level, new_multiplier, consume)
                        leveled_up = True

            updated = await get_company(self.pool, ctx.guild.id, comp["stock_channel_id"])
            embed = discord.Embed(title=f"{company['name']} - Financial Summary", color=discord.Color.blue())
            embed.add_field(name="Weekly Revenue", value=f"{weekly_revenue}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Operating Cost (5%)", value=f"{cost}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Profit", value=f"{profit}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Dividend/Share", value=f"{dividend_per_share}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Total Dividends Paid", value=f"{dividends_paid}{MAIN_CURRENCY_EMOJI}", inline=True)
            embed.add_field(name="Treasury", value=f"{updated['treasury']}{MAIN_CURRENCY_EMOJI}", inline=True)
            if leveled_up:
                embed.add_field(name="LEVEL UP!", value=f"Level {next_level} reached!", inline=False)
            await ctx.send(embed=embed)

    # ── Trading ──

    @commands.command(aliases=['mb', 'mbuy'])
    @require_channel("trading_channel")
    async def marketbuy(self, ctx, stock: discord.TextChannel, quantity: int = 1):
        """Buy shares immediately at the best available price. Mention the stock channel and specify the quantity."""
        if quantity <= 0:
            await ctx.send("Quantity must be positive.")
            return

        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                bought = 0
                total_cost = 0

                # Phase 1: Buy from IPO
                company = await lock_company(conn, ctx.guild.id, stock.id)
                if company["available_ipo_shares"] > 0:
                    wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                    ipo_qty = min(quantity, company["available_ipo_shares"])
                    affordable = min(ipo_qty, wallet["wallet"] // company["ipo_price"])
                    if affordable > 0:
                        cost = affordable * company["ipo_price"]
                        await update_wallet(conn, ctx.guild.id, ctx.author.id, -cost)
                        await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, affordable)
                        await conn.execute(
                            "UPDATE companies SET available_ipo_shares = available_ipo_shares - $3 WHERE guild_id = $1 AND stock_channel_id = $2",
                            ctx.guild.id, stock.id, affordable,
                        )
                        await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, None, affordable, company["ipo_price"], "ipo")
                        await update_treasury(conn, ctx.guild.id, stock.id, cost)
                        bought += affordable
                        total_cost += cost

                # Phase 2: Buy from sell orders (lowest price first)
                if bought < quantity:
                    sell_orders = await get_open_orders_locked(conn, ctx.guild.id, stock.id, "sell")
                    wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                    remaining_funds = wallet["wallet"]

                    for order in sell_orders:
                        if bought >= quantity:
                            break
                        if order["user_id"] == ctx.author.id:
                            continue

                        fill_qty = min(quantity - bought, order["remaining"])
                        cost = fill_qty * order["price"]

                        if cost > remaining_funds:
                            fill_qty = remaining_funds // order["price"]
                            if fill_qty <= 0:
                                break
                            cost = fill_qty * order["price"]

                        await update_wallet(conn, ctx.guild.id, ctx.author.id, -cost)
                        await update_wallet(conn, ctx.guild.id, order["user_id"], cost)
                        await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, -fill_qty)
                        await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, fill_qty)
                        await conn.execute(
                            "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                            order["id"], fill_qty,
                        )
                        await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, order["user_id"], fill_qty, order["price"], "market")

                        bought += fill_qty
                        total_cost += cost
                        remaining_funds -= cost

                if bought > 0:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, -total_cost, "market_buy", f"Bought {bought}x {company['name']}")

        if bought == 0:
            await ctx.send(f"Could not buy any shares of **{company['name']}**. No shares available or insufficient funds.")
            return

        avg_price = total_cost // bought
        embed = discord.Embed(title="Market Buy", color=discord.Color.green())
        embed.add_field(name="Stock", value=company["name"], inline=True)
        embed.add_field(name="Bought", value=f"{bought}/{quantity}", inline=True)
        embed.add_field(name="Avg Price", value=f"{avg_price}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Total Cost", value=f"{total_cost}{MAIN_CURRENCY_EMOJI}", inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=['ms', 'msell'])
    @require_channel("trading_channel")
    async def marketsell(self, ctx, stock: discord.TextChannel, quantity: int = 1):
        """Sell shares immediately at the best available price. Mention the stock channel and specify the quantity."""
        if quantity <= 0:
            await ctx.send("Quantity must be positive.")
            return

        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                holding = await get_holding(conn, ctx.guild.id, ctx.author.id, stock.id)
                escrowed = await get_escrowed_shares(conn, ctx.guild.id, ctx.author.id, stock.id)
                available = holding - escrowed
                if available < quantity:
                    await ctx.send(f"You only have {available} available shares of **{company['name']}** ({holding} held, {escrowed} in open sell orders).")
                    return

                sold = 0
                total_revenue = 0

                buy_orders = await get_open_orders_locked(conn, ctx.guild.id, stock.id, "buy")
                for order in buy_orders:
                    if sold >= quantity:
                        break
                    if order["user_id"] == ctx.author.id:
                        continue

                    fill_qty = min(quantity - sold, order["remaining"])
                    revenue = fill_qty * order["price"]

                    await update_wallet(conn, ctx.guild.id, ctx.author.id, revenue)
                    await update_wallet(conn, ctx.guild.id, order["user_id"], 0)  # buyer already paid when placing order
                    await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, -fill_qty)
                    await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, order["user_id"], ctx.author.id, fill_qty, order["price"], "market")

                    sold += fill_qty
                    total_revenue += revenue

                if sold > 0:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, total_revenue, "market_sell", f"Sold {sold}x {company['name']}")

        if sold == 0:
            await ctx.send(f"No buy orders available for **{company['name']}**. Place a sell order instead with `sellorder`.")
            return

        avg_price = total_revenue // sold
        embed = discord.Embed(title="Market Sell", color=discord.Color.red())
        embed.add_field(name="Stock", value=company["name"], inline=True)
        embed.add_field(name="Sold", value=f"{sold}/{quantity}", inline=True)
        embed.add_field(name="Avg Price", value=f"{avg_price}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Total Revenue", value=f"{total_revenue}{MAIN_CURRENCY_EMOJI}", inline=True)
        await ctx.send(embed=embed)

    # ── Limit orders ──

    @commands.command(aliases=['bo', 'border'])
    @require_channel("trading_channel")
    async def buyorder(self, ctx, stock: discord.TextChannel, quantity: int, price: str):
        """Place a limit buy order. The order will execute as soon as a matching sell order is placed at or below your specified price. Use by mentioning channel, then quantity and highest price you are paying."""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        if quantity <= 0:
            await ctx.send("Quantity must be positive.")
            return

        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                total_cost = quantity * price
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                if wallet["wallet"] < total_cost:
                    await ctx.send(f"You need {total_cost}{MAIN_CURRENCY_EMOJI} to place this order but only have {wallet['wallet']}{MAIN_CURRENCY_EMOJI}.")
                    return

                # Escrow the funds
                await update_wallet(conn, ctx.guild.id, ctx.author.id, -total_cost)

                # Try to match against existing sell orders first
                filled = 0
                spent = 0
                sell_orders = await get_open_orders_locked(conn, ctx.guild.id, stock.id, "sell")
                for order in sell_orders:
                    if filled >= quantity:
                        break
                    if order["user_id"] == ctx.author.id:
                        continue
                    if order["price"] > price:
                        break

                    fill_qty = min(quantity - filled, order["remaining"])
                    fill_cost = fill_qty * order["price"]

                    await update_wallet(conn, ctx.guild.id, order["user_id"], fill_cost)
                    refund = fill_qty * (price - order["price"])
                    if refund > 0:
                        await update_wallet(conn, ctx.guild.id, ctx.author.id, refund)

                    await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, -fill_qty)
                    await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, order["user_id"], fill_qty, order["price"], "limit")

                    filled += fill_qty
                    spent += fill_cost

                remaining = quantity - filled
                if remaining > 0:
                    row = await create_order(conn, ctx.guild.id, stock.id, ctx.author.id, "buy", remaining, price)
                else:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, -spent, "market_buy", f"Bought {filled}x {company['name']} via limit")

        if remaining > 0:
            await ctx.send(f"Buy order placed: {remaining}x **{company['name']}** @ {price}{MAIN_CURRENCY_EMOJI} (Order #{row['id']})" +
                           (f"\n{filled} shares filled immediately." if filled > 0 else ""))
        else:
            await ctx.send(f"Buy order fully filled! Bought {filled}x **{company['name']}** for {spent}{MAIN_CURRENCY_EMOJI}.")

    @commands.command(aliases=['so', 'sorder'])
    @require_channel("trading_channel")
    async def sellorder(self, ctx, stock: discord.TextChannel, quantity: int, price: str):
        """Place a limit sell order. The order will execute as soon as a matching buy order is placed at or above your specified price. Use by mentioning channel, then quantity and lowest price you are accepting."""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        if quantity <= 0:
            await ctx.send("Quantity must be positive.")
            return

        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                holding = await get_holding(conn, ctx.guild.id, ctx.author.id, stock.id)
                escrowed = await get_escrowed_shares(conn, ctx.guild.id, ctx.author.id, stock.id)
                available = holding - escrowed
                if available < quantity:
                    await ctx.send(f"You only have {available} available shares of **{company['name']}** ({holding} held, {escrowed} in open sell orders).")
                    return

                # Try to match against existing buy orders first
                filled = 0
                revenue = 0
                buy_orders = await get_open_orders_locked(conn, ctx.guild.id, stock.id, "buy")
                for order in buy_orders:
                    if filled >= quantity:
                        break
                    if order["user_id"] == ctx.author.id:
                        continue
                    if order["price"] < price:
                        break

                    fill_qty = min(quantity - filled, order["remaining"])
                    fill_revenue = fill_qty * order["price"]

                    await update_wallet(conn, ctx.guild.id, ctx.author.id, fill_revenue)
                    await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, -fill_qty)
                    await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, order["user_id"], ctx.author.id, fill_qty, order["price"], "limit")

                    filled += fill_qty
                    revenue += fill_revenue

                remaining = quantity - filled
                if remaining > 0:
                    row = await create_order(conn, ctx.guild.id, stock.id, ctx.author.id, "sell", remaining, price)
                if filled > 0:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, revenue, "market_sell", f"Sold {filled}x {company['name']} via limit")

        if remaining > 0:
            await ctx.send(f"Sell order placed: {remaining}x **{company['name']}** @ {price}{MAIN_CURRENCY_EMOJI} (Order #{row['id']})" +
                           (f"\n{filled} shares filled immediately." if filled > 0 else ""))
        else:
            await ctx.send(f"Sell order fully filled! Sold {filled}x **{company['name']}** for {revenue}{MAIN_CURRENCY_EMOJI}.")

    @commands.command(aliases=['co', 'corder'])
    @require_channel("trading_channel")
    async def cancelorder(self, ctx, order_id: int):
        """Cancel an open order by its ID. Use the `orderbook` command to see order IDs."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                order = await cancel_order(conn, ctx.guild.id, order_id, ctx.author.id)
                if not order:
                    await ctx.send("Order not found or already filled.")
                    return

                if order["side"] == "buy":
                    refund = order["remaining"] * order["price"]
                    await update_wallet(conn, ctx.guild.id, ctx.author.id, refund)

        if order["side"] == "buy":
            await ctx.send(f"Buy order #{order_id} cancelled. Refunded {refund}{MAIN_CURRENCY_EMOJI}.")
        else:
            await ctx.send(f"Sell order #{order_id} cancelled. {order['remaining']} shares are available again.")

    # ── Error handling ──

    @commands.command()
    @commands.is_owner()
    async def settradingchannel(self, ctx, channel: discord.TextChannel = None):
        """Admin: Set (or clear) the channel where trading commands are allowed."""
        if channel is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'trading_channel'",
                ctx.guild.id,
            )
            invalidate(ctx.guild.id, "trading_channel")
            await ctx.send("Trading channel restriction removed — commands allowed everywhere.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'trading_channel', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(channel.id),
            )
            invalidate(ctx.guild.id, "trading_channel")
            await ctx.send(f"Trading commands restricted to {channel.mention}.")

    @commands.Cog.listener()
    async def on_command_error(self, ctx, error):
        if not ctx.command or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.ChannelNotFound):
            await ctx.send(f"Stock not found: `{error.argument}`. Please provide a valid channel.")
        elif isinstance(error, commands.CommandInvokeError):
            await ctx.send(f"An error occurred: `{error.original}`")
            raise error.original
        else:
            raise error


async def setup(bot):
    await bot.add_cog(Market(bot))
