import asyncio
import datetime
import secrets
import statistics
import time
from collections import defaultdict

import discord
from discord.ext import commands, tasks

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.market.chart import render_price_chart
from cogs.market.sim import recommend_ipo
from cogs.market.db import (
    get_company, list_companies, create_company, delete_company,
    get_portfolio, lock_holding, update_holding,
    get_open_orders, get_open_orders_locked, get_user_orders, create_order, cancel_order, get_escrowed_shares,
    add_trade, get_last_trade_price, get_price_history, PRICE_HISTORY_LIMIT,
    get_price_history_since, get_last_trade_price_before,
    lock_company,
    upsert_char_count, compute_daily_revenue,
    get_weekly_revenue, get_weekly_revenue_total,
    update_treasury, set_company_level, get_shareholders,
    get_avg_buy_price,
    remove_member_shares,
    process_dilution,
    refund_company_buy_orders,
)
from core.checks import require_channel, WrongChannel, invalidate, require_not_locked, UserLocked, user_is_locked
from core.money import parse_amount, AmountError
from core.confirm import confirm
from config import (
    MAIN_CURRENCY_EMOJI,
    LEVEL_BASE_THRESHOLD,
    DIVIDEND_PROFIT_SHARE,
    LEVEL_UP_TREASURY_CONSUME,
)


# Time windows offered by the price-chart buttons: key -> (button label, chart subtitle, days)
CHART_WINDOWS = {
    "daily": ("Daily", "Past 24 hours", 1),
    "weekly": ("Weekly", "Past 7 days", 7),
    "monthly": ("Monthly", "Past 30 days", 30),
}


class StockChartView(discord.ui.View):
    """Buttons to switch a company's price chart between daily / weekly / monthly / all-time.

    Re-renders the chart and swaps the embed image on each press; all rendering goes through
    ``cog._render_window``. Anyone may toggle the view — it shows only public market data.
    """

    def __init__(self, cog, guild_id, channel, company, embed, *, current="all", timeout=180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.channel = channel
        self.company = company
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
        file = await self.cog._render_window(self.guild_id, self.channel, self.company, key)
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
        if isinstance(error, UserLocked):
            return
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
        if await user_is_locked(self.pool, message.guild.id, message.author.id):
            return
        char_count = len(message.content)
        if char_count == 0:
            return
        today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
        key = (message.guild.id, message.channel.id, message.author.id, today)
        async with self._buffer_lock:
            self._char_buffer[key] += char_count

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        # Balances/transactions cleanup is owned by the Economy cog's own listener.
        guild_id = member.guild.id
        user_id = member.id
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_shares(conn, guild_id, user_id)
                await conn.execute(
                    "DELETE FROM channel_activity WHERE guild_id = $1 AND user_id = $2",
                    guild_id, user_id,
                )

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
        if now.weekday() != 2:
            return
        monday = (now - datetime.timedelta(days=now.weekday())).date()
        yesterday = (now - datetime.timedelta(days=1)).date()

        for guild in self.bot.guilds:
            owner_row = await self.pool.fetchrow(
                "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'market_owner_channel'",
                guild.id,
            )
            owner_channel = guild.get_channel(int(owner_row["value"])) if owner_row else None
            companies = await list_companies(self.pool, guild.id)

            if owner_channel:
                embed = discord.Embed(
                    title="Mid-Week Market Recap",
                    description=f"{monday.strftime('%b %d')} – {yesterday.strftime('%b %d')}",
                    color=discord.Color.gold(),
                )
                for company in companies:
                    daily_records = await get_weekly_revenue(
                        self.pool, guild.id, company["stock_channel_id"], monday, yesterday,
                    )
                    days_so_far = len(daily_records)
                    total_so_far = sum(r["revenue"] for r in daily_records)
                    expected = int(total_so_far / days_so_far * 5) if days_so_far else 0
                    projected_cost = max(5000, int(0.075 * company["treasury"]))
                    projected_profit = expected - projected_cost
                    projected_dps = int(DIVIDEND_PROFIT_SHARE * max(0, projected_profit)) // company["total_shares"]
                    projected_eps = projected_profit / company["total_shares"] if company["total_shares"] else 0
                    day_line = " | ".join(
                        f"{r['revenue_date'].strftime('%a')}: {r['revenue']:,}{MAIN_CURRENCY_EMOJI}"
                        for r in daily_records
                    )
                    field_value = "\n".join([
                        day_line or "No revenue yet",
                        f"So far: **{total_so_far:,}{MAIN_CURRENCY_EMOJI}** → Expected: **~{expected:,}{MAIN_CURRENCY_EMOJI}**",
                        f"DPS ~{projected_dps:,}{MAIN_CURRENCY_EMOJI} | EPS ~{projected_eps:.1f}{MAIN_CURRENCY_EMOJI} | Treasury: {company['treasury']:,}{MAIN_CURRENCY_EMOJI} | Lv{company['company_level']}",
                    ])
                    embed.add_field(name=company["name"], value=field_value, inline=False)
                if embed.fields:
                    await owner_channel.send(embed=embed)
            else:
                for company in companies:
                    channel = guild.get_channel(company["stock_channel_id"])
                    if not channel:
                        continue
                    daily_records = await get_weekly_revenue(
                        self.pool, guild.id, company["stock_channel_id"], monday, yesterday,
                    )
                    days_so_far = len(daily_records)
                    total_so_far = sum(r["revenue"] for r in daily_records)
                    expected = int(total_so_far / days_so_far * 5) if days_so_far else 0
                    projected_cost = max(5000, int(0.075 * company["treasury"]))
                    projected_profit = expected - projected_cost
                    projected_dps = int(DIVIDEND_PROFIT_SHARE * max(0, projected_profit)) // company["total_shares"]
                    projected_eps = projected_profit / company["total_shares"] if company["total_shares"] else 0
                    lines = [
                        f"  {r['revenue_date'].strftime('%A')}: {r['revenue']:,}{MAIN_CURRENCY_EMOJI}"
                        for r in daily_records
                    ]
                    embed = discord.Embed(title=f"{company['name']} - Mid-Week Revenue Recap", color=discord.Color.gold())
                    embed.add_field(name="Daily Breakdown", value="\n".join(lines) or "No revenue yet", inline=False)
                    embed.add_field(name="Total So Far", value=f"{total_so_far:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Expected", value=f"~{expected:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="DPS (proj.)", value=f"~{projected_dps:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="EPS (proj.)", value=f"~{projected_eps:.1f}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Treasury", value=f"{company['treasury']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                    embed.add_field(name="Level", value=str(company["company_level"]), inline=True)
                    await channel.send(embed=embed)

    @wednesday_recap_task.before_loop
    async def before_wednesday(self):
        await self.bot.wait_until_ready()

    @tasks.loop(time=datetime.time(hour=0, minute=0, tzinfo=datetime.timezone.utc))
    async def sunday_financials_task(self):
        """Process weekly financials every Monday."""
        now = datetime.datetime.now(datetime.timezone.utc)
        if now.weekday() != 0:
            return
        yesterday = (now - datetime.timedelta(days=1)).date()
        monday = yesterday - datetime.timedelta(days=yesterday.weekday())
        saturday = yesterday

        for guild in self.bot.guilds:
            owner_row = await self.pool.fetchrow(
                "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'market_owner_channel'",
                guild.id,
            )
            owner_channel = guild.get_channel(int(owner_row["value"])) if owner_row else None
            companies = await list_companies(self.pool, guild.id)
            results = []

            for comp in companies:
                comp_channel = guild.get_channel(comp["stock_channel_id"])
                killed = False
                kill_reason = ""
                weekly_revenue = cost = profit = 0
                cost_rate = 0.05
                dividend_per_share = dividends_paid = 0
                leveled_up = False
                next_level = 1
                dilution = {"new_shares": 0}

                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        company = await lock_company(conn, guild.id, comp["stock_channel_id"])

                        age = datetime.datetime.now(datetime.timezone.utc) - company["listed_at"]
                        if age.days >= 7:
                            trade_count = await conn.fetchval(
                                "SELECT COUNT(*) FROM trade_history WHERE guild_id = $1 AND stock_channel_id = $2",
                                guild.id, comp["stock_channel_id"],
                            )
                            if trade_count == 0:
                                await refund_company_buy_orders(conn, guild.id, comp["stock_channel_id"])
                                await delete_company(conn, guild.id, comp["stock_channel_id"])
                                killed = True
                                kill_reason = "No shares were ever traded. The company has been dissolved."

                        if not killed:
                            weekly_revenue = await get_weekly_revenue_total(
                                conn, guild.id, company["stock_channel_id"], monday, saturday,
                            )

                            cost_rate = (0.05 + 0.05 * secrets.randbelow(1_000_001) / 1_000_000)
                            cost = max(5000, int(cost_rate * company["treasury"]))
                            profit = weekly_revenue - cost
                            dividend_pool = int(DIVIDEND_PROFIT_SHARE * profit)
                            dividend_per_share = dividend_pool // company["total_shares"]
                            dividends_paid = 0

                            if dividend_per_share > 0:
                                shareholders = await get_shareholders(conn, guild.id, company["stock_channel_id"])
                                for sh in shareholders:
                                    payout = dividend_per_share * sh["quantity"]
                                    await ensure_wallet(conn, guild.id, sh["user_id"])
                                    await update_wallet(conn, guild.id, sh["user_id"], payout)
                                    await add_transaction(conn, guild.id, sh["user_id"], payout, "dividend",
                                                          f"Dividend from {company['name']}")
                                    dividends_paid += payout

                            treasury_delta = weekly_revenue - dividends_paid - cost
                            treasury_after = company["treasury"] + treasury_delta

                            if treasury_after < 0:
                                await refund_company_buy_orders(conn, guild.id, comp["stock_channel_id"])
                                await delete_company(conn, guild.id, comp["stock_channel_id"])
                                killed = True
                                kill_reason = "Treasury depleted by operating costs. The company has gone bankrupt."
                            else:
                                await update_treasury(conn, guild.id, company["stock_channel_id"], treasury_delta)

                                leveled_up = False
                                next_level = company["company_level"] + 1
                                threshold = LEVEL_BASE_THRESHOLD * (2 ** (next_level - 1))

                                if treasury_after >= threshold:
                                    consume = int(LEVEL_UP_TREASURY_CONSUME * treasury_after)
                                    new_multiplier = company["revenue_multiplier"] * 2
                                    await set_company_level(conn, guild.id, company["stock_channel_id"],
                                                             next_level, new_multiplier, consume)
                                    leveled_up = True

                                dilution = await process_dilution(conn, guild.id, company["stock_channel_id"],
                                                                  profit, company)

                if killed:
                    self._company_channels.pop(guild.id, None)
                    results.append({
                        "killed": True,
                        "name": comp["name"],
                        "reason": kill_reason,
                        "channel": comp_channel,
                    })
                else:
                    updated = await get_company(self.pool, guild.id, comp["stock_channel_id"])
                    eps = profit / company["total_shares"] if company["total_shares"] else 0
                    results.append({
                        "killed": False,
                        "name": company["name"],
                        "channel": comp_channel,
                        "weekly_revenue": weekly_revenue,
                        "cost": cost,
                        "cost_rate": cost_rate,
                        "profit": profit,
                        "eps": eps,
                        "dividend_per_share": dividend_per_share,
                        "dividends_paid": dividends_paid,
                        "treasury_before": company["treasury"],
                        "treasury_after": updated["treasury"],
                        "leveled_up": leveled_up,
                        "next_level": next_level,
                        "dilution": dilution,
                        "company_level": updated["company_level"],
                    })

            if owner_channel:
                embed = discord.Embed(
                    title="Weekly Market Financials",
                    description=f"{monday.strftime('%b %d')} – {saturday.strftime('%b %d')}",
                    color=discord.Color.blue(),
                )
                for r in results:
                    if r["killed"]:
                        embed.add_field(name=f"💀 {r['name']}", value=r["reason"], inline=False)
                    else:
                        lines = [
                            f"Rev: {r['weekly_revenue']:,}{MAIN_CURRENCY_EMOJI} | Cost ({r['cost_rate']*100:.1f}%): {r['cost']:,}{MAIN_CURRENCY_EMOJI} | Profit: {r['profit']:,}{MAIN_CURRENCY_EMOJI}",
                            f"DPS: {r['dividend_per_share']:,}{MAIN_CURRENCY_EMOJI} | EPS: {r['eps']:.1f}{MAIN_CURRENCY_EMOJI} | Divs paid: {r['dividends_paid']:,}{MAIN_CURRENCY_EMOJI}",
                            f"Treasury: {r['treasury_before']:,}{MAIN_CURRENCY_EMOJI} → {r['treasury_after']:,}{MAIN_CURRENCY_EMOJI} | Lv{r['company_level']}",
                        ]
                        if r["dilution"]["new_shares"] > 0:
                            lines.append(
                                f"+{r['dilution']['new_shares']:,} shares @ {r['dilution']['dilution_price']:,}{MAIN_CURRENCY_EMOJI} "
                                f"({r['dilution']['filled_via_orders']:,} filled, {r['dilution']['ipo_pool_added']:,} to IPO)"
                            )
                        if r["leveled_up"]:
                            lines.append(f"⬆️ Level Up: Lv{r['next_level']}!")
                        embed.add_field(name=r["name"], value="\n".join(lines), inline=False)
                if embed.fields:
                    await owner_channel.send(embed=embed)
            else:
                for r in results:
                    channel = r["channel"]
                    if not channel:
                        continue
                    if r["killed"]:
                        embed = discord.Embed(
                            title=f"{r['name']} - BANKRUPT",
                            description=r["reason"],
                            color=discord.Color.dark_red(),
                        )
                    else:
                        embed = discord.Embed(title=f"{r['name']} - Weekly Financial Summary", color=discord.Color.blue())
                        embed.add_field(name="Weekly Revenue", value=f"{r['weekly_revenue']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name=f"Operating Cost ({r['cost_rate'] * 100:.1f}%)", value=f"{r['cost']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name="Profit", value=f"{r['profit']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name="DPS", value=f"{r['dividend_per_share']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name="EPS", value=f"{r['eps']:.1f}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name="Total Dividends Paid", value=f"{r['dividends_paid']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name="Treasury", value=f"{r['treasury_before']:,} → {r['treasury_after']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
                        embed.add_field(name="Level", value=str(r["company_level"]), inline=True)
                        if r["dilution"]["new_shares"] > 0:
                            embed.add_field(
                                name="Dilution",
                                value=(
                                    f"+{r['dilution']['new_shares']:,} shares @ {r['dilution']['dilution_price']:,}{MAIN_CURRENCY_EMOJI} "
                                    f"({r['dilution']['filled_via_orders']:,} filled, {r['dilution']['ipo_pool_added']:,} to IPO pool)"
                                ),
                                inline=False,
                            )
                        if r["leveled_up"]:
                            embed.add_field(name="LEVEL UP!", value=f"Level {r['next_level']} reached!", inline=False)
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
            best_bid = f"{buy_orders[0]['price']:,}{MAIN_CURRENCY_EMOJI}" if buy_orders else "None"
            best_ask = f"{sell_orders[0]['price']:,}{MAIN_CURRENCY_EMOJI}" if sell_orders else "None"
            ipo_status = f" | IPO: {c['available_ipo_shares']:,}/{c['total_shares']:,} @ {c['ipo_price']:,}{MAIN_CURRENCY_EMOJI}" if c["available_ipo_shares"] > 0 else ""
            embed.add_field(name=f"{c['name']} ({name})", value=f"Bid: {best_bid} / Ask: {best_ask}{ipo_status}", inline=False)

        await ctx.send(embed=embed)

    @commands.command(aliases=['p', 'port'])
    @require_not_locked()
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
            pl_str = f"+{pl:,}" if pl >= 0 else f"{pl:,}"
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
                value=f"{h['quantity']:,} shares @ {price:,}{MAIN_CURRENCY_EMOJI} = {value:,}{MAIN_CURRENCY_EMOJI}\n"
                      f"Avg cost: {avg_cost:,}{MAIN_CURRENCY_EMOJI} | P/L: {pl_str}{MAIN_CURRENCY_EMOJI}\n"
                      f"Dividends received: {divs:,}{MAIN_CURRENCY_EMOJI}",
                inline=False,
            )
        total_pl = total_value - total_cost
        total_pl_str = f"+{total_pl:,}" if total_pl >= 0 else f"{total_pl:,}"

        if orders:
            order_lines = []
            for o in orders:
                company = await get_company(self.pool, ctx.guild.id, o["stock_channel_id"])
                stock_name = company["name"] if company else str(o["stock_channel_id"])
                side = "BUY" if o["side"] == "buy" else "SELL"
                order_lines.append(f"#{o['id']} {side} {o['remaining']:,}x {stock_name} @ {o['price']:,}{MAIN_CURRENCY_EMOJI}")
            embed.add_field(name="Open Orders", value="\n".join(order_lines), inline=False)

        embed.set_footer(text=f"Total value: {total_value:,}{MAIN_CURRENCY_EMOJI} | Total P/L: {total_pl_str}{MAIN_CURRENCY_EMOJI} | Total dividends: {total_divs:,}{MAIN_CURRENCY_EMOJI}")
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

    async def _render_window(self, guild_id, channel, company, key):
        """Render the price chart for a window key ('daily'/'weekly'/'monthly'/'all').

        Returns a discord.File named ``price_<key>.png``, or None when there is nothing to draw.
        Windowed views anchor the line at the last price before the window (falling back to the
        IPO price) so the chart spans the whole period even with few or no recent trades.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        if key == "all":
            history = await get_price_history(self.pool, guild_id, channel.id)
            if not history:
                return None
            points = [(r["traded_at"], r["price"]) for r in history]
            if len(history) < PRICE_HISTORY_LIMIT:
                points.insert(0, (company["listed_at"], company["base_ipo_price"]))
            period_label = "All time"
        else:
            _label, period_label, days = CHART_WINDOWS[key]
            cutoff = max(now - datetime.timedelta(days=days), company["listed_at"])
            rows = await get_price_history_since(self.pool, guild_id, channel.id, cutoff)
            anchor = await get_last_trade_price_before(self.pool, guild_id, channel.id, cutoff)
            if anchor is None:
                anchor = company["base_ipo_price"]
            points = [(cutoff, anchor)] + [(r["traded_at"], r["price"]) for r in rows]
            if len(points) == 1:  # no trades in the window — draw a flat line across it
                points.append((now, anchor))

        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(None, render_price_chart, company["name"], points, period_label)
        return discord.File(buf, filename=f"price_{key}.png")

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
        shareholders = await get_shareholders(self.pool, ctx.guild.id, stock.id)

        best_bid = f"{buy_orders[0]['price']:,}{MAIN_CURRENCY_EMOJI}" if buy_orders else "None"
        best_ask = f"{sell_orders[0]['price']:,}{MAIN_CURRENCY_EMOJI}" if sell_orders else "None"

        total_shares = company["total_shares"]
        top_holders = sorted(shareholders, key=lambda r: r["quantity"], reverse=True)[:5]
        if top_holders:
            owners_lines = []
            for row in top_holders:
                member = ctx.guild.get_member(row["user_id"])
                name = member.display_name if member else f"<@{row['user_id']}>"
                pct = row["quantity"] / total_shares * 100
                owners_lines.append(f"{name} — {row['quantity']:,} ({pct:.1f}%)")
            owners_value = "\n".join(owners_lines)
        else:
            owners_value = "No shareholders yet"

        embed = discord.Embed(title=f"{company['name']} - Company Info", color=discord.Color.blue())
        embed.add_field(name="Channel", value=stock.mention, inline=True)
        embed.add_field(name="Total Shares", value=f"{total_shares:,}", inline=True)
        embed.add_field(name="IPO Price", value=f"{company['ipo_price']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="IPO Shares Left", value=f"{company['available_ipo_shares']:,}", inline=True)
        embed.add_field(name="Last Trade", value=f"{last_price:,}{MAIN_CURRENCY_EMOJI}" if last_price else "No trades yet", inline=True)
        embed.add_field(name="Best Bid / Ask", value=f"{best_bid} / {best_ask}", inline=True)
        embed.add_field(name="Treasury", value=f"{company['treasury']:,}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Level", value=str(company["company_level"]), inline=True)
        embed.add_field(name="Top Shareholders", value=owners_value, inline=False)

        # Price-history chart with daily / weekly / monthly / all-time toggle buttons.
        file = await self._render_window(ctx.guild.id, stock, company, "all")
        if file is None:  # never traded — nothing to chart yet
            await ctx.send(embed=embed)
            return

        embed.set_image(url="attachment://price_all.png")
        view = StockChartView(self, ctx.guild.id, stock, company, embed, current="all")
        view.message = await ctx.send(embed=embed, file=file, view=view)

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
            sell_lines.append(f"{company['available_ipo_shares']:,}x @ {company['ipo_price']:,}{MAIN_CURRENCY_EMOJI} — IPO")
        if sell_orders:
            for o in sell_orders[:10]:
                member = ctx.guild.get_member(o["user_id"])
                name = member.display_name if member else str(o["user_id"])
                sell_lines.append(f"{o['remaining']:,}x @ {o['price']:,}{MAIN_CURRENCY_EMOJI} — {name}")
        embed.add_field(name="Sell Orders (Asks)", value="\n".join(sell_lines) if sell_lines else "None", inline=False)

        if buy_orders:
            buy_lines = []
            for o in buy_orders[:10]:
                member = ctx.guild.get_member(o["user_id"])
                name = member.display_name if member else str(o["user_id"])
                buy_lines.append(f"{o['remaining']:,}x @ {o['price']:,}{MAIN_CURRENCY_EMOJI} — {name}")
            embed.add_field(name="Buy Orders (Bids)", value="\n".join(buy_lines), inline=False)
        else:
            embed.add_field(name="Buy Orders (Bids)", value="None", inline=False)

        await ctx.send(embed=embed)

    # ── Admin ──

    @commands.command()
    @commands.is_owner()
    async def listcompany(self, ctx, stock: discord.TextChannel, name: str, ipo_price: str = "100", total_shares: int = 10000):
        """Admin: list a new company on the market, associating it with a text channel. Optionally use IPO, total shares to adjust the default 100, 10000."""
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
        self._company_channels.pop(ctx.guild.id, None)
        await ctx.send(f"**{name}** has been listed! {total_shares:,} shares available at {ipo_price:,}{MAIN_CURRENCY_EMOJI} each via IPO.")

    @commands.command(aliases=['ipo'])
    @commands.is_owner()
    async def ipohelper(self, ctx, channel: discord.TextChannel, days: int = 14,
                        total_shares: int = 10000, wish_pct: str = None):
        """Admin: recommend an IPO price for a channel from its recent activity.

        Step 1: samples the last 100 messages for a representative chars/message mean (fast).
        Step 2: counts all messages + distinct users in the last `days` days for accurate volume.
        Combines both to project DPS and suggest IPO prices for several weekly dividend yields.
        Optionally pass a target yield % last to highlight one price,
        e.g. `.ipohelper #chan 14 10000 8` (8% weekly yield)."""
        if not 1 <= days <= 90:
            await ctx.send("`days` must be between 1 and 90.")
            return
        if total_shares <= 0:
            await ctx.send("`total_shares` must be positive.")
            return

        wish = None
        if wish_pct is not None:
            try:
                wish = float(wish_pct.strip().rstrip("%")) / 100
            except ValueError:
                await ctx.send(f"Couldn't read `{wish_pct}` as a percentage. Try e.g. `8` or `8%`.")
                return
            if wish <= 0:
                await ctx.send("The target yield must be greater than 0%.")
                return

        timed_out = False
        try:
            # Step 1: small fixed sample → chars/message mean + std + oldest timestamp (≤ 2 API calls).
            char_lengths: list[int] = []
            sample_user_ids: set[int] = set()
            oldest_ts = None
            async for message in channel.history(limit=100):
                if message.author.bot:
                    continue
                length = len(message.content)
                if length > 0:
                    char_lengths.append(length)
                    sample_user_ids.add(message.author.id)
                    oldest_ts = message.created_at  # history() is newest-first, so last assigned = oldest

            if not char_lengths:
                await ctx.send(
                    f"No measurable activity in {channel.mention} — "
                    f"can't estimate an IPO. Try a busier channel."
                )
                return

            mean_chars = statistics.fmean(char_lengths)
            std_chars = statistics.pstdev(char_lengths) if len(char_lengths) > 1 else 0.0

            # Step 2: full window → message count + distinct users (may be slow for busy channels).
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)
            msg_count = 0
            user_ids: set[int] = set()

            async def _count_window():
                nonlocal msg_count
                async for message in channel.history(after=cutoff, limit=None):
                    if message.author.bot:
                        continue
                    if len(message.content) == 0:
                        continue
                    msg_count += 1
                    user_ids.add(message.author.id)

            try:
                async with ctx.typing():
                    await asyncio.wait_for(_count_window(), timeout=30.0)
            except asyncio.TimeoutError:
                timed_out = True
                # Fall back to estimating volume from the step-1 sample.
                now = datetime.datetime.now(datetime.timezone.utc)
                sample_span = max(1.0, (now - oldest_ts).total_seconds() / 86400)
                msg_count = len(char_lengths)
                user_ids = sample_user_ids
                days = sample_span  # rescale denominator to match what we actually measured

        except discord.Forbidden:
            await ctx.send(f"I don't have permission to read history in {channel.mention}.")
            return

        if msg_count == 0:
            await ctx.send(
                f"No messages found in {channel.mention} over the last {days} day(s) — "
                f"try a longer window or a busier channel."
            )
            return

        users = max(1, len(user_ids))
        msgs_per_user_per_day = msg_count / days / users
        mean = mean_chars * msgs_per_user_per_day
        std = std_chars * msgs_per_user_per_day

        rec = recommend_ipo(users, mean, std, total_shares=total_shares)
        dps = rec["dps"]

        embed = discord.Embed(title=f"IPO recommendation — {channel.name}", color=discord.Color.gold())
        if timed_out:
            embed.description = (
                "⚠️ Full window scan timed out — fell back to the sample of the last 100 messages. "
                "Results may be less accurate; try a shorter time span for a full count."
            )
        embed.add_field(
            name="Measured activity",
            value=(
                f"{msg_count:,} messages · {users} user(s) · last {days:.1f} day(s)\n"
                f"~{msgs_per_user_per_day:.1f} msg/user/day · ~{mean_chars:.0f} chars/msg avg "
                f"→ ~{mean:,.0f} chars/user/day"
            ),
            inline=False,
        )

        if dps <= 0:
            embed.description = (
                "Projected dividends are **0** at these settings — this activity wouldn't cover "
                "weekly operating costs, so no IPO price yields income. Try a busier channel, a "
                "longer lookback, or fewer total shares."
            )
            await ctx.send(embed=embed)
            return

        embed.add_field(
            name="Projected dividend / share",
            value=f"~{dps:,}{MAIN_CURRENCY_EMOJI} per week  (total_shares = {total_shares:,})",
            inline=False,
        )

        table = ["```", f"{'Yield':>6}  {'IPO price':>11}  {'Payback':>9}"]
        for row in rec["rows"]:
            table.append(
                f"{row['yield'] * 100:>5.0f}%  {row['ipo_price']:>11,}  {row['payback']:>4} wks"
            )
        table.append("```")
        embed.add_field(
            name=f"Weekly dividend yield → IPO price ({MAIN_CURRENCY_EMOJI})",
            value="\n".join(table),
            inline=False,
        )

        if wish is not None:
            price = round(dps / wish)
            payback = round(price / dps)
            embed.add_field(
                name=f"→ Your target {wish * 100:g}%",
                value=f"List at **{price:,}{MAIN_CURRENCY_EMOJI}** / share  (~{payback} wks payback)",
                inline=False,
            )

        embed.set_footer(text="Yield is dividend income only — capital gains from trading are not included.")
        await ctx.send(embed=embed)

    @commands.command(aliases=['delist'])
    @commands.is_owner()
    async def delistcompany(self, ctx, stock: discord.TextChannel):
        """Admin: delist a company, deleting all shares, orders, and trade history."""
        company = await get_company(self.pool, ctx.guild.id, stock.id)
        if not company:
            await ctx.send("This channel is not a listed company.")
            return

        if not await confirm(
            ctx,
            f"⚠️ Delist **{company['name']}**? This deletes all its shares, orders, and trade history.",
        ):
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
            results.append(f"**{company['name']}**: {rev:,}{MAIN_CURRENCY_EMOJI}")
        await ctx.send("Revenue calculated for today:\n" + "\n".join(results) if results else "No companies listed.")

    @commands.command()
    @commands.is_owner()
    async def forcerecap(self, ctx):
        """Admin: force a revenue recap for the current week."""
        now = datetime.datetime.now(datetime.timezone.utc)
        monday = (now - datetime.timedelta(days=now.weekday())).date()
        today = now.date()
        companies = await list_companies(self.pool, ctx.guild.id)
        embed = discord.Embed(
            title="Market Revenue Recap",
            description=f"{monday.strftime('%b %d')} – {today.strftime('%b %d')}",
            color=discord.Color.gold(),
        )
        for company in companies:
            daily_records = await get_weekly_revenue(
                self.pool, ctx.guild.id, company["stock_channel_id"], monday, today,
            )
            days_so_far = len(daily_records)
            total_so_far = sum(r["revenue"] for r in daily_records)
            expected = int(total_so_far / days_so_far * 5) if days_so_far else 0
            projected_cost = max(5000, int(0.075 * company["treasury"]))
            projected_profit = expected - projected_cost
            projected_dps = int(DIVIDEND_PROFIT_SHARE * max(0, projected_profit)) // company["total_shares"]
            projected_eps = projected_profit / company["total_shares"] if company["total_shares"] else 0
            day_line = " | ".join(
                f"{r['revenue_date'].strftime('%a')}: {r['revenue']:,}{MAIN_CURRENCY_EMOJI}"
                for r in daily_records
            )
            field_value = "\n".join([
                day_line or "No revenue yet",
                f"So far: **{total_so_far:,}{MAIN_CURRENCY_EMOJI}** → Expected: **~{expected:,}{MAIN_CURRENCY_EMOJI}**",
                f"DPS ~{projected_dps:,}{MAIN_CURRENCY_EMOJI} | EPS ~{projected_eps:.1f}{MAIN_CURRENCY_EMOJI} | Treasury: {company['treasury']:,}{MAIN_CURRENCY_EMOJI} | Lv{company['company_level']}",
            ])
            embed.add_field(name=company["name"], value=field_value, inline=False)
        if embed.fields:
            await ctx.send(embed=embed)
        else:
            await ctx.send("No companies listed.")

    @commands.command()
    @commands.is_owner()
    async def forcefinancials(self, ctx):
        """Admin: trigger the weekly financial processing (treasury update, dividends, level-up)."""
        now = datetime.datetime.now(datetime.timezone.utc)
        yesterday = (now - datetime.timedelta(days=1)).date()
        monday = yesterday - datetime.timedelta(days=yesterday.weekday())

        companies = await list_companies(self.pool, ctx.guild.id)
        results = []

        for comp in companies:
            killed = False
            kill_reason = ""
            weekly_revenue = cost = profit = 0
            cost_rate = 0.05
            dividend_per_share = dividends_paid = 0
            leveled_up = False
            next_level = 1
            dilution = {"new_shares": 0}

            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    company = await lock_company(conn, ctx.guild.id, comp["stock_channel_id"])

                    age = datetime.datetime.now(datetime.timezone.utc) - company["listed_at"]
                    if age.days >= 7:
                        trade_count = await conn.fetchval(
                            "SELECT COUNT(*) FROM trade_history WHERE guild_id = $1 AND stock_channel_id = $2",
                            ctx.guild.id, comp["stock_channel_id"],
                        )
                        if trade_count == 0:
                            await refund_company_buy_orders(conn, ctx.guild.id, comp["stock_channel_id"])
                            await delete_company(conn, ctx.guild.id, comp["stock_channel_id"])
                            killed = True
                            kill_reason = "No shares were ever traded. The company has been dissolved."

                    if not killed:
                        weekly_revenue = await get_weekly_revenue_total(
                            conn, ctx.guild.id, company["stock_channel_id"], monday, yesterday,
                        )

                        cost_rate = (0.05 + 0.05 * secrets.randbelow(1_000_001) / 1_000_000)
                        cost = max(5000, int(cost_rate * company["treasury"]))
                        profit = weekly_revenue - cost
                        dividend_pool = int(DIVIDEND_PROFIT_SHARE * profit)
                        dividend_per_share = dividend_pool // company["total_shares"]
                        dividends_paid = 0

                        if dividend_per_share > 0:
                            shareholders = await get_shareholders(conn, ctx.guild.id, company["stock_channel_id"])
                            for sh in shareholders:
                                payout = dividend_per_share * sh["quantity"]
                                await ensure_wallet(conn, ctx.guild.id, sh["user_id"])
                                await update_wallet(conn, ctx.guild.id, sh["user_id"], payout)
                                await add_transaction(conn, ctx.guild.id, sh["user_id"], payout, "dividend",
                                                      f"Dividend from {company['name']}")
                                dividends_paid += payout

                        treasury_delta = weekly_revenue - dividends_paid - cost
                        treasury_after = company["treasury"] + treasury_delta

                        if treasury_after < 0:
                            await refund_company_buy_orders(conn, ctx.guild.id, comp["stock_channel_id"])
                            await delete_company(conn, ctx.guild.id, comp["stock_channel_id"])
                            killed = True
                            kill_reason = "Treasury depleted by operating costs. The company has gone bankrupt."
                        else:
                            await update_treasury(conn, ctx.guild.id, company["stock_channel_id"], treasury_delta)

                            leveled_up = False
                            next_level = company["company_level"] + 1
                            threshold = LEVEL_BASE_THRESHOLD * (2 ** (next_level - 1))
                            if treasury_after >= threshold:
                                consume = int(LEVEL_UP_TREASURY_CONSUME * treasury_after)
                                new_multiplier = company["revenue_multiplier"] * 2
                                await set_company_level(conn, ctx.guild.id, company["stock_channel_id"],
                                                         next_level, new_multiplier, consume)
                                leveled_up = True

                            dilution = await process_dilution(conn, ctx.guild.id, company["stock_channel_id"],
                                                              profit, company)

            if killed:
                self._company_channels.pop(ctx.guild.id, None)
                results.append({"killed": True, "name": comp["name"], "reason": kill_reason})
            else:
                updated = await get_company(self.pool, ctx.guild.id, comp["stock_channel_id"])
                eps = profit / company["total_shares"] if company["total_shares"] else 0
                results.append({
                    "killed": False,
                    "name": company["name"],
                    "weekly_revenue": weekly_revenue,
                    "cost": cost,
                    "cost_rate": cost_rate,
                    "profit": profit,
                    "eps": eps,
                    "dividend_per_share": dividend_per_share,
                    "dividends_paid": dividends_paid,
                    "treasury_before": company["treasury"],
                    "treasury_after": updated["treasury"],
                    "leveled_up": leveled_up,
                    "next_level": next_level,
                    "dilution": dilution,
                    "company_level": updated["company_level"],
                })

        embed = discord.Embed(
            title="Market Financial Summary",
            description=f"{monday.strftime('%b %d')} – {yesterday.strftime('%b %d')}",
            color=discord.Color.blue(),
        )
        for r in results:
            if r["killed"]:
                embed.add_field(name=f"💀 {r['name']}", value=r["reason"], inline=False)
            else:
                lines = [
                    f"Rev: {r['weekly_revenue']:,}{MAIN_CURRENCY_EMOJI} | Cost ({r['cost_rate']*100:.1f}%): {r['cost']:,}{MAIN_CURRENCY_EMOJI} | Profit: {r['profit']:,}{MAIN_CURRENCY_EMOJI}",
                    f"DPS: {r['dividend_per_share']:,}{MAIN_CURRENCY_EMOJI} | EPS: {r['eps']:.1f}{MAIN_CURRENCY_EMOJI} | Divs paid: {r['dividends_paid']:,}{MAIN_CURRENCY_EMOJI}",
                    f"Treasury: {r['treasury_before']:,}{MAIN_CURRENCY_EMOJI} → {r['treasury_after']:,}{MAIN_CURRENCY_EMOJI} | Lv{r['company_level']}",
                ]
                if r["dilution"]["new_shares"] > 0:
                    lines.append(
                        f"+{r['dilution']['new_shares']:,} shares @ {r['dilution']['dilution_price']:,}{MAIN_CURRENCY_EMOJI} "
                        f"({r['dilution']['filled_via_orders']:,} filled, {r['dilution']['ipo_pool_added']:,} to IPO)"
                    )
                if r["leveled_up"]:
                    lines.append(f"⬆️ Level Up: Lv{r['next_level']}!")
                embed.add_field(name=r["name"], value="\n".join(lines), inline=False)
        if embed.fields:
            await ctx.send(embed=embed)
        else:
            await ctx.send("No companies listed.")

    # ── Trading ──

    @commands.command(aliases=['mb', 'mbuy'])
    @require_not_locked()
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

                company = await lock_company(conn, ctx.guild.id, stock.id)
                sell_orders = await get_open_orders_locked(conn, ctx.guild.id, stock.id, "sell")
                wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                remaining_funds = wallet["wallet"]

                ipo_rem = company["available_ipo_shares"]
                ipo_price = company["ipo_price"]
                order_rems = {o["id"]: o["remaining"] for o in sell_orders}

                while bought < quantity and remaining_funds > 0:
                    need = quantity - bought

                    # Find cheapest available sell order (skip own orders)
                    best_order = None
                    for o in sell_orders:
                        if o["user_id"] == ctx.author.id:
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
                        await update_wallet(conn, ctx.guild.id, ctx.author.id, -cost)
                        await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, fill_qty)
                        await conn.execute(
                            "UPDATE companies SET available_ipo_shares = available_ipo_shares - $3 WHERE guild_id = $1 AND stock_channel_id = $2",
                            ctx.guild.id, stock.id, fill_qty,
                        )
                        await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, None, fill_qty, ipo_price, "ipo")
                        await update_treasury(conn, ctx.guild.id, stock.id, cost)
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
                            ctx.guild.id, order["user_id"], stock.id,
                        )
                        fill_qty = min(fill_qty, seller_qty or 0)
                        if fill_qty <= 0:
                            order_rems[order["id"]] = 0
                            continue
                        cost = fill_qty * order["price"]
                        await update_wallet(conn, ctx.guild.id, ctx.author.id, -cost)
                        await ensure_wallet(conn, ctx.guild.id, order["user_id"])
                        await update_wallet(conn, ctx.guild.id, order["user_id"], cost)
                        try:
                            await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, -fill_qty)
                        except ValueError:
                            order_rems[order["id"]] = 0
                        await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, fill_qty)
                        await conn.execute(
                            "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                            order["id"], fill_qty,
                        )
                        await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, order["user_id"], fill_qty, order["price"], "market")
                        await add_transaction(conn, ctx.guild.id, order["user_id"], cost, "market_sell", f"Sold {fill_qty}x {company['name']}")
                        bought += fill_qty
                        total_cost += cost
                        remaining_funds -= cost
                        order_rems[order["id"]] -= fill_qty

                if bought > 0:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, -total_cost, "market_buy", f"Bought {bought}x {company['name']}")

        if bought == 0:
            await ctx.send(f"Could not buy any shares of **{company['name']}**. No shares available or insufficient funds.")
            return

        avg_price = total_cost // bought
        embed = discord.Embed(title="Market Buy", color=discord.Color.green())
        embed.add_field(name="Stock", value=company["name"], inline=True)
        embed.add_field(name="Bought", value=f"{bought:,}/{quantity:,}", inline=True)
        embed.add_field(name="Avg Price", value=f"{avg_price:,}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Total Cost", value=f"{total_cost:,}{MAIN_CURRENCY_EMOJI}", inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=['ms', 'msell'])
    @require_not_locked()
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
                holding = await lock_holding(conn, ctx.guild.id, ctx.author.id, stock.id)
                escrowed = await get_escrowed_shares(conn, ctx.guild.id, ctx.author.id, stock.id)
                available = holding - escrowed
                if available < quantity:
                    await ctx.send(f"You only have {available:,} available shares of **{company['name']}** ({holding:,} held, {escrowed:,} in open sell orders).")
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
                    try:
                        await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, -fill_qty)
                    except ValueError:
                        continue
                    await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, order["user_id"], ctx.author.id, fill_qty, order["price"], "market")
                    await add_transaction(conn, ctx.guild.id, order["user_id"], -revenue, "market_buy", f"Bought {fill_qty}x {company['name']}")

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
        embed.add_field(name="Sold", value=f"{sold:,}/{quantity:,}", inline=True)
        embed.add_field(name="Avg Price", value=f"{avg_price:,}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Total Revenue", value=f"{total_revenue:,}{MAIN_CURRENCY_EMOJI}", inline=True)
        await ctx.send(embed=embed)

    # ── Limit orders ──

    @commands.command(aliases=['bo', 'border'])
    @require_not_locked()
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
                    await ctx.send(f"You need {total_cost:,}{MAIN_CURRENCY_EMOJI} to place this order but only have {wallet['wallet']:,}{MAIN_CURRENCY_EMOJI}.")
                    return

                await update_wallet(conn, ctx.guild.id, ctx.author.id, -total_cost)

                filled = 0
                spent = 0
                company_locked = await lock_company(conn, ctx.guild.id, stock.id)
                sell_orders = await get_open_orders_locked(conn, ctx.guild.id, stock.id, "sell")
                for order in sell_orders:
                    if filled >= quantity:
                        break
                    if order["user_id"] == ctx.author.id:
                        continue
                    if order["price"] > price:
                        break

                    fill_qty = min(quantity - filled, order["remaining"])

                    # Lock seller's portfolio and cap fill_qty at what they actually hold
                    seller_qty = await conn.fetchval(
                        "SELECT COALESCE(quantity, 0) FROM portfolios WHERE guild_id = $1 AND user_id = $2 AND stock_channel_id = $3 FOR UPDATE",
                        ctx.guild.id, order["user_id"], stock.id,
                    )
                    fill_qty = min(fill_qty, seller_qty or 0)
                    if fill_qty <= 0:
                        continue

                    fill_cost = fill_qty * order["price"]

                    await ensure_wallet(conn, ctx.guild.id, order["user_id"])
                    await update_wallet(conn, ctx.guild.id, order["user_id"], fill_cost)
                    refund = fill_qty * (price - order["price"])
                    if refund > 0:
                        await update_wallet(conn, ctx.guild.id, ctx.author.id, refund)

                    try:
                        await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, -fill_qty)
                    except ValueError:
                        continue
                    await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, order["user_id"], fill_qty, order["price"], "limit")
                    await add_transaction(conn, ctx.guild.id, order["user_id"], fill_cost, "market_sell", f"Sold {fill_qty}x {company['name']} via limit")

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
                        await update_wallet(conn, ctx.guild.id, ctx.author.id, refund)
                    await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE companies SET available_ipo_shares = available_ipo_shares - $3 WHERE guild_id = $1 AND stock_channel_id = $2",
                        ctx.guild.id, stock.id, fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, ctx.author.id, None, fill_qty, ipo_price_val, "ipo")
                    await update_treasury(conn, ctx.guild.id, stock.id, fill_cost)
                    filled += fill_qty
                    spent += fill_cost

                remaining = quantity - filled
                if remaining > 0:
                    row = await create_order(conn, ctx.guild.id, stock.id, ctx.author.id, "buy", remaining, price)
                if filled > 0:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, -spent, "market_buy", f"Bought {filled}x {company['name']} via limit")

        if remaining > 0:
            await ctx.send(f"Buy order placed: {remaining:,}x **{company['name']}** @ {price:,}{MAIN_CURRENCY_EMOJI} (Order #{row['id']})" +
                           (f"\n{filled:,} shares filled immediately." if filled > 0 else ""))
        else:
            await ctx.send(f"Buy order fully filled! Bought {filled:,}x **{company['name']}** for {spent:,}{MAIN_CURRENCY_EMOJI}.")

    @commands.command(aliases=['so', 'sorder'])
    @require_not_locked()
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
                holding = await lock_holding(conn, ctx.guild.id, ctx.author.id, stock.id)
                escrowed = await get_escrowed_shares(conn, ctx.guild.id, ctx.author.id, stock.id)
                available = holding - escrowed
                if available < quantity:
                    await ctx.send(f"You only have {available:,} available shares of **{company['name']}** ({holding:,} held, {escrowed:,} in open sell orders).")
                    return

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
                    try:
                        await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, -fill_qty)
                    except ValueError:
                        continue
                    await update_holding(conn, ctx.guild.id, order["user_id"], stock.id, fill_qty)
                    await conn.execute(
                        "UPDATE orders SET remaining = remaining - $2 WHERE id = $1",
                        order["id"], fill_qty,
                    )
                    await add_trade(conn, ctx.guild.id, stock.id, order["user_id"], ctx.author.id, fill_qty, order["price"], "limit")
                    await add_transaction(conn, ctx.guild.id, order["user_id"], -fill_revenue, "market_buy", f"Bought {fill_qty}x {company['name']} via limit")

                    filled += fill_qty
                    revenue += fill_revenue

                remaining = quantity - filled
                if remaining > 0:
                    row = await create_order(conn, ctx.guild.id, stock.id, ctx.author.id, "sell", remaining, price)
                if filled > 0:
                    await add_transaction(conn, ctx.guild.id, ctx.author.id, revenue, "market_sell", f"Sold {filled}x {company['name']} via limit")

        if remaining > 0:
            await ctx.send(f"Sell order placed: {remaining:,}x **{company['name']}** @ {price:,}{MAIN_CURRENCY_EMOJI} (Order #{row['id']})" +
                           (f"\n{filled:,} shares filled immediately." if filled > 0 else ""))
        else:
            await ctx.send(f"Sell order fully filled! Sold {filled:,}x **{company['name']}** for {revenue:,}{MAIN_CURRENCY_EMOJI}.")

    @commands.command(aliases=['gs', 'giftstock'])
    @require_not_locked()
    @require_channel("trading_channel")
    async def giftstocks(self, ctx, member: discord.Member, stock: discord.TextChannel, quantity: int = 1):
        """Gift shares to another member for free. Usage: .giftstocks @member #stock-channel [quantity]"""
        if member.bot:
            await ctx.send("You cannot gift stocks to a bot.")
            return
        if member == ctx.author:
            await ctx.send("You cannot gift stocks to yourself.")
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
                holding = await lock_holding(conn, ctx.guild.id, ctx.author.id, stock.id)
                escrowed = await get_escrowed_shares(conn, ctx.guild.id, ctx.author.id, stock.id)
                available = holding - escrowed
                if available < quantity:
                    await ctx.send(
                        f"You only have {available:,} available shares of **{company['name']}** "
                        f"({holding:,} held, {escrowed:,} in open sell orders)."
                    )
                    return

                await update_holding(conn, ctx.guild.id, ctx.author.id, stock.id, -quantity)
                await update_holding(conn, ctx.guild.id, member.id, stock.id, quantity)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, 0, "gift_send",
                                      f"Gifted {quantity}x {company['name']} to {member.display_name}")
                await add_transaction(conn, ctx.guild.id, member.id, 0, "gift_receive",
                                      f"Received {quantity}x {company['name']} from {ctx.author.display_name}")

        embed = discord.Embed(title="Stocks Gifted", color=discord.Color.purple())
        embed.add_field(name="From", value=ctx.author.display_name, inline=True)
        embed.add_field(name="To", value=member.display_name, inline=True)
        embed.add_field(name="Stock", value=company["name"], inline=True)
        embed.add_field(name="Quantity", value=str(quantity), inline=True)
        await ctx.send(embed=embed)

    @commands.command(aliases=['co', 'corder'])
    @require_not_locked()
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
            await ctx.send(f"Buy order #{order_id} cancelled. Refunded {refund:,}{MAIN_CURRENCY_EMOJI}.")
        else:
            await ctx.send(f"Sell order #{order_id} cancelled. {order['remaining']} shares are available again.")

    # ── Channel admin ──

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

    @commands.command()
    @commands.is_owner()
    async def setmarketownerchannel(self, ctx, channel: discord.TextChannel = None):
        """Admin: Set (or clear) the channel where weekly recaps and financials are posted."""
        if channel is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'market_owner_channel'",
                ctx.guild.id,
            )
            await ctx.send("Market owner channel cleared — recaps will post to each stock channel.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'market_owner_channel', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(channel.id),
            )
            await ctx.send(f"Weekly recaps and financials will be posted to {channel.mention}.")

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
