import math

import discord
from discord.ext import commands

from cogs.economy.db import (
    ensure_wallet, update_wallet, update_bank, add_transaction, remove_member_data,
    set_salary_role, remove_salary_role, list_salary_roles, get_salary_roles_for,
)
from cogs.market.db import remove_member_shares, create_company
from core.money import parse_amount, AmountError
from core.time_utils import parse_duration, humanize_duration
from core.confirm import confirm
from config import REVENUE_BASE_MULTIPLIER

import datetime
import secrets

from config import (
    WORK_COOLDOWN, CRIME_COOLDOWN, DEFAULT_CRIME_SUCCESS_RATE, DEFAULT_CRIME_PENALTY_PCT,
    CRIME_MIN_PAYOUT, CRIME_MAX_PAYOUT, CRIME_PAYOUT_EXPONENT,
)
from core.currency import Currency
from core.checks import require_channel, WrongChannel, invalidate, require_not_locked, UserLocked, invalidate_lock

PER_PAGE = 10


class TransactionPaginator(discord.ui.View):
    def __init__(self, rows: list, member: discord.Member, invoker_id: int, currency: Currency, *, counting_note: bool = False, timeout=120):
        super().__init__(timeout=timeout)
        self.rows = rows
        self.member = member
        self.invoker_id = invoker_id
        self.currency = currency
        self.counting_note = counting_note
        self.page = 0
        self.max_page = max(0, math.ceil(len(rows) / PER_PAGE) - 1)
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.page == 0
        self.next_btn.disabled = self.page >= self.max_page

    def build_embed(self) -> discord.Embed:
        start = self.page * PER_PAGE
        page_rows = self.rows[start:start + PER_PAGE]
        embed = discord.Embed(
            title=f"{self.member.display_name}'s Transactions",
            color=discord.Color.green(),
        )
        lines = []
        for row in page_rows:
            date = row["created_at"].strftime("%Y-%m-%d %H:%M")
            sign = "+" if row["amount"] >= 0 else ""
            desc = row["description"] or row["tx_type"]
            lines.append(f"`{date}` **{sign}{row['amount']:,}**{self.currency.emoji} — {desc}")
        embed.description = "\n".join(lines) if lines else "No transactions."
        footer = f"Page {self.page + 1}/{self.max_page + 1} — {len(self.rows)} total"
        if self.counting_note:
            footer += f" | counting collapsed — use `.curtrs counting` to expand"
        embed.set_footer(text=footer)
        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message("This isn't your transaction list.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="◀ Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)


class Economy(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        # UserLocked is silent; WrongChannel is reported by each command's own
        # error handler, so swallow it here to avoid a duplicate traceback.
        if isinstance(error, (UserLocked, WrongChannel)):
            return
        raise error

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Clean up economy data when a member leaves, is kicked, or is banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    @commands.command(aliases=["dep", "d"])
    @require_not_locked()
    async def deposit(self, ctx, amount: str):
        """Deposit money from you wallet into your banj account. You can specify and amount or use 'all' to deposit everything."""
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        wallet = bal["wallet"]
        try:
            amount = parse_amount(amount, wallet_balance=wallet)
        except AmountError as e:
            await ctx.send(str(e))
            return

        if wallet < amount:
            await ctx.send("You can't deposit more than you have in your wallet!")
            return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await update_bank(self.pool, ctx.guild.id, ctx.author.id, amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, amount, "deposit")

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Deposit", description=f"You deposited {amount}{cur.emoji} into your bank account!", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["with", "w"])
    @require_not_locked()
    async def withdraw(self, ctx, amount: str):
        """Withdraw money from your bank account into your wallet. You can specify and amount or use 'all' to withdraw everything."""
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        bank = bal["bank"]
        try:
            amount = parse_amount(amount, wallet_balance=bank)
        except AmountError as e:
            await ctx.send(str(e))
            return

        if bank < amount:
            await ctx.send("You can't withdraw more than you have in your bank account!")
            return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, amount)
        await update_bank(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, amount, "withdraw")

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Withdraw", description=f"You withdrew {amount}{cur.emoji} from your bank account!", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["bal", "b", "$"])
    @require_not_locked()
    async def balance(self, ctx, member: discord.Member = None):
        """Check your balance or someone else's balance. You can mention a member to check their balance."""
        member = member or ctx.author
        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        wallet = bal["wallet"]
        bank = bal["bank"]

        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title=f"{member.display_name}'s Balance", color=discord.Color.green())
        embed.add_field(name=f"Wallet ({cur.name})", value=f"{wallet}{cur.emoji}")
        embed.add_field(name=f"Bank ({cur.name})", value=f"{bank}{cur.emoji}")
        embed.add_field(name="Total", value=f"{wallet + bank}{cur.emoji}")
        embed.set_thumbnail(url=member.display_avatar.url)

        await ctx.send(embed=embed)

    @balance.error
    async def balance_error(self, ctx, error):
        if isinstance(error, commands.MemberNotFound):
            await ctx.send("Member not found. Please mention a valid member or provide a valid user ID.")

    @commands.command()
    @require_not_locked()
    @require_channel("work_channel")
    async def work(self, ctx):
        """Work to earn some money"""
        cooldown = await self.pool.fetchval(
                    "SELECT expires_at FROM cooldowns WHERE guild_id = $1 AND user_id = $2 AND command = 'work' AND expires_at > now()",
                    ctx.guild.id, ctx.author.id,
                )

        if cooldown is None:
            earnings = secrets.randbelow(201) + 100
            await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, earnings)
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, earnings, "work", "Earnings from work")
            await self.pool.execute(
                """
                INSERT INTO cooldowns (guild_id, user_id, command, expires_at)
                VALUES ($1, $2, 'work', $3)
                ON CONFLICT (guild_id, user_id, command) DO UPDATE SET expires_at = EXCLUDED.expires_at
                """,
                ctx.guild.id, ctx.author.id, datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=WORK_COOLDOWN)
            )

            cur = self.bot.get_currency(ctx.guild.id)
            embed = discord.Embed(title="Work", description=f"You earned {earnings}{cur.emoji} from your work!", color=discord.Color.blue())
            await ctx.send(embed=embed)
            return

        remaining = cooldown - datetime.datetime.now(datetime.timezone.utc)
        minutes, seconds = divmod(int(remaining.total_seconds()), 60)
        await ctx.send(f"You need to wait *{minutes}m {seconds}s* before you can work again.")

    @work.error
    async def work_error(self, ctx, error):
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), ephemeral=True)

    @commands.command()
    @commands.is_owner()
    async def setworkchannel(self, ctx, channel: discord.TextChannel = None):
        """Set (or clear) the channel where .work is allowed."""
        if channel is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'work_channel'",
                ctx.guild.id,
            )
            invalidate(ctx.guild.id, "work_channel")
            await ctx.send("Work channel restriction removed — `.work` allowed everywhere.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'work_channel', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(channel.id),
            )
            invalidate(ctx.guild.id, "work_channel")
            await ctx.send(f"`.work` restricted to {channel.mention}.")

    @commands.command()
    @require_not_locked()
    @require_channel("work_channel")
    async def collect(self, ctx):
        """Collect the salary for every salaried role you hold whose timer is ready."""
        role_ids = [r.id for r in ctx.author.roles]
        salary_roles = await get_salary_roles_for(self.pool, ctx.guild.id, role_ids)
        if not salary_roles:
            await ctx.send("None of your roles pay a salary.")
            return

        now = datetime.datetime.now(datetime.timezone.utc)
        await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)

        collected = []   # (role_name, amount)
        on_cooldown = []  # (role_name, remaining_seconds)
        total = 0
        for sr in salary_roles:
            role = ctx.guild.get_role(sr["role_id"])
            role_name = role.name if role else f"role {sr['role_id']}"
            command = f"collect:{sr['role_id']}"
            expires_at = await self.pool.fetchval(
                "SELECT expires_at FROM cooldowns WHERE guild_id = $1 AND user_id = $2 AND command = $3 AND expires_at > now()",
                ctx.guild.id, ctx.author.id, command,
            )
            if expires_at is not None:
                on_cooldown.append((role_name, (expires_at - now).total_seconds()))
                continue

            amount = sr["amount"]
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, amount)
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, amount, "salary", f"Salary for {role_name}")
            await self.pool.execute(
                """INSERT INTO cooldowns (guild_id, user_id, command, expires_at)
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (guild_id, user_id, command) DO UPDATE SET expires_at = EXCLUDED.expires_at""",
                ctx.guild.id, ctx.author.id, command, now + datetime.timedelta(seconds=sr["interval_seconds"]),
            )
            collected.append((role_name, amount))
            total += amount

        if collected:
            cur = self.bot.get_currency(ctx.guild.id)
            embed = discord.Embed(title="Salary Collected", color=discord.Color.green())
            embed.description = "\n".join(f"**{name}** — +{amt:,}{cur.emoji}" for name, amt in collected)
            embed.add_field(name="Total", value=f"{total:,}{cur.emoji}")
            if on_cooldown:
                embed.add_field(
                    name="Not ready yet",
                    value="\n".join(f"**{name}** — in {humanize_duration(rem, short=True)}" for name, rem in on_cooldown),
                    inline=False,
                )
            await ctx.send(embed=embed)
        else:
            soonest = min(on_cooldown, key=lambda x: x[1])
            await ctx.send(
                f"You've already collected all your salaries. Next up: **{soonest[0]}** in "
                f"*{humanize_duration(soonest[1], short=True)}*."
            )

    @collect.error
    async def collect_error(self, ctx, error):
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), ephemeral=True)

    @commands.command()
    async def salaries(self, ctx):
        """List every role that pays a salary via `.collect`, and how much."""
        await self._show_salary_roles(ctx)

    # ── Crime ──

    async def _get_crime_config(self, guild_id: int) -> tuple[int, int]:
        """Return (success_rate, penalty_pct) for this guild, falling back to defaults."""
        rows = await self.pool.fetch(
            "SELECT key, value FROM guild_settings WHERE guild_id = $1 AND key = ANY($2)",
            guild_id, ["crime_success_rate", "crime_penalty_pct"],
        )
        settings = {r["key"]: int(r["value"]) for r in rows}
        return (
            settings.get("crime_success_rate", DEFAULT_CRIME_SUCCESS_RATE),
            settings.get("crime_penalty_pct", DEFAULT_CRIME_PENALTY_PCT),
        )

    async def _set_crime_setting(self, guild_id: int, key: str, value: int):
        await self.pool.execute(
            """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, $2, $3)
               ON CONFLICT (guild_id, key) DO UPDATE SET value = EXCLUDED.value""",
            guild_id, key, str(value),
        )

    @commands.command()
    @require_not_locked()
    @require_channel("work_channel")
    async def crime(self, ctx):
        """Commit a crime: a chance at a big payout, but risk losing a cut of your total money."""
        now = datetime.datetime.now(datetime.timezone.utc)
        cooldown = await self.pool.fetchval(
            "SELECT expires_at FROM cooldowns WHERE guild_id = $1 AND user_id = $2 AND command = 'crime' AND expires_at > now()",
            ctx.guild.id, ctx.author.id,
        )
        if cooldown is not None:
            remaining = cooldown - now
            minutes, seconds = divmod(int(remaining.total_seconds()), 60)
            await ctx.send(f"You're laying low after your last job. Wait *{minutes}m {seconds}s* before your next crime.")
            return

        success_rate, penalty_pct = await self._get_crime_config(ctx.guild.id)
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        cur = self.bot.get_currency(ctx.guild.id)

        # Start the cooldown regardless of the outcome.
        await self.pool.execute(
            """INSERT INTO cooldowns (guild_id, user_id, command, expires_at)
               VALUES ($1, $2, 'crime', $3)
               ON CONFLICT (guild_id, user_id, command) DO UPDATE SET expires_at = EXCLUDED.expires_at""",
            ctx.guild.id, ctx.author.id, now + datetime.timedelta(seconds=CRIME_COOLDOWN),
        )

        if secrets.randbelow(100) < success_rate:
            # Skew the payout toward CRIME_MIN_PAYOUT: a uniform roll raised to an
            # exponent makes large payouts increasingly unlikely.
            roll = secrets.randbelow(1_000_000) / 1_000_000
            span = CRIME_MAX_PAYOUT - CRIME_MIN_PAYOUT
            payout = CRIME_MIN_PAYOUT + int(span * (roll ** CRIME_PAYOUT_EXPONENT))
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, payout)
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, payout, "crime", "Successful crime")
            embed = discord.Embed(
                title="Crime — Success 🤑",
                description=f"You pulled it off and got away with **{payout:,}**{cur.emoji}!",
                color=discord.Color.green(),
            )
            await ctx.send(embed=embed)
            return

        # Failure: lose penalty_pct of total money, taken from the wallet first then the bank.
        total = bal["wallet"] + bal["bank"]
        loss = total * penalty_pct // 100
        from_wallet = min(loss, bal["wallet"])
        from_bank = loss - from_wallet
        if from_wallet > 0:
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -from_wallet)
        if from_bank > 0:
            await update_bank(self.pool, ctx.guild.id, ctx.author.id, -from_bank)
        if loss > 0:
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, -loss, "crime", "Failed crime")

        embed = discord.Embed(
            title="Crime — Busted 🚔",
            description=(
                f"You got caught and lost **{loss:,}**{cur.emoji} ({penalty_pct}% of your total wallet + bank)."
                if loss > 0 else
                "You got caught — lucky for you, you had nothing worth taking."
            ),
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @crime.error
    async def crime_error(self, ctx, error):
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), ephemeral=True)

    @commands.group(invoke_without_command=True)
    @commands.is_owner()
    async def crimeconfig(self, ctx):
        """Owner: view or change the `.crime` success rate and failure penalty."""
        rate, penalty = await self._get_crime_config(ctx.guild.id)
        embed = discord.Embed(title="Crime Settings", color=discord.Color.dark_red())
        embed.add_field(name="Success rate", value=f"{rate}%")
        embed.add_field(name="Failure penalty", value=f"{penalty}% of total")
        embed.set_footer(text="Change with .crimeconfig rate <percent> or .crimeconfig penalty <percent>")
        await ctx.send(embed=embed)

    @crimeconfig.command(name="rate")
    @commands.is_owner()
    async def crimeconfig_rate(self, ctx, percent: int):
        """Owner: set the `.crime` success rate (0–100%)."""
        if not 0 <= percent <= 100:
            await ctx.send("Success rate must be between 0 and 100.")
            return
        await self._set_crime_setting(ctx.guild.id, "crime_success_rate", percent)
        await ctx.send(f"`.crime` success rate set to **{percent}%**.")

    @crimeconfig.command(name="penalty")
    @commands.is_owner()
    async def crimeconfig_penalty(self, ctx, percent: int):
        """Owner: set the `.crime` failure penalty as a % of total wallet + bank (0–100%)."""
        if not 0 <= percent <= 100:
            await ctx.send("Penalty must be between 0 and 100.")
            return
        await self._set_crime_setting(ctx.guild.id, "crime_penalty_pct", percent)
        await ctx.send(f"`.crime` failure penalty set to **{percent}%** of total money.")

    @crimeconfig.error
    @crimeconfig_rate.error
    @crimeconfig_penalty.error
    async def crimeconfig_error(self, ctx, error):
        if isinstance(error, commands.NotOwner):
            await ctx.send("Only the owner, an admin, or an owner-role holder can change crime settings.")
        elif isinstance(error, (commands.BadArgument, commands.MissingRequiredArgument)):
            await ctx.send("Usage: `.crimeconfig rate <percent>` or `.crimeconfig penalty <percent>`")
        else:
            raise error

    @commands.group(invoke_without_command=True)
    @commands.is_owner()
    async def collectrole(self, ctx):
        """Owner: manage role salaries collected via `.collect`."""
        await self._show_salary_roles(ctx)

    @collectrole.command(name="bind")
    @commands.is_owner()
    async def collectrole_bind(self, ctx, role: discord.Role, duration: str, amount: str):
        """Owner: bind a role to a salary. Usage: .collectrole bind <role> <time e.g. 1h> <amount>"""
        delta = parse_duration(duration)
        if delta is None or delta.total_seconds() <= 0:
            await ctx.send("Invalid time. Use a duration like `30m`, `1h`, or `2d`.")
            return
        try:
            amount = parse_amount(amount)
        except AmountError as e:
            await ctx.send(str(e))
            return

        interval_seconds = int(delta.total_seconds())
        await set_salary_role(self.pool, ctx.guild.id, role.id, interval_seconds, amount)
        cur = self.bot.get_currency(ctx.guild.id)
        await ctx.send(
            f"Bound {role.mention}: members can `.collect` **{amount:,}**{cur.emoji} "
            f"every **{humanize_duration(interval_seconds)}**."
        )

    @collectrole.command(name="unbind")
    @commands.is_owner()
    async def collectrole_unbind(self, ctx, role: discord.Role):
        """Owner: remove a role's salary."""
        result = await remove_salary_role(self.pool, ctx.guild.id, role.id)
        if result == "DELETE 0":
            await ctx.send(f"{role.mention} doesn't have a salary.")
        else:
            await ctx.send(f"Removed the salary for {role.mention}.")

    @collectrole.command(name="list")
    @commands.is_owner()
    async def collectrole_list(self, ctx):
        """Owner: list all role salaries."""
        await self._show_salary_roles(ctx)

    async def _show_salary_roles(self, ctx):
        rows = await list_salary_roles(self.pool, ctx.guild.id)
        if not rows:
            await ctx.send("No role salaries set. Use `.collectrole bind <role> <time> <amount>` to add one.")
            return
        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Role Salaries", color=discord.Color.gold())
        lines = []
        for i, r in enumerate(rows, start=1):
            role = ctx.guild.get_role(r["role_id"])
            name = role.mention if role else f"`{r['role_id']}` (deleted)"
            lines.append(f"{i}. {name} — **{r['amount']:,}**{cur.emoji} every {humanize_duration(r['interval_seconds'])}")
        embed.description = "\n".join(lines)
        await ctx.send(embed=embed)

    @collectrole.error
    @collectrole_bind.error
    @collectrole_unbind.error
    @collectrole_list.error
    async def collectrole_error(self, ctx, error):
        if isinstance(error, commands.NotOwner):
            await ctx.send("Only the owner, an admin, or an owner-role holder can manage role salaries.")
        elif isinstance(error, commands.RoleNotFound):
            await ctx.send("Role not found. Mention the role, or use its exact name or ID.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send("Usage: `.collectrole bind <role> <time e.g. 1h> <amount>`")
        else:
            raise error

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        """Drop a role's salary binding when the role is deleted."""
        await remove_salary_role(self.pool, role.guild.id, role.id)

    @commands.command()
    @require_not_locked()
    async def gift(self, ctx, member: discord.Member, amount: str):
        """Gift money from your wallet to another user's wallet. You must mention the recipient and specify the amount."""
        bal = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        try:
            amount = parse_amount(amount, wallet_balance=bal["wallet"])
        except AmountError as e:
            await ctx.send(str(e))
            return
        if bal["wallet"] < amount:
            await ctx.send("You can't give more than you have in your wallet!")
            return

        await ensure_wallet(self.pool, ctx.guild.id, member.id)
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await update_wallet(self.pool, ctx.guild.id, member.id, amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, -amount, "gift", f"Gift to {member}")
        await add_transaction(self.pool, ctx.guild.id, member.id, amount, "gift", f"Gift from {ctx.author}")
        cur = self.bot.get_currency(ctx.guild.id)
        await ctx.send(f"You gifted {amount}{cur.emoji} to {member.mention}!")

    @commands.command(aliases=["transactions", "txlog"])
    @require_not_locked()
    async def curtrs(self, ctx, *, args: str = ""):
        """Show your (or someone's) transaction history.
        Usage: .curtrs [@member] [counting]
        Pass 'counting' to see individual counting entries instead of the summary."""
        member = ctx.author
        show_counting_detail = False

        parts = args.split()
        member_parts = []
        for part in parts:
            if part.lower() == "counting":
                show_counting_detail = True
            else:
                member_parts.append(part)

        if member_parts:
            try:
                member = await commands.MemberConverter().convert(ctx, " ".join(member_parts))
            except commands.MemberNotFound:
                await ctx.send("Member not found.")
                return

        if show_counting_detail:
            rows = await self.pool.fetch(
                """SELECT amount, tx_type, description, created_at FROM transactions
                   WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'counting'
                   ORDER BY created_at DESC""",
                ctx.guild.id, member.id,
            )
            if not rows:
                await ctx.send(f"{member.display_name} has no counting transactions.")
                return
            rows = [dict(r) for r in rows]
            view = TransactionPaginator(rows, member, ctx.author.id, self.bot.get_currency(ctx.guild.id))
            await ctx.send(embed=view.build_embed(), view=view)
            return

        other_rows = await self.pool.fetch(
            """SELECT amount, tx_type, description, created_at FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type != 'counting'
               ORDER BY created_at DESC""",
            ctx.guild.id, member.id,
        )
        counting_agg = await self.pool.fetchrow(
            """SELECT COALESCE(SUM(amount), 0) AS amount, COUNT(*) AS cnt, MAX(created_at) AS created_at
               FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'counting'""",
            ctx.guild.id, member.id,
        )

        rows = [dict(r) for r in other_rows]
        has_counting = counting_agg and counting_agg["cnt"] > 0
        if has_counting:
            rows.append({
                "amount": counting_agg["amount"],
                "tx_type": "counting",
                "description": f"counting ×{counting_agg['cnt']}",
                "created_at": counting_agg["created_at"],
            })
            rows.sort(key=lambda r: r["created_at"], reverse=True)

        if not rows:
            await ctx.send(f"{member.display_name} has no transactions.")
            return

        view = TransactionPaginator(rows, member, ctx.author.id, self.bot.get_currency(ctx.guild.id), counting_note=has_counting)
        await ctx.send(embed=view.build_embed(), view=view)

    @commands.command()
    @commands.is_owner()
    async def add(self, ctx, member: discord.Member, amount: str):
        """Add money to a user's wallet."""
        try:
            amount = parse_amount(amount)
        except AmountError as e:
            await ctx.send(str(e))
            return

        cur = self.bot.get_currency(ctx.guild.id)
        if not await confirm(ctx, f"⚠️ Add **{amount:,}**{cur.emoji} to {member.mention}'s wallet?"):
            return

        await ensure_wallet(self.pool, ctx.guild.id, member.id)
        await update_wallet(self.pool, ctx.guild.id, member.id, amount)
        await add_transaction(self.pool, ctx.guild.id, member.id, amount, "admin_add", f"Added by {ctx.author}")
        await ctx.send(f"Added **{amount:,}**{cur.emoji} to {member.mention}'s wallet.")

    @commands.command()
    @commands.is_owner()
    async def lockuser(self, ctx, member: discord.Member, *, flags: str = ""):
        """Lock a user from using economy commands. Pass --delete to also zero their balance and return all shares to IPO."""
        if "--delete" in flags:
            if not await confirm(
                ctx,
                f"⚠️ Lock **{member.display_name}**, **zero their balance**, and return all their shares to IPO?",
            ):
                return

        await self.pool.execute(
            "INSERT INTO locked_users (guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            ctx.guild.id, member.id,
        )
        invalidate_lock(ctx.guild.id, member.id)

        returned = []
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                open_orders = await conn.fetch(
                    "SELECT side, remaining, price FROM orders WHERE guild_id = $1 AND user_id = $2 AND remaining > 0",
                    ctx.guild.id, member.id,
                )
                refund = sum(o["remaining"] * o["price"] for o in open_orders if o["side"] == "buy")
                if refund > 0:
                    await conn.execute(
                        "UPDATE balances SET wallet = wallet + $3 WHERE guild_id = $1 AND user_id = $2",
                        ctx.guild.id, member.id, refund,
                    )
                await conn.execute(
                    "DELETE FROM orders WHERE guild_id = $1 AND user_id = $2",
                    ctx.guild.id, member.id,
                )
                if "--delete" in flags:
                    returned = await remove_member_shares(conn, ctx.guild.id, member.id)
                    await conn.execute(
                        "UPDATE balances SET wallet = 0, bank = 0 WHERE guild_id = $1 AND user_id = $2",
                        ctx.guild.id, member.id,
                    )

        cur = self.bot.get_currency(ctx.guild.id)
        parts = [f"**{member.display_name}** has been locked from the economy."]
        if open_orders:
            parts.append(f"{len(open_orders)} open order(s) cancelled" + (f", {refund}{cur.emoji} escrowed gold refunded." if refund else "."))
        if "--delete" in flags:
            share_lines = ", ".join(f"{h['quantity']}x {h['name']}" for h in returned) if returned else "none"
            parts.append(f"Balance zeroed, shares returned to IPO ({share_lines}).")
        await ctx.send(" ".join(parts))

    @commands.command()
    @commands.is_owner()
    async def unlockuser(self, ctx, member: discord.Member):
        """Unlock a user's access to economy commands."""
        result = await self.pool.execute(
            "DELETE FROM locked_users WHERE guild_id = $1 AND user_id = $2",
            ctx.guild.id, member.id,
        )
        invalidate_lock(ctx.guild.id, member.id)
        if result == "DELETE 0":
            await ctx.send(f"**{member.display_name}** was not locked.")
        else:
            await ctx.send(f"**{member.display_name}** has been unlocked.")

    @commands.command()
    @commands.is_owner()
    async def reseteconomy(self, ctx):
        """Wipe all balances, stock relations and recreate stocks at their original IPO settings."""
        if not await confirm(
            ctx,
            "⚠️ This will zero **all wallets and banks**, delete all transactions and stock relations, "
            "and recreate every stock at 10,000 shares / original IPO price.",
        ):
            return

        async with self.pool.acquire() as conn:
            companies = await conn.fetch(
                "SELECT name, stock_channel_id, listed_by, base_ipo_price FROM companies WHERE guild_id = $1",
                ctx.guild.id,
            )
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM transactions WHERE guild_id = $1",
                    ctx.guild.id,
                )
                await conn.execute(
                    "UPDATE balances SET wallet = 0, bank = 0 WHERE guild_id = $1",
                    ctx.guild.id,
                )
                await conn.execute(
                    "DELETE FROM companies WHERE guild_id = $1",
                    ctx.guild.id,
                )
                await conn.execute(
                    "DELETE FROM locked_users WHERE guild_id = $1",
                    ctx.guild.id,
                )
                for c in companies:
                    await create_company(
                        conn,
                        guild_id=ctx.guild.id,
                        stock_channel_id=c["stock_channel_id"],
                        name=c["name"],
                        listed_by=c["listed_by"],
                        total_shares=10000,
                        ipo_price=c["base_ipo_price"],
                    )
                    await conn.execute(
                        "UPDATE companies SET revenue_multiplier = $3 WHERE guild_id = $1 AND stock_channel_id = $2",
                        ctx.guild.id, c["stock_channel_id"], REVENUE_BASE_MULTIPLIER,
                    )

        embed = discord.Embed(
            title="Economy Reset",
            color=discord.Color.red(),
            description=(
                f"All wallets, banks, and transaction history cleared.\n"
                f"{len(companies)} stock(s) recreated at 10,000 shares / original IPO price.\n"
                "All portfolios, orders, and trade history cleared."
            ),
        )
        await ctx.send(embed=embed)

    @commands.command()
    @commands.is_owner()
    async def remove(self, ctx, member: discord.Member, amount: str):
        """Remove money from a user's wallet and bank."""
        try:
            amount = parse_amount(amount)
        except AmountError as e:
            await ctx.send(str(e))
            return

        cur = self.bot.get_currency(ctx.guild.id)
        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        if bal["wallet"] + bal["bank"] < amount:
            await ctx.send(f"{member.display_name} only has {bal['wallet'] + bal['bank']}{cur.emoji} total.")
            return

        if not await confirm(ctx, f"⚠️ Remove **{amount:,}**{cur.emoji} from {member.mention}'s wallet/bank?"):
            return

        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        if bal["wallet"] + bal["bank"] < amount:
            await ctx.send(f"{member.display_name} only has {bal['wallet'] + bal['bank']}{cur.emoji} total.")
            return

        from_wallet = min(amount, bal["wallet"])
        from_bank = amount - from_wallet

        if from_wallet > 0:
            await update_wallet(self.pool, ctx.guild.id, member.id, -from_wallet)
        if from_bank > 0:
            await update_bank(self.pool, ctx.guild.id, member.id, -from_bank)
        await add_transaction(self.pool, ctx.guild.id, member.id, -amount, "admin_remove", f"Removed by {ctx.author}")
        await ctx.send(f"Removed **{amount:,}**{cur.emoji} from {member.mention}'s wallet/bank.")
