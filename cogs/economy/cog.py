import datetime
import math

import discord
from discord.ext import commands, tasks

from cogs.economy import service
from cogs.economy.db import (
    ensure_wallet, update_wallet, update_bank, add_transaction, remove_member_data,
    set_salary_role, remove_salary_role, list_salary_roles,
    get_jail_config, get_due_jails, remove_jail,
)
from cogs.market.db import remove_member_shares, create_company
from core.money import parse_amount, AmountError
from core.time_utils import parse_duration, humanize_duration
from core.confirm import confirm
from config import REVENUE_BASE_MULTIPLIER, WORK_COOLDOWN, DEFAULT_JAIL_DURATION

from core.currency import Currency
from core.checks import require_channel, invalidate, require_not_locked, invalidate_lock
from core.names import format_name

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
            title=f"{format_name(self.member)}'s Transactions",
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
        self.release_jails.start()

    def cog_unload(self):
        self.release_jails.cancel()

    @property
    def pool(self):
        return self.bot.pool

    # ── Jail release (removes expired .crime prisoner roles) ──

    @tasks.loop(seconds=30)
    async def release_jails(self):
        """Take the prisoner role back off members whose jail sentence has expired.

        Runs off the DB so sentences survive restarts. Rows are cleared even when the
        role/member/guild is gone, so a stale sentence can't wedge the loop."""
        now = datetime.datetime.now(datetime.timezone.utc)
        due = await get_due_jails(self.pool, now)
        for row in due:
            guild = self.bot.get_guild(row["guild_id"])
            if guild is not None:
                member = guild.get_member(row["user_id"])
                role = guild.get_role(row["role_id"])
                if member is not None and role is not None and role in member.roles:
                    try:
                        await member.remove_roles(role, reason="Crime jail sentence served")
                    except discord.HTTPException:
                        continue  # keep the row; retry next cycle
            await remove_jail(self.pool, row["guild_id"], row["user_id"])

    @release_jails.before_loop
    async def before_release_jails(self):
        await self.bot.wait_until_ready()

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
        amount = await service.deposit(self.pool, ctx.guild.id, ctx.author.id, amount)
        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Deposit", description=f"You deposited {amount}{cur.emoji} into your bank account!", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["with", "w"])
    @require_not_locked()
    async def withdraw(self, ctx, amount: str):
        """Withdraw money from your bank account into your wallet. You can specify and amount or use 'all' to withdraw everything."""
        amount = await service.withdraw(self.pool, ctx.guild.id, ctx.author.id, amount)
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
        embed = discord.Embed(title=f"{format_name(member)}'s Balance", color=discord.Color.green())
        embed.add_field(name=f"Wallet ({cur.name})", value=f"{wallet}{cur.emoji}")
        embed.add_field(name=f"Bank ({cur.name})", value=f"{bank}{cur.emoji}")
        embed.add_field(name="Total", value=f"{wallet + bank}{cur.emoji}")
        embed.set_thumbnail(url=member.display_avatar.url)

        await ctx.send(embed=embed)

    @commands.command()
    @require_not_locked()
    @require_channel("work_channel")
    async def work(self, ctx):
        """Work to earn some money"""
        earnings = await service.work(self.pool, ctx.guild.id, ctx.author.id, ctx.channel.id)
        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Work", description=f"You earned {earnings}{cur.emoji} from your work!", color=discord.Color.blue())
        await ctx.send(embed=embed)

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
    @commands.is_owner()
    async def setworkcooldown(self, ctx, minutes: int = None):
        """Set (or reset) the work cooldown in minutes for this server."""
        if minutes is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'work_cooldown'",
                ctx.guild.id,
            )
            await ctx.send(f"Work cooldown reset to default ({WORK_COOLDOWN // 60} minutes).")
        elif minutes <= 0:
            await ctx.send("Cooldown must be at least 1 minute.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'work_cooldown', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(minutes * 60),
            )
            await ctx.send(f"Work cooldown set to {minutes} minute(s) for this server.")

    @commands.command()
    @require_not_locked()
    @require_channel("work_channel")
    async def collect(self, ctx):
        """Collect the salary for every salaried role you hold whose timer is ready."""
        result = await service.collect(self.pool, ctx.guild, ctx.author, ctx.channel.id)
        if result.collected:
            cur = self.bot.get_currency(ctx.guild.id)
            embed = discord.Embed(title="Salary Collected", color=discord.Color.green())
            embed.description = "\n".join(f"**{name}** — +{amt:,}{cur.emoji}" for name, amt in result.collected)
            embed.add_field(name="Total", value=f"{result.total:,}{cur.emoji}")
            if result.on_cooldown:
                embed.add_field(
                    name="Not ready yet",
                    value="\n".join(f"**{name}** — in {humanize_duration(rem, short=True)}" for name, rem in result.on_cooldown),
                    inline=False,
                )
            await ctx.send(embed=embed)
        else:
            soonest = min(result.on_cooldown, key=lambda x: x[1])
            await ctx.send(
                f"You've already collected all your salaries. Next up: **{soonest[0]}** in "
                f"*{humanize_duration(soonest[1], short=True)}*."
            )

    @commands.command()
    async def salaries(self, ctx):
        """List every role that pays a salary via `.collect`, and how much."""
        await self._show_salary_roles(ctx)

    # ── Crime ──

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
        result = await service.crime(self.pool, ctx.guild.id, ctx.author.id, ctx.channel.id)
        cur = self.bot.get_currency(ctx.guild.id)

        if result.success:
            embed = discord.Embed(
                title="Crime — Success 🤑",
                description=f"You pulled it off and got away with **{result.payout:,}**{cur.emoji}!",
                color=discord.Color.green(),
            )
        else:
            embed = discord.Embed(
                title="Crime — Busted 🚔",
                description=f"You got caught and were fined **{result.loss:,}**{cur.emoji}.",
                color=discord.Color.red(),
            )
            await self._apply_jail(ctx, result, embed)
        await ctx.send(embed=embed)

    async def _apply_jail(self, ctx, result, embed):
        """Put the prisoner role on a busted member (the sentence is already recorded
        in the DB by service.crime, so the background task frees them either way)."""
        if result.jail_role_id is None:
            return
        role = ctx.guild.get_role(result.jail_role_id)
        served = humanize_duration(result.jail_seconds, short=True)
        if role is None:
            embed.add_field(name="🔒 Jailed", value=f"Sentenced to **{served}** (prisoner role missing).", inline=False)
            return
        try:
            await ctx.author.add_roles(role, reason="Busted committing a crime")
        except discord.Forbidden:
            embed.add_field(name="🔒 Jailed",
                            value=f"Sentenced to **{served}** — but I lack permission to grant {role.mention}.",
                            inline=False)
            return
        except discord.HTTPException:
            embed.add_field(name="🔒 Jailed", value=f"Sentenced to **{served}**.", inline=False)
            return
        embed.add_field(name="🔒 Jailed", value=f"You're in {role.mention} for **{served}**.", inline=False)

    @commands.group(invoke_without_command=True)
    @commands.is_owner()
    async def crimeconfig(self, ctx):
        """Owner: view or change the `.crime` success rate, payout and fine."""
        rate, payout, fine = await service.get_crime_config(self.pool, ctx.guild.id)
        jail_role_id, jail_seconds = await get_jail_config(self.pool, ctx.guild.id)
        role = ctx.guild.get_role(jail_role_id) if jail_role_id else None
        if jail_role_id is None:
            jail_value = "Off — bind a role with `.crimeconfig jailrole <role>`"
        else:
            role_label = role.mention if role else f"`{jail_role_id}` (deleted)"
            jail_value = f"{role_label} for {humanize_duration(jail_seconds, short=True)}"

        cur = self.bot.get_currency(ctx.guild.id)
        ev = payout * rate // 100 - fine * (100 - rate) // 100

        embed = discord.Embed(title="Crime Settings", color=discord.Color.dark_red())
        embed.add_field(name="Success rate", value=f"{rate}%")
        embed.add_field(name="Payout on success", value=f"{payout:,}{cur.emoji}")
        embed.add_field(name="Fine on failure", value=f"{fine:,}{cur.emoji}")
        embed.add_field(name="Expected value", value=f"{ev:+,}{cur.emoji} per attempt", inline=False)
        embed.add_field(name="Jail on bust", value=jail_value, inline=False)
        embed.set_footer(text="Change with .crimeconfig rate <percent>, payout/fine <amount>, "
                              "jailrole <role>, or jailtime <duration>")
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

    @crimeconfig.command(name="payout")
    @commands.is_owner()
    async def crimeconfig_payout(self, ctx, amount: int):
        """Owner: set the flat reward for a successful `.crime`."""
        if amount < 0:
            await ctx.send("Payout can't be negative.")
            return
        await self._set_crime_setting(ctx.guild.id, "crime_payout", amount)
        await ctx.send(f"`.crime` success payout set to **{amount:,}**.")

    @crimeconfig.command(name="fine")
    @commands.is_owner()
    async def crimeconfig_fine(self, ctx, amount: int):
        """Owner: set the flat fine for a failed `.crime` (can push a wallet negative)."""
        if amount < 0:
            await ctx.send("Fine can't be negative.")
            return
        await self._set_crime_setting(ctx.guild.id, "crime_fine", amount)
        await ctx.send(f"`.crime` failure fine set to **{amount:,}**.")

    @crimeconfig.command(name="jailrole")
    @commands.is_owner()
    async def crimeconfig_jailrole(self, ctx, role: discord.Role = None):
        """Owner: bind (or clear) the prisoner role given on a failed `.crime`.
        Pass a role to enable jail, or nothing to disable it. Usage: .crimeconfig jailrole @Prisoner"""
        if role is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'crime_jail_role'",
                ctx.guild.id,
            )
            await ctx.send("Crime jail disabled — no prisoner role will be given on a failed `.crime`.")
            return
        if role >= ctx.guild.me.top_role:
            await ctx.send(
                f"I can't manage {role.mention} — it's above my highest role. "
                "Move my role above it (or pick a lower role) so I can jail and free members."
            )
            return
        await self._set_crime_setting(ctx.guild.id, "crime_jail_role", role.id)
        _, jail_seconds = await get_jail_config(self.pool, ctx.guild.id)
        await ctx.send(
            f"Prisoner role set to {role.mention}. Busted criminals wear it for "
            f"**{humanize_duration(jail_seconds, short=True)}** (change with `.crimeconfig jailtime <duration>`)."
        )

    @crimeconfig.command(name="jailtime")
    @commands.is_owner()
    async def crimeconfig_jailtime(self, ctx, *, duration: str = None):
        """Owner: set how long the prisoner role is worn after a failed `.crime`.
        Usage: .crimeconfig jailtime 10m  (also accepts e.g. 30m, 1h, 2d)"""
        if duration is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'crime_jail_duration'",
                ctx.guild.id,
            )
            await ctx.send(f"Jail time reset to the default (**{humanize_duration(DEFAULT_JAIL_DURATION, short=True)}**).")
            return
        delta = parse_duration(duration)
        if delta is None or delta.total_seconds() <= 0:
            await ctx.send("Invalid time. Use a duration like `10m`, `1h`, or `2d`.")
            return
        seconds = int(delta.total_seconds())
        await self._set_crime_setting(ctx.guild.id, "crime_jail_duration", seconds)
        await ctx.send(f"Jail time set to **{humanize_duration(seconds, short=True)}** for failed `.crime`s.")

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

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        """Drop a role's salary binding — and its use as the crime prisoner role — when
        the role is deleted."""
        await remove_salary_role(self.pool, role.guild.id, role.id)
        await self.pool.execute(
            "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'crime_jail_role' AND value = $2",
            role.guild.id, str(role.id),
        )

    @commands.command()
    @require_not_locked()
    async def gift(self, ctx, member: discord.Member, amount: str):
        """Gift money from your wallet to another user's wallet. You must mention the recipient and specify the amount."""
        amount = await service.gift(self.pool, ctx.guild.id, ctx.author, member, amount)
        self.bot.dispatch("money_gift", ctx.guild.id, ctx.author.id, member.id, amount)
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
            rows, _ = await service.fetch_transactions(self.pool, ctx.guild.id, member.id, counting_detail=True)
            if not rows:
                await ctx.send(f"{format_name(member)} has no counting transactions.")
                return
            view = TransactionPaginator(rows, member, ctx.author.id, self.bot.get_currency(ctx.guild.id))
            await ctx.send(embed=view.build_embed(), view=view)
            return

        rows, has_counting = await service.fetch_transactions(self.pool, ctx.guild.id, member.id)
        if not rows:
            await ctx.send(f"{format_name(member)} has no transactions.")
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
                f"⚠️ Lock **{format_name(member)}**, **zero their balance**, and return all their shares to IPO?",
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
        parts = [f"**{format_name(member)}** has been locked from the economy."]
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
            await ctx.send(f"**{format_name(member)}** was not locked.")
        else:
            await ctx.send(f"**{format_name(member)}** has been unlocked.")

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
            await ctx.send(f"{format_name(member)} only has {bal['wallet'] + bal['bank']}{cur.emoji} total.")
            return

        if not await confirm(ctx, f"⚠️ Remove **{amount:,}**{cur.emoji} from {member.mention}'s wallet/bank?"):
            return

        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        if bal["wallet"] + bal["bank"] < amount:
            await ctx.send(f"{format_name(member)} only has {bal['wallet'] + bal['bank']}{cur.emoji} total.")
            return

        from_wallet = min(amount, bal["wallet"])
        from_bank = amount - from_wallet

        if from_wallet > 0:
            await update_wallet(self.pool, ctx.guild.id, member.id, -from_wallet)
        if from_bank > 0:
            await update_bank(self.pool, ctx.guild.id, member.id, -from_bank)
        await add_transaction(self.pool, ctx.guild.id, member.id, -amount, "admin_remove", f"Removed by {ctx.author}")
        await ctx.send(f"Removed **{amount:,}**{cur.emoji} from {member.mention}'s wallet/bank.")
