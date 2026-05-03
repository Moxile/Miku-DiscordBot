import math

import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, update_bank, add_transaction
from cogs.market.db import remove_member_shares
from core.money import parse_amount, AmountError

import datetime
import random

from config import MAIN_CURRENCY_EMOJI, CURRENCY_NAME, WORK_COOLDOWN
from core.checks import require_channel, WrongChannel, invalidate, require_not_locked, UserLocked, invalidate_lock

PER_PAGE = 10


class TransactionPaginator(discord.ui.View):
    def __init__(self, rows: list, member: discord.Member, invoker_id: int, *, timeout=120):
        super().__init__(timeout=timeout)
        self.rows = rows
        self.member = member
        self.invoker_id = invoker_id
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
            lines.append(f"`{date}` **{sign}{row['amount']:,}**{MAIN_CURRENCY_EMOJI} — {desc}")
        embed.description = "\n".join(lines) if lines else "No transactions."
        embed.set_footer(text=f"Page {self.page + 1}/{self.max_page + 1} — {len(self.rows)} total transactions")
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
        if isinstance(error, UserLocked):
            return
        raise error

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

        embed = discord.Embed(title="Deposit", description=f"You deposited {amount}{MAIN_CURRENCY_EMOJI} into your bank account!", color=discord.Color.blue())
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

        embed = discord.Embed(title="Withdraw", description=f"You withdrew {amount}{MAIN_CURRENCY_EMOJI} from your bank account!", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["bal", "b", "$"])
    @require_not_locked()
    async def balance(self, ctx, member: discord.Member = None):
        """Check your balance or someone else's balance. You can mention a member to check their balance."""
        member = member or ctx.author
        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        wallet = bal["wallet"]
        bank = bal["bank"]

        embed = discord.Embed(title=f"{member.display_name}'s Balance", color=discord.Color.green())
        embed.add_field(name=f"Wallet ({CURRENCY_NAME})", value=f"{wallet}{MAIN_CURRENCY_EMOJI}")
        embed.add_field(name=f"Bank ({CURRENCY_NAME})", value=f"{bank}{MAIN_CURRENCY_EMOJI}")
        embed.add_field(name="Total", value=f"{wallet + bank}{MAIN_CURRENCY_EMOJI}")
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
            earnings = random.randint(100, 300)
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

            embed = discord.Embed(title="Work", description=f"You earned {earnings}{MAIN_CURRENCY_EMOJI} from your work!", color=discord.Color.blue())
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
        """Admin: Set (or clear) the channel where .work is allowed."""
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
        await ctx.send(f"You gifted {amount}{MAIN_CURRENCY_EMOJI} to {member.mention}!")

    @commands.command(aliases=["transactions", "txlog"])
    @require_not_locked()
    async def curtrs(self, ctx, member: discord.Member = None):
        """Show your (or someone's) full transaction history with pagination.
        Usage: .curtrs [@member]"""
        member = member or ctx.author
        rows = await self.pool.fetch(
            """SELECT amount, tx_type, description, created_at FROM transactions
               WHERE guild_id = $1 AND user_id = $2
               ORDER BY created_at DESC""",
            ctx.guild.id, member.id,
        )
        if not rows:
            await ctx.send(f"{member.display_name} has no transactions.")
            return

        view = TransactionPaginator(rows, member, ctx.author.id)
        await ctx.send(embed=view.build_embed(), view=view)

    @commands.command()
    @commands.is_owner()
    async def add(self, ctx, member: discord.Member, amount: str):
        """Admin: Add money to a user's wallet."""
        try:
            amount = parse_amount(amount)
        except AmountError as e:
            await ctx.send(str(e))
            return

        await ensure_wallet(self.pool, ctx.guild.id, member.id)
        await update_wallet(self.pool, ctx.guild.id, member.id, amount)
        await add_transaction(self.pool, ctx.guild.id, member.id, amount, "admin_add", f"Added by {ctx.author}")

    @commands.command()
    @commands.is_owner()
    async def lockuser(self, ctx, member: discord.Member, *, flags: str = ""):
        """Admin: Lock a user from using economy commands. Pass --delete to also zero their balance and return all shares to IPO."""
        await self.pool.execute(
            "INSERT INTO locked_users (guild_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            ctx.guild.id, member.id,
        )
        invalidate_lock(ctx.guild.id, member.id)

        wipe_msg = ""
        if "--delete" in flags:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    returned = await remove_member_shares(conn, ctx.guild.id, member.id)
                    await conn.execute(
                        "UPDATE balances SET wallet = 0, bank = 0 WHERE guild_id = $1 AND user_id = $2",
                        ctx.guild.id, member.id,
                    )
            share_lines = ", ".join(f"{h['quantity']}x {h['name']}" for h in returned) if returned else "none"
            wipe_msg = f" Balance zeroed and shares returned to IPO ({share_lines})."

        await ctx.send(f"**{member.display_name}** has been locked from the economy.{wipe_msg}")

    @commands.command()
    @commands.is_owner()
    async def unlockuser(self, ctx, member: discord.Member):
        """Admin: Unlock a user's access to economy commands."""
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
    async def remove(self, ctx, member: discord.Member, amount: str):
        """Admin: Remove money from a user's wallet and bank."""
        try:
            amount = parse_amount(amount)
        except AmountError as e:
            await ctx.send(str(e))
            return

        bal = await ensure_wallet(self.pool, ctx.guild.id, member.id)
        if bal["wallet"] + bal["bank"] < amount:
            await ctx.send(f"{member.display_name} only has {bal['wallet'] + bal['bank']}{MAIN_CURRENCY_EMOJI} total.")
            return

        from_wallet = min(amount, bal["wallet"])
        from_bank = amount - from_wallet

        if from_wallet > 0:
            await update_wallet(self.pool, ctx.guild.id, member.id, -from_wallet)
        if from_bank > 0:
            await update_bank(self.pool, ctx.guild.id, member.id, -from_bank)
        await add_transaction(self.pool, ctx.guild.id, member.id, -amount, "admin_remove", f"Removed by {ctx.author}")
