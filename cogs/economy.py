import asyncio
import random
import re
import time
import discord
import aiosqlite
from discord.ext import commands
from utils import is_guild_owner, check_channel_allowed, log_tx

DB_PATH = "data/economy.db"
DEFAULT_WORK_COOLDOWN = 3600
DEFAULT_WORK_MIN = 50
DEFAULT_WORK_MAX = 300

ROB_BASE_CHANCE = 0.20       # 20% base success rate
ROB_MIN_STEAL_PCT = 0.20     # steal at least 20% of target's cash on success
ROB_MAX_STEAL_PCT = 0.40     # steal at most 40% of target's cash on success
ROB_FINE_PCT = 0.25          # fine = 25% of what you would have stolen, paid to victim


class Economy(commands.Cog):
    _owner_commands = {"setcooldown", "setworkpay", "add", "take"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "economy", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS economy (
                user_id INTEGER PRIMARY KEY,
                cash INTEGER DEFAULT 0,
                bank INTEGER DEFAULT 0
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS settings (
                guild_id INTEGER PRIMARY KEY,
                work_cooldown INTEGER DEFAULT 3600,
                work_min INTEGER DEFAULT 50,
                work_max INTEGER DEFAULT 300
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS transactions (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id        INTEGER NOT NULL,
                amount         INTEGER NOT NULL,
                source         TEXT NOT NULL,
                counterpart_id INTEGER,
                timestamp      TEXT NOT NULL
            )"""
        )
        await self.db.commit()
        self.work_cooldowns: dict[tuple[int, int], float] = {}   # (guild_id, user_id) -> last_work_time

    async def cog_unload(self):
        if self.db:
            await self.db.close()

    async def get_account(self, user_id: int) -> tuple[int, int]:
        """Get (cash, bank) for a user, creating the row if it doesn't exist."""
        async with self.db.execute(
            "SELECT cash, bank FROM economy WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            return row[0], row[1]
        await self.db.execute(
            "INSERT INTO economy (user_id, cash, bank) VALUES (?, 0, 0)", (user_id,)
        )
        await self.db.commit()
        return 0, 0

    async def get_work_cooldown(self, guild_id: int) -> int:
        """Get the work cooldown in seconds for a guild."""
        async with self.db.execute(
            "SELECT work_cooldown FROM settings WHERE guild_id = ?", (guild_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else DEFAULT_WORK_COOLDOWN

    async def get_work_pay(self, guild_id: int) -> tuple[int, int]:
        """Get (min, max) work pay for a guild."""
        async with self.db.execute(
            "SELECT work_min, work_max FROM settings WHERE guild_id = ?", (guild_id,)
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            return row[0], row[1]
        return DEFAULT_WORK_MIN, DEFAULT_WORK_MAX

    # --- Balance ---

    @commands.command(aliases=["bal"])
    async def balance(self, ctx: commands.Context, member: discord.Member = None):
        """Check your (or another user's) balance."""
        member = member or ctx.author
        cash, bank = await self.get_account(member.id)

        embed = discord.Embed(
            title=f"{member.display_name}'s Balance",
            color=discord.Color.gold(),
        )
        embed.add_field(name="Cash", value=f"{cash:,} \U0001f338")
        embed.add_field(name="Bank", value=f"{bank:,} \U0001f338")
        embed.add_field(name="Total", value=f"{cash + bank:,} \U0001f338")
        await ctx.send(embed=embed)

    # --- Deposit ---

    @commands.command(aliases=["dep"])
    async def deposit(self, ctx: commands.Context, amount: str):
        """Deposit flowers into your bank. Use 'all' to deposit everything."""
        cash, bank = await self.get_account(ctx.author.id)

        if amount.lower() == "all":
            amount = cash
        else:
            try:
                amount = int(amount)
            except ValueError:
                await ctx.send("Please enter a valid number or `all`.")
                return

        if amount <= 0:
            await ctx.send("You must deposit a positive amount.")
            return
        if amount > cash:
            await ctx.send(f"You only have **{cash:,}** \U0001f338 on hand.")
            return

        await self.db.execute(
            "UPDATE economy SET cash = cash - ?, bank = bank + ? WHERE user_id = ?",
            (amount, amount, ctx.author.id),
        )
        await log_tx(self.db, ctx.author.id, -amount, "deposit")
        await self.db.commit()

        embed = discord.Embed(
            title="Deposit Successful",
            description=f"Deposited **{amount:,}** \U0001f338 into your bank.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Withdraw ---

    @commands.command(aliases=["with"])
    async def withdraw(self, ctx: commands.Context, amount: str):
        """Withdraw flowers from your bank. Use 'all' to withdraw everything."""
        cash, bank = await self.get_account(ctx.author.id)

        if amount.lower() == "all":
            amount = bank
        else:
            try:
                amount = int(amount)
            except ValueError:
                await ctx.send("Please enter a valid number or `all`.")
                return

        if amount <= 0:
            await ctx.send("You must withdraw a positive amount.")
            return
        if amount > bank:
            await ctx.send(f"You only have **{bank:,}** \U0001f338 in your bank.")
            return

        await self.db.execute(
            "UPDATE economy SET cash = cash + ?, bank = bank - ? WHERE user_id = ?",
            (amount, amount, ctx.author.id),
        )
        await log_tx(self.db, ctx.author.id, amount, "withdraw")
        await self.db.commit()

        embed = discord.Embed(
            title="Withdrawal Successful",
            description=f"Withdrew **{amount:,}** \U0001f338 from your bank.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Work ---

    @commands.command()
    async def work(self, ctx: commands.Context):
        """Work to earn some flowers."""
        cooldown = await self.get_work_cooldown(ctx.guild.id)
        key = (ctx.guild.id, ctx.author.id)
        last_used = self.work_cooldowns.get(key, 0)
        remaining = cooldown - (time.time() - last_used)

        if remaining > 0:
            minutes, secs = divmod(int(remaining), 60)
            hours, minutes = divmod(minutes, 60)
            parts = []
            if hours:
                parts.append(f"{hours}h")
            if minutes:
                parts.append(f"{minutes}m")
            if secs or not parts:
                parts.append(f"{secs}s")
            await ctx.send(f"You're on cooldown! Try again in **{' '.join(parts)}**.")
            return

        await self.get_account(ctx.author.id)
        work_min, work_max = await self.get_work_pay(ctx.guild.id)
        earnings = random.randint(work_min, work_max)

        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (earnings, ctx.author.id),
        )
        await log_tx(self.db, ctx.author.id, earnings, "work")
        await self.db.commit()
        self.work_cooldowns[key] = time.time()

        embed = discord.Embed(
            title="Work Complete!",
            description=f"You earned **{earnings:,}** \U0001f338!",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Rob ---

    @commands.command()
    async def rob(self, ctx: commands.Context, member: discord.Member):
        """Attempt to rob another user's wallet. Usage: .rob @someone"""
        if member == ctx.author:
            await ctx.send("You can't rob yourself.")
            return
        if member.bot:
            await ctx.send("You can't rob a bot.")
            return

        robber_cash, _ = await self.get_account(ctx.author.id)
        target_cash, _ = await self.get_account(member.id)

        if target_cash <= 0:
            await ctx.send(f"**{member.display_name}** has nothing in their wallet.")
            return

        # Success chance: base 20%, reduced as target gets richer relative to robber.
        # ratio = target_cash / max(robber_cash, 1); chance halves every time target has 5× more cash.
        ratio = target_cash / max(robber_cash, 1)
        chance = ROB_BASE_CHANCE / (1 + ratio / 5)
        chance = max(0.03, min(chance, ROB_BASE_CHANCE))  # clamp to [3%, 20%]

        steal_amount = int(target_cash * random.uniform(ROB_MIN_STEAL_PCT, ROB_MAX_STEAL_PCT))
        steal_amount = max(steal_amount, 1)

        if random.random() < chance:
            # Success
            steal_amount = min(steal_amount, target_cash)
            await self.db.execute(
                "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
                (steal_amount, member.id),
            )
            await self.db.execute(
                "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
                (steal_amount, ctx.author.id),
            )
            await log_tx(self.db, ctx.author.id, steal_amount, "rob:success", member.id)
            await log_tx(self.db, member.id, -steal_amount, "rob:victim", ctx.author.id)
            await self.db.commit()

            embed = discord.Embed(
                title="Robbery Successful!",
                description=(
                    f"You slipped into **{member.display_name}**'s pockets and got away with "
                    f"**{steal_amount:,}** \U0001f338!"
                ),
                color=discord.Color.green(),
            )
            embed.set_footer(text=f"Success chance was {chance*100:.1f}%")
        else:
            # Failure — block next work shift
            work_key = (ctx.guild.id, ctx.author.id)
            self.work_cooldowns[work_key] = time.time()

            fine = int(steal_amount * ROB_FINE_PCT)
            fine = min(fine, robber_cash)  # can't pay more than you have

            if fine > 0:
                await self.db.execute(
                    "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
                    (fine, ctx.author.id),
                )
                await self.db.execute(
                    "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
                    (fine, member.id),
                )
                await log_tx(self.db, ctx.author.id, -fine, "rob:fine", member.id)
                await log_tx(self.db, member.id, fine, "rob:fine_received", ctx.author.id)
                await self.db.commit()

            embed = discord.Embed(
                title="Caught Red-Handed!",
                description=(
                    f"You were caught trying to rob **{member.display_name}** and paid a fine of "
                    f"**{fine:,}** \U0001f338. You've lost your next work shift."
                ),
                color=discord.Color.red(),
            )
            embed.set_footer(text=f"Success chance was {chance*100:.1f}%")

        await ctx.send(embed=embed)

    # --- Set Cooldown (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def setcooldown(self, ctx: commands.Context, hours: float):
        """Set the work cooldown in hours. Server owner only."""
        if hours < 0:
            await ctx.send("Cooldown must be 0 or more hours.")
            return

        seconds = int(hours * 3600)
        await self.db.execute(
            "INSERT INTO settings (guild_id, work_cooldown) VALUES (?, ?) "
            "ON CONFLICT(guild_id) DO UPDATE SET work_cooldown = ?",
            (ctx.guild.id, seconds, seconds),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Cooldown Updated",
            description=f"Work cooldown set to **{hours}h**.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- Set Work Pay (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def setworkpay(self, ctx: commands.Context, minimum: int, maximum: int):
        """Set the min and max work earnings. Server owner only."""
        if minimum < 0 or maximum < 0:
            await ctx.send("Values must be positive.")
            return
        if minimum > maximum:
            await ctx.send("Minimum cannot be greater than maximum.")
            return

        await self.db.execute(
            "INSERT INTO settings (guild_id, work_min, work_max) VALUES (?, ?, ?) "
            "ON CONFLICT(guild_id) DO UPDATE SET work_min = ?, work_max = ?",
            (ctx.guild.id, minimum, maximum, minimum, maximum),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Work Pay Updated",
            description=f"Work earnings set to **{minimum:,}** - **{maximum:,}** \U0001f338.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- Give ---

    @commands.command(aliases=["pay"])
    async def give(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Give flowers to another user."""
        if member == ctx.author:
            await ctx.send("You cannot give flowers to yourself.")
            return
        if member.bot:
            await ctx.send("You cannot give flowers to a bot.")
            return
        if amount <= 0:
            await ctx.send("You must give a positive amount.")
            return

        cash, _ = await self.get_account(ctx.author.id)
        if amount > cash:
            await ctx.send(f"You only have **{cash:,}** \U0001f338 on hand.")
            return

        await self.get_account(member.id)

        await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
            (amount, ctx.author.id),
        )
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, member.id),
        )
        await log_tx(self.db, ctx.author.id, -amount, "give", member.id)
        await log_tx(self.db, member.id, amount, "give", ctx.author.id)
        await self.db.commit()

        embed = discord.Embed(
            title="Transfer Successful",
            description=f"{ctx.author.mention} gave **{amount:,}** \U0001f338 to {member.mention}.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Add (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def add(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Add flowers to a user's balance. Server owner only."""
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        await self.get_account(member.id)
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, member.id),
        )
        await log_tx(self.db, member.id, amount, "admin:add")
        await self.db.commit()

        embed = discord.Embed(
            title="Flowers Added",
            description=f"Added **{amount:,}** \U0001f338 to {member.mention}.",
            color=discord.Color.green(),
        )
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Take (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def take(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Take flowers from a user's balance. Server owner only."""
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        cash, _ = await self.get_account(member.id)
        if amount > cash:
            await ctx.send(f"{member.display_name} only has **{cash:,}** \U0001f338.")
            return

        await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
            (amount, member.id),
        )
        await log_tx(self.db, member.id, -amount, "admin:take")
        await self.db.commit()

        embed = discord.Embed(
            title="Flowers Taken",
            description=f"Took **{amount:,}** \U0001f338 from {member.mention}.",
            color=discord.Color.red(),
        )
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Currency Transactions ---

    @commands.command(aliases=["curtrs"])
    async def currencytransactions(self, ctx: commands.Context, member: discord.Member = None):
        """View the last 10 cash transactions. Usage: .curtrs [@user]"""
        target = member or ctx.author
        async with self.db.execute(
            "SELECT amount, source, counterpart_id, timestamp FROM transactions "
            "WHERE user_id = ? ORDER BY id DESC LIMIT 10",
            (target.id,),
        ) as cur:
            rows = await cur.fetchall()

        if not rows:
            who = "You have" if target == ctx.author else f"{target.display_name} has"
            await ctx.send(f"{who} no recorded transactions yet.")
            return

        lines = []
        for amount, source, counterpart_id, timestamp in rows:
            sign = "+" if amount >= 0 else ""
            ts = timestamp[:16].replace("T", " ")
            counterpart = ""
            if counterpart_id:
                m = ctx.guild.get_member(counterpart_id)
                counterpart = f" ↔ {m.display_name if m else f'User {counterpart_id}'}"
            lines.append(f"`{ts}` **{sign}{amount:,}** \U0001f338 — {source}{counterpart}")

        who = "Your" if target == ctx.author else f"{target.display_name}'s"
        embed = discord.Embed(
            title=f"{who} Last Transactions",
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- Remind ---

    @commands.command()
    async def remind(self, ctx: commands.Context, time_str: str, *, message: str = None):
        """Set a reminder. Usage: .remind 10m, .remind 2h30m, .remind 1h30m45s [message]"""
        total_seconds = 0
        for value, unit in re.findall(r"(\d+)\s*([dhms])", time_str.lower()):
            value = int(value)
            if unit == "d":
                total_seconds += value * 86400
            elif unit == "h":
                total_seconds += value * 3600
            elif unit == "m":
                total_seconds += value * 60
            elif unit == "s":
                total_seconds += value

        if total_seconds <= 0:
            await ctx.send("Invalid time format. Examples: `10m`, `2h`, `1h30m`, `90s`.")
            return
        if total_seconds > 7 * 86400:
            await ctx.send("Reminders can be at most 7 days.")
            return

        # Format a human-readable duration
        remaining = total_seconds
        parts = []
        for label, secs in (("d", 86400), ("h", 3600), ("m", 60), ("s", 1)):
            if remaining >= secs:
                parts.append(f"{remaining // secs}{label}")
                remaining %= secs
        duration = " ".join(parts)

        await ctx.send(f"Got it! I'll remind you in **{duration}**{f': *{message}*' if message else ''}.")

        await asyncio.sleep(total_seconds)

        mention = ctx.author.mention
        content = f"{mention} Reminder!" + (f" {message}" if message else "")
        try:
            await ctx.send(content)
        except discord.HTTPException:
            try:
                await ctx.author.send(content)
            except discord.Forbidden:
                pass

    # --- Error Handler ---

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return

        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
            await ctx.send("Only the server owner can use this command.")
        elif isinstance(error, commands.MemberNotFound):
            await ctx.send("Could not find that member.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Economy(bot))
