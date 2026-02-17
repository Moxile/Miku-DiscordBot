import random
import time
import discord
import aiosqlite
from discord.ext import commands
from utils import is_guild_owner, check_channel_allowed

DB_PATH = "data/economy.db"
DEFAULT_WORK_COOLDOWN = 3600
DEFAULT_WORK_MIN = 50
DEFAULT_WORK_MAX = 300


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
        await self.db.commit()
        self.work_cooldowns: dict[tuple[int, int], float] = {}  # (guild_id, user_id) -> last_work_time

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
        embed.add_field(name="Cash", value=f"${cash:,}")
        embed.add_field(name="Bank", value=f"${bank:,}")
        embed.add_field(name="Total", value=f"${cash + bank:,}")
        await ctx.send(embed=embed)

    # --- Deposit ---

    @commands.command(aliases=["dep"])
    async def deposit(self, ctx: commands.Context, amount: str):
        """Deposit cash into your bank. Use 'all' to deposit everything."""
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
            await ctx.send(f"You only have **${cash:,}** in cash.")
            return

        await self.db.execute(
            "UPDATE economy SET cash = cash - ?, bank = bank + ? WHERE user_id = ?",
            (amount, amount, ctx.author.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Deposit Successful",
            description=f"Deposited **${amount:,}** into your bank.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Withdraw ---

    @commands.command(aliases=["with"])
    async def withdraw(self, ctx: commands.Context, amount: str):
        """Withdraw money from your bank. Use 'all' to withdraw everything."""
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
            await ctx.send(f"You only have **${bank:,}** in your bank.")
            return

        await self.db.execute(
            "UPDATE economy SET cash = cash + ?, bank = bank - ? WHERE user_id = ?",
            (amount, amount, ctx.author.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Withdrawal Successful",
            description=f"Withdrew **${amount:,}** from your bank.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Work ---

    @commands.command()
    async def work(self, ctx: commands.Context):
        """Work to earn some cash."""
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
        await self.db.commit()
        self.work_cooldowns[key] = time.time()

        embed = discord.Embed(
            title="Work Complete!",
            description=f"You earned **${earnings:,}**!",
            color=discord.Color.green(),
        )
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
            description=f"Work earnings set to **${minimum:,}** - **${maximum:,}**.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- Give ---

    @commands.command(aliases=["pay"])
    async def give(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Give cash to another user."""
        if member == ctx.author:
            await ctx.send("You cannot give money to yourself.")
            return
        if member.bot:
            await ctx.send("You cannot give money to a bot.")
            return
        if amount <= 0:
            await ctx.send("You must give a positive amount.")
            return

        cash, _ = await self.get_account(ctx.author.id)
        if amount > cash:
            await ctx.send(f"You only have **${cash:,}** in cash.")
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
        await self.db.commit()

        embed = discord.Embed(
            title="Transfer Successful",
            description=f"{ctx.author.mention} gave **${amount:,}** to {member.mention}.",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Add (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def add(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Add cash to a user's balance. Server owner only."""
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        await self.get_account(member.id)
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, member.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Cash Added",
            description=f"Added **${amount:,}** to {member.mention}'s cash.",
            color=discord.Color.green(),
        )
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Take (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def take(self, ctx: commands.Context, member: discord.Member, amount: int):
        """Take cash from a user's balance. Server owner only."""
        if amount <= 0:
            await ctx.send("Amount must be positive.")
            return

        cash, _ = await self.get_account(member.id)
        if amount > cash:
            await ctx.send(f"{member.display_name} only has **${cash:,}** in cash.")
            return

        await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
            (amount, member.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Cash Taken",
            description=f"Took **${amount:,}** from {member.mention}'s cash.",
            color=discord.Color.red(),
        )
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

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
