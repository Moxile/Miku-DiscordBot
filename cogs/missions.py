import discord
import aiosqlite
from discord.ext import commands
from utils import is_guild_owner, check_channel_allowed

DB_PATH = "data/economy.db"


class Missions(commands.Cog):
    _owner_commands = {"createmission", "deletemission", "editmission"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "missions", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS missions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                cost INTEGER NOT NULL,
                funded INTEGER DEFAULT 0,
                completed INTEGER DEFAULT 0
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS mission_contributions (
                mission_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (mission_id, user_id)
            )"""
        )
        await self.db.commit()

    async def cog_unload(self):
        if self.db:
            await self.db.close()

    # --- Create Mission (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def createmission(self, ctx: commands.Context, title: str, cost: int):
        """Create a mission. Usage: {prefix}createmission "Mission Title" 5000. Server owner only."""
        if cost <= 0:
            await ctx.send("Cost must be a positive number.")
            return

        async with self.db.execute(
            "SELECT id FROM missions WHERE guild_id = ? AND LOWER(title) = LOWER(?) AND completed = 0",
            (ctx.guild.id, title),
        ) as cursor:
            existing = await cursor.fetchone()

        if existing:
            await ctx.send(f"An active mission called **{title}** already exists.")
            return

        async with self.db.execute(
            """INSERT INTO missions (guild_id, title, description, cost)
               VALUES (?, ?, ?, ?)""",
            (ctx.guild.id, title, "No description.", cost),
        ) as cursor:
            mission_id = cursor.lastrowid
        await self.db.commit()

        embed = discord.Embed(
            title="Mission Created",
            description=f"**{title}** — Goal: **${cost:,}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="ID", value=str(mission_id))
        embed.set_footer(text=f"Use {ctx.prefix}editmission description \"Title\" <text> to add a description")
        await ctx.send(embed=embed)

    # --- Delete Mission (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def deletemission(self, ctx: commands.Context, *, title: str):
        """Delete a mission. Usage: {prefix}deletemission "Title". Server owner only."""
        async with self.db.execute(
            "SELECT id, title FROM missions WHERE guild_id = ? AND LOWER(title) = LOWER(?)",
            (ctx.guild.id, title),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            await ctx.send(f"No mission called **{title}** found.")
            return

        mission_id = row[0]
        await self.db.execute("DELETE FROM mission_contributions WHERE mission_id = ?", (mission_id,))
        await self.db.execute("DELETE FROM missions WHERE id = ?", (mission_id,))
        await self.db.commit()

        embed = discord.Embed(
            title="Mission Deleted",
            description=f"**{row[1]}** has been removed.",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    # --- Edit Mission (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def editmission(self, ctx: commands.Context, field: str, title: str, *, value: str):
        """Edit a mission. Usage: {prefix}editmission <field> "Title" <value>. Server owner only.
        Fields: title, description, cost."""
        allowed_fields = {"title", "description", "cost"}
        field = field.lower()
        if field not in allowed_fields:
            await ctx.send(f"Invalid field. Choose from: {', '.join(allowed_fields)}")
            return

        async with self.db.execute(
            "SELECT id FROM missions WHERE guild_id = ? AND LOWER(title) = LOWER(?)",
            (ctx.guild.id, title),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            await ctx.send(f"No mission called **{title}** found.")
            return

        mission_id = row[0]

        if field == "cost":
            try:
                value = int(value)
                if value <= 0:
                    await ctx.send("Cost must be a positive number.")
                    return
            except ValueError:
                await ctx.send("Invalid number.")
                return

        await self.db.execute(
            f"UPDATE missions SET {field} = ? WHERE id = ?",
            (value, mission_id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Mission Updated",
            description=f"**{title}** — `{field}` set to **{value}**.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- List Active Missions ---

    @commands.command()
    async def missions(self, ctx: commands.Context):
        """View all active missions."""
        async with self.db.execute(
            "SELECT id, title, description, cost, funded FROM missions WHERE guild_id = ? AND completed = 0",
            (ctx.guild.id,),
        ) as cursor:
            rows = await cursor.fetchall()

        if not rows:
            await ctx.send("There are no active missions.")
            return

        embed = discord.Embed(
            title=f"{ctx.guild.name}'s Missions",
            color=discord.Color.gold(),
        )

        for mission_id, title, description, cost, funded in rows:
            pct = funded / cost if cost > 0 else 0
            filled = round(pct * 10)
            bar = "\u2588" * filled + "\u2591" * (10 - filled)
            details = (
                f"{description}\n"
                f"**Goal:** ${cost:,} | **Funded:** ${funded:,} ({pct:.0%})\n"
                f"`[{bar}]`"
            )
            embed.add_field(
                name=f"#{mission_id} — {title}",
                value=details,
                inline=False,
            )

        await ctx.send(embed=embed)

    # --- List Completed Missions ---

    @commands.command()
    async def completedmissions(self, ctx: commands.Context):
        """View all completed missions."""
        async with self.db.execute(
            "SELECT id, title, description, cost FROM missions WHERE guild_id = ? AND completed = 1",
            (ctx.guild.id,),
        ) as cursor:
            rows = await cursor.fetchall()

        if not rows:
            await ctx.send("No completed missions yet.")
            return

        embed = discord.Embed(
            title=f"{ctx.guild.name}'s Completed Missions",
            color=discord.Color.green(),
        )

        for mission_id, title, description, cost in rows:
            embed.add_field(
                name=f"#{mission_id} — {title}",
                value=f"{description}\n**Goal:** ${cost:,} — **Completed!**",
                inline=False,
            )

        await ctx.send(embed=embed)

    # --- Fund a Mission ---

    @commands.command()
    async def fund(self, ctx: commands.Context, mission_id: int, amount: str):
        """Contribute cash to a mission. Usage: {prefix}fund <mission_id> <amount|all>."""
        # Fetch mission
        async with self.db.execute(
            "SELECT title, cost, funded FROM missions WHERE id = ? AND guild_id = ? AND completed = 0",
            (mission_id, ctx.guild.id),
        ) as cursor:
            mission = await cursor.fetchone()

        if not mission:
            await ctx.send("Mission not found or already completed.")
            return

        title, cost, funded = mission
        remaining = cost - funded

        # Fetch user cash
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (ctx.author.id,)
        ) as cursor:
            row = await cursor.fetchone()

        cash = row[0] if row else 0

        if amount.lower() == "all":
            amount = min(cash, remaining)
        else:
            try:
                amount = int(amount)
            except ValueError:
                await ctx.send("Invalid amount. Use a number or `all`.")
                return

        if amount <= 0:
            await ctx.send("You must contribute a positive amount.")
            return

        # Cap to remaining goal
        amount = min(amount, remaining)

        if cash < amount:
            await ctx.send(f"You don't have enough cash. You have **${cash:,}** but tried to fund **${amount:,}**.")
            return

        # Deduct cash
        await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
            (amount, ctx.author.id),
        )

        # Update mission funded amount
        new_funded = funded + amount
        completed = 1 if new_funded >= cost else 0
        await self.db.execute(
            "UPDATE missions SET funded = ?, completed = ? WHERE id = ?",
            (new_funded, completed, mission_id),
        )

        # Track contribution
        await self.db.execute(
            """INSERT INTO mission_contributions (mission_id, user_id, amount)
               VALUES (?, ?, ?)
               ON CONFLICT(mission_id, user_id)
               DO UPDATE SET amount = amount + ?""",
            (mission_id, ctx.author.id, amount, amount),
        )

        await self.db.commit()

        if completed:
            embed = discord.Embed(
                title="Mission Complete!",
                description=f"**{title}** has been fully funded! Goal of **${cost:,}** reached!",
                color=discord.Color.green(),
            )
            embed.set_footer(text=f"Final contribution by {ctx.author.display_name}")
        else:
            pct = new_funded / cost
            filled = round(pct * 10)
            bar = "\u2588" * filled + "\u2591" * (10 - filled)
            embed = discord.Embed(
                title="Mission Funded",
                description=(
                    f"You contributed **${amount:,}** to **{title}**!\n"
                    f"**Progress:** ${new_funded:,} / ${cost:,} ({pct:.0%})\n"
                    f"`[{bar}]`"
                ),
                color=discord.Color.blue(),
            )

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
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Missions(bot))
