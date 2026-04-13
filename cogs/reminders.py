import discord
from datetime import datetime, timezone, timedelta
from discord.ext import commands, tasks

from config import REMINDER_MAX_DAYS
from cogs.moderation import parse_duration
from cogs.utils.db import (
    create_reminder, get_due_reminders, delete_reminder,
    get_user_reminders, cancel_reminder,
)


class Reminders(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.check_reminders.start()

    def cog_unload(self):
        self.check_reminders.cancel()

    @property
    def pool(self):
        return self.bot.pool

    # ── Background task ──

    @tasks.loop(seconds=30)
    async def check_reminders(self):
        async with self.pool.acquire() as conn:
            due = await get_due_reminders(conn)
        for row in due:
            channel = self.bot.get_channel(row["channel_id"])
            user = self.bot.get_user(row["user_id"])
            if channel and user:
                msg = row["message"] or "your reminder!"
                try:
                    await channel.send(f"{user.mention} ⏰ {msg}")
                except discord.Forbidden:
                    pass
            async with self.pool.acquire() as conn:
                await delete_reminder(conn, row["id"])

    @check_reminders.before_loop
    async def before_check(self):
        await self.bot.wait_until_ready()

    # ── Commands ──

    @commands.command(aliases=["remind", "remindme"])
    async def remember(self, ctx: commands.Context, duration: str, *, message: str = None):
        """Set a reminder. Duration: 10m, 2h, 1d (max 10d).
        Usage: .remember <duration> [message]"""
        delta = parse_duration(duration)
        if delta is None:
            await ctx.send("Invalid duration. Use a number followed by s/m/h/d (e.g. `10m`, `2h`, `1d`).")
            return
        if delta > timedelta(days=REMINDER_MAX_DAYS):
            await ctx.send(f"Maximum reminder duration is {REMINDER_MAX_DAYS} days.")
            return
        if delta.total_seconds() < 10:
            await ctx.send("Minimum reminder duration is 10 seconds.")
            return

        remind_at = datetime.now(timezone.utc) + delta
        async with self.pool.acquire() as conn:
            row = await create_reminder(conn, ctx.guild.id, ctx.author.id, ctx.channel.id, message, remind_at)

        # Human-readable time
        total = int(delta.total_seconds())
        parts = []
        for unit, secs in (("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)):
            val = total // secs
            total %= secs
            if val:
                parts.append(f"{val} {unit}{'s' if val != 1 else ''}")
        human = ", ".join(parts)

        embed = discord.Embed(
            description=f"Got it! I'll remind you in **{human}**." + (f"\n> {message}" if message else ""),
            color=discord.Color.from_rgb(255, 182, 193),
        )
        embed.set_footer(text=f"Reminder ID: {row['id']}")
        await ctx.send(embed=embed)

    @commands.command(name="reminders", aliases=["myreminders"])
    async def list_reminders(self, ctx: commands.Context):
        """List your pending reminders."""
        async with self.pool.acquire() as conn:
            rows = await get_user_reminders(conn, ctx.guild.id, ctx.author.id)

        if not rows:
            await ctx.send("You have no pending reminders.")
            return

        embed = discord.Embed(title="Your Reminders", color=discord.Color.from_rgb(255, 182, 193))
        now = datetime.now(timezone.utc)
        for row in rows:
            delta = row["remind_at"] - now
            total = max(0, int(delta.total_seconds()))
            parts = []
            for unit, secs in (("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)):
                val = total // secs
                total %= secs
                if val:
                    parts.append(f"{val}{unit[0]}")
            time_left = " ".join(parts) if parts else "soon"
            msg = row["message"] or "*(no message)*"
            embed.add_field(name=f"ID {row['id']} — in {time_left}", value=msg, inline=False)
        await ctx.send(embed=embed)

    @commands.command(aliases=["cancelremind"])
    async def cancelreminder(self, ctx: commands.Context, reminder_id: int):
        """Cancel a pending reminder by its ID."""
        async with self.pool.acquire() as conn:
            row = await cancel_reminder(conn, ctx.guild.id, ctx.author.id, reminder_id)
        if not row:
            await ctx.send("Reminder not found or it doesn't belong to you.")
            return
        await ctx.send(f"Reminder **#{reminder_id}** cancelled.")

    @remember.error
    @cancelreminder.error
    async def reminder_error(self, ctx, error):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Usage: `.remember <duration> [message]` or `.cancelreminder <id>`")
        elif isinstance(error, commands.BadArgument):
            await ctx.send("Invalid argument. Reminder ID must be a number.")


async def setup(bot: commands.Bot):
    await bot.add_cog(Reminders(bot))
