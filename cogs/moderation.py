import re
import discord
from datetime import timedelta
from discord.ext import commands


def parse_duration(text: str) -> timedelta | None:
    """Parse a duration string like '10s', '5m', '1h', '1d' into a timedelta."""
    match = re.fullmatch(r"(\d+)([smhd])", text.lower())
    if not match:
        return None
    value, unit = int(match.group(1)), match.group(2)
    units = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}
    return timedelta(**{units[unit]: value})


class Moderation(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def check_target(self, ctx: commands.Context, member: discord.Member) -> bool:
        """Validate that the target is safe to act on. Returns False and sends a message if not."""
        if member == ctx.author:
            await ctx.send("You cannot use this command on yourself.")
            return False
        if member == ctx.guild.me:
            await ctx.send("I cannot use this command on myself.")
            return False
        if member == ctx.guild.owner:
            await ctx.send("You cannot use this command on the server owner.")
            return False
        if member.top_role >= ctx.author.top_role:
            await ctx.send("You cannot target someone with an equal or higher role than yours.")
            return False
        if member.top_role >= ctx.guild.me.top_role:
            await ctx.send("I cannot target someone with an equal or higher role than mine.")
            return False
        return True

    # --- Kick ---

    @commands.command()
    @commands.has_permissions(kick_members=True)
    @commands.bot_has_permissions(kick_members=True)
    async def kick(self, ctx: commands.Context, member: discord.Member, *, reason: str = None):
        """Kick a member from the server."""
        if not await self.check_target(ctx, member):
            return
        try:
            await member.send(
                f"You have been kicked from **{ctx.guild.name}**."
                + (f"\nReason: {reason}" if reason else "")
            )
        except discord.Forbidden:
            pass

        await member.kick(reason=reason)

        embed = discord.Embed(
            title="Member Kicked",
            description=f"{member.mention} has been kicked.",
            color=discord.Color.orange(),
        )
        if reason:
            embed.add_field(name="Reason", value=reason)
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Ban ---

    @commands.command()
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    async def ban(self, ctx: commands.Context, member: discord.Member, *, reason: str = None):
        """Ban a member from the server."""
        if not await self.check_target(ctx, member):
            return
        try:
            await member.send(
                f"You have been banned from **{ctx.guild.name}**."
                + (f"\nReason: {reason}" if reason else "")
            )
        except discord.Forbidden:
            pass

        await member.ban(reason=reason)

        embed = discord.Embed(
            title="Member Banned",
            description=f"{member.mention} has been banned.",
            color=discord.Color.red(),
        )
        if reason:
            embed.add_field(name="Reason", value=reason)
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Unban ---

    @commands.command()
    @commands.has_permissions(ban_members=True)
    @commands.bot_has_permissions(ban_members=True)
    async def unban(self, ctx: commands.Context, user_id: int):
        """Unban a user by their ID."""
        user = await self.bot.fetch_user(user_id)
        await ctx.guild.unban(user)

        embed = discord.Embed(
            title="User Unbanned",
            description=f"**{user}** has been unbanned.",
            color=discord.Color.green(),
        )
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Mute (Timeout) ---

    @commands.command()
    @commands.has_permissions(moderate_members=True)
    @commands.bot_has_permissions(moderate_members=True)
    async def mute(self, ctx: commands.Context, member: discord.Member, duration: str, *, reason: str = None):
        """Timeout a member. Duration: 10s, 5m, 1h, 1d."""
        if not await self.check_target(ctx, member):
            return
        delta = parse_duration(duration)
        if delta is None:
            await ctx.send("Invalid duration. Use a number followed by s/m/h/d (e.g. `10m`, `1h`).")
            return

        if delta > timedelta(days=28):
            await ctx.send("Discord timeouts cannot exceed 28 days.")
            return

        try:
            await member.send(
                f"You have been muted in **{ctx.guild.name}** for {duration}."
                + (f"\nReason: {reason}" if reason else "")
            )
        except discord.Forbidden:
            pass

        await member.timeout(delta, reason=reason)

        embed = discord.Embed(
            title="Member Muted",
            description=f"{member.mention} has been timed out for **{duration}**.",
            color=discord.Color.greyple(),
        )
        if reason:
            embed.add_field(name="Reason", value=reason)
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Unmute ---

    @commands.command()
    @commands.has_permissions(moderate_members=True)
    @commands.bot_has_permissions(moderate_members=True)
    async def unmute(self, ctx: commands.Context, member: discord.Member):
        """Remove timeout from a member."""
        if not await self.check_target(ctx, member):
            return
        await member.timeout(None)

        embed = discord.Embed(
            title="Member Unmuted",
            description=f"{member.mention} has been unmuted.",
            color=discord.Color.green(),
        )
        embed.set_footer(text=f"By {ctx.author}")
        await ctx.send(embed=embed)

    # --- Error Handler ---

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return

        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You don't have permission to use this command.")
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.send("I don't have the required permissions to do that.")
        elif isinstance(error, commands.MemberNotFound):
            await ctx.send("Could not find that member.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Moderation(bot))
