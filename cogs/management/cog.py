import discord
from discord.ext import commands


class Management(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _resolve_cog(self, name: str) -> str | None:
        """Return the canonical cog class name matching name (case-insensitive)."""
        for cog_name in self.bot.cogs:
            if cog_name.lower() == name.lower():
                return cog_name
        return None

    @commands.command()
    @commands.has_permissions(manage_guild=True)
    async def disable(self, ctx: commands.Context, *, name: str):
        """Admin: Disable all commands from a cog in this server."""
        cog_name = self._resolve_cog(name)
        if cog_name is None:
            await ctx.send(f"No cog named **{name}** found.")
            return
        if cog_name == "Management":
            await ctx.send("The Management cog cannot be disabled.")
            return
        await self.bot.pool.execute(
            "INSERT INTO disabled_cogs (guild_id, cog_name) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            ctx.guild.id, cog_name,
        )
        self.bot._disabled_cogs_cache.pop(ctx.guild.id, None)
        await ctx.send(f"**{cog_name}** disabled. Use `.enable {cog_name}` to re-enable it.")

    @commands.command()
    @commands.has_permissions(manage_guild=True)
    async def enable(self, ctx: commands.Context, *, name: str):
        """Admin: Re-enable a previously disabled cog."""
        cog_name = self._resolve_cog(name)
        if cog_name is None:
            await ctx.send(f"No cog named **{name}** found.")
            return
        result = await self.bot.pool.execute(
            "DELETE FROM disabled_cogs WHERE guild_id = $1 AND cog_name = $2",
            ctx.guild.id, cog_name,
        )
        self.bot._disabled_cogs_cache.pop(ctx.guild.id, None)
        if result == "DELETE 0":
            await ctx.send(f"**{cog_name}** is not currently disabled.")
        else:
            await ctx.send(f"**{cog_name}** re-enabled.")

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need the **Manage Server** permission to use this command.")
        else:
            raise error
