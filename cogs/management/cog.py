import discord
from discord.ext import commands

from core.checks import guild_or_bot_owner
from core.currency import Currency, validate_emoji


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
        """Disable all commands from a cog in this server."""
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
        """Re-enable a previously disabled cog."""
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

    @commands.group(invoke_without_command=True)
    @guild_or_bot_owner()
    async def ownerrole(self, ctx: commands.Context):
        """Server/bot owner: show the role that grants access to all owner commands."""
        role_id = await self.bot._get_owner_role(ctx.guild.id)
        if role_id is None:
            await ctx.send(
                "No owner role is set. Use `.ownerrole create <name>` to make one, "
                "or `.ownerrole set <role>` to use an existing role."
            )
            return
        role = ctx.guild.get_role(role_id)
        if role is None:
            await ctx.send(
                f"The owner role (ID `{role_id}`) no longer exists. "
                "Use `.ownerrole clear` to reset, or set a new one."
            )
        else:
            await ctx.send(f"Members with {role.mention} can use **all** owner commands.")

    @ownerrole.command(name="create")
    @guild_or_bot_owner()
    @commands.bot_has_permissions(manage_roles=True)
    async def ownerrole_create(self, ctx: commands.Context, *, name: str):
        """Server/bot owner: create a new role and register it as the owner role."""
        role = await ctx.guild.create_role(
            name=name, reason=f"Owner role created by {ctx.author}"
        )
        await self._set_owner_role(ctx.guild.id, role.id)
        await ctx.send(
            f"Created {role.mention} and registered it as the owner role. "
            "Anyone you give this role can now use all owner commands."
        )

    @ownerrole.command(name="set")
    @guild_or_bot_owner()
    async def ownerrole_set(self, ctx: commands.Context, *, role: discord.Role):
        """Server/bot owner: register an existing role as the owner role."""
        await self._set_owner_role(ctx.guild.id, role.id)
        await ctx.send(
            f"{role.mention} is now the owner role. "
            "Anyone with it can use all owner commands."
        )

    @ownerrole.command(name="clear")
    @guild_or_bot_owner()
    async def ownerrole_clear(self, ctx: commands.Context):
        """Server/bot owner: stop granting owner access through a role."""
        await self.bot.pool.execute(
            "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'owner_role'",
            ctx.guild.id,
        )
        self.bot._owner_role_cache.pop(ctx.guild.id, None)
        await ctx.send("Owner role cleared. Only the bot owner can use owner commands now.")

    async def _set_owner_role(self, guild_id: int, role_id: int) -> None:
        await self.bot.pool.execute(
            """INSERT INTO guild_settings (guild_id, key, value)
               VALUES ($1, 'owner_role', $2)
               ON CONFLICT (guild_id, key) DO UPDATE SET value = EXCLUDED.value""",
            guild_id, str(role_id),
        )
        self.bot._owner_role_cache.pop(guild_id, None)

    @commands.command()
    @commands.is_owner()
    async def setcurrency(self, ctx: commands.Context, emoji: str = None, *, name: str = None):
        """Owner: set this server's currency emoji and name (e.g. `.setcurrency 🪙 Coins`)."""
        if emoji is None or name is None:
            cur = self.bot.get_currency(ctx.guild.id)
            await ctx.send(
                f"Currency here is **{cur.name}** {cur.emoji}.\n"
                "Set it with `.setcurrency <emoji> <name>` (emoji may be a unicode emoji or a "
                "custom emote from a server I'm in). Use `.resetcurrency` to revert to the default."
            )
            return

        stored_emoji = validate_emoji(self.bot, emoji)
        if stored_emoji is None:
            await ctx.send(
                "That emoji can't be used. Use a unicode emoji (like 🪙) or a custom emote "
                "from a server I'm also in."
            )
            return

        name = name.strip()
        if not name:
            await ctx.send("Please provide a name, e.g. `.setcurrency 🪙 Coins`.")
            return
        await self.bot.pool.execute(
            """INSERT INTO guild_currency (guild_id, name, emoji)
               VALUES ($1, $2, $3)
               ON CONFLICT (guild_id) DO UPDATE SET name = EXCLUDED.name, emoji = EXCLUDED.emoji""",
            ctx.guild.id, name, stored_emoji,
        )
        self.bot._currency_cache[ctx.guild.id] = Currency(name, stored_emoji)
        await ctx.send(f"Currency set to **{name}** {stored_emoji} — e.g. `1,000`{stored_emoji}.")

    @commands.command()
    @commands.is_owner()
    async def resetcurrency(self, ctx: commands.Context):
        """Owner: revert this server's currency to the default."""
        await self.bot.pool.execute(
            "DELETE FROM guild_currency WHERE guild_id = $1", ctx.guild.id
        )
        self.bot._currency_cache.pop(ctx.guild.id, None)
        cur = self.bot.get_currency(ctx.guild.id)
        await ctx.send(f"Currency reset to the default **{cur.name}** {cur.emoji}.")

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.MissingPermissions):
            return
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.send("I need the **Manage Roles** permission to create a role.")
        elif isinstance(error, commands.CheckFailure):
            return
        elif isinstance(error, commands.BadArgument):
            await ctx.send(str(error))
        else:
            raise error
