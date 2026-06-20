from __future__ import annotations

import discord
from discord.ext import commands

from cogs.bot_reactions.db import add_reaction, remove_reaction, list_reactions


class BotReactions(commands.Cog, name="BotReactions"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # guild_id -> list of rows, populated lazily and invalidated on mutations
        self._cache: dict[int, list] = {}

    @property
    def pool(self):
        return self.bot.pool

    async def _get_reactions(self, guild_id: int) -> list:
        if guild_id not in self._cache:
            async with self.pool.acquire() as conn:
                self._cache[guild_id] = await list_reactions(conn, guild_id)
        return self._cache[guild_id]

    def _invalidate(self, guild_id: int) -> None:
        self._cache.pop(guild_id, None)

    # ── Listener ──

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.guild is None:
            return

        content = message.content.strip().lower()
        if not content:
            return

        rows = await self._get_reactions(message.guild.id)
        for row in rows:
            if row["trigger"].lower() != content:
                continue
            if row["role_id"] is not None:
                role = message.guild.get_role(row["role_id"])
                if role is None or role not in message.author.roles:
                    continue
            await message.channel.send(row["response"])
            break

    # ── Commands ──

    @commands.group(name="botreaction", aliases=["br"], invoke_without_command=True)
    @commands.has_permissions(manage_guild=True)
    async def botreaction(self, ctx: commands.Context):
        """Manage bot reactions. Subcommands: add, remove, list."""
        await ctx.send_help(ctx.command)

    @botreaction.command(name="add")
    @commands.has_permissions(manage_guild=True)
    async def br_add(self, ctx: commands.Context, trigger: str, response: str, role: discord.Role = None):
        """Add a bot reaction.
        Usage: .br add <trigger> <response> [role]
        Wrap trigger or response in quotes if they contain spaces.
        If a role is given, only members with that role will trigger it."""
        async with self.pool.acquire() as conn:
            row = await add_reaction(
                conn, ctx.guild.id, trigger.lower(), response,
                role.id if role else None, ctx.author.id,
            )

        if row is None:
            await ctx.send(f'A reaction for trigger `{trigger}` already exists. Remove it first.')
            return

        self._invalidate(ctx.guild.id)

        role_str = f" (role: {role.mention})" if role else " (no role restriction)"
        embed = discord.Embed(
            title="Bot reaction added",
            description=f"Trigger: `{trigger}`\nResponse: {response}{role_str}",
            color=discord.Color.from_rgb(255, 165, 0),
        )
        await ctx.send(embed=embed)

    @botreaction.command(name="remove", aliases=["rm", "delete"])
    @commands.has_permissions(manage_guild=True)
    async def br_remove(self, ctx: commands.Context, *, trigger: str):
        """Remove a bot reaction by its trigger.
        Usage: .br remove <trigger>"""
        async with self.pool.acquire() as conn:
            row = await remove_reaction(conn, ctx.guild.id, trigger.lower())

        if row is None:
            await ctx.send(f'No reaction found for trigger `{trigger}`.')
            return

        self._invalidate(ctx.guild.id)
        await ctx.send(f'Removed reaction for trigger `{trigger}`.')

    @botreaction.command(name="list")
    @commands.has_permissions(manage_guild=True)
    async def br_list(self, ctx: commands.Context):
        """List all bot reactions in this server."""
        rows = await self._get_reactions(ctx.guild.id)

        if not rows:
            await ctx.send('No bot reactions configured.')
            return

        embed = discord.Embed(title='Bot Reactions', color=discord.Color.from_rgb(255, 165, 0))
        for row in rows:
            role_str = 'Anyone'
            if row['role_id']:
                role = ctx.guild.get_role(row['role_id'])
                role_str = role.mention if role else f'*(deleted role {row["role_id"]})*'
            embed.add_field(
                name=f'`{row["trigger"]}`',
                value=f'Response: {row["response"]}\nRole: {role_str}',
                inline=False,
            )
        await ctx.send(embed=embed)

    # ── Error handler ──

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.MissingPermissions):
            return
        elif isinstance(error, commands.RoleNotFound):
            await ctx.send("Couldn't find that role.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f'Missing argument. Check `.help {ctx.command.qualified_name}`.')
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f'Invalid argument. Check `.help {ctx.command.qualified_name}`.')
        else:
            raise error
