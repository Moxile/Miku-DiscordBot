import re
import discord
from discord.ext import commands

from cogs.utils.db import (
    add_reaction_role,
    get_reaction_role,
    remove_reaction_role,
    list_reaction_roles_for_message,
    list_reaction_roles_for_guild,
    clear_reaction_roles_for_message,
    delete_reaction_roles_for_role,
)

CUSTOM_EMOJI_RE = re.compile(r"<(a)?:([A-Za-z0-9_~]+):(\d+)>")


def parse_emoji(bot: commands.Bot, guild: discord.Guild, raw: str) -> tuple[discord.PartialEmoji, str, bool] | None:
    """Parse a user-supplied emoji string into (PartialEmoji, storage_key, is_custom).

    Returns None if the emoji can't be resolved or is a custom emoji the bot
    can't access (custom emojis from other guilds can't be reacted with).
    """
    raw = raw.strip()
    match = CUSTOM_EMOJI_RE.fullmatch(raw)
    if match:
        animated = match.group(1) == "a"
        name = match.group(2)
        emoji_id = int(match.group(3))
        if not any(e.id == emoji_id for e in bot.emojis):
            return None
        partial = discord.PartialEmoji(name=name, id=emoji_id, animated=animated)
        return partial, str(emoji_id), True

    # Treat anything else as unicode. Discord will reject bogus strings when we
    # try to react; here we just ensure it's not empty.
    if not raw:
        return None
    partial = discord.PartialEmoji(name=raw)
    return partial, raw, False


def emoji_key_from_payload(payload: discord.RawReactionActionEvent) -> tuple[str, bool]:
    if payload.emoji.is_custom_emoji():
        return str(payload.emoji.id), True
    return payload.emoji.name, False


def format_emoji(guild: discord.Guild, emoji: str, is_custom: bool) -> str:
    """Render a stored emoji for display."""
    if not is_custom:
        return emoji
    emoji_id = int(emoji)
    e = discord.utils.get(guild.emojis, id=emoji_id)
    if e is None:
        return f"<:unknown:{emoji_id}>"
    return str(e)


class ReactionRoles(commands.Cog, name="ReactionRoles"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    # ── Listeners ──

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.guild_id is None or payload.member is None or payload.member.bot:
            return
        emoji_key, is_custom = emoji_key_from_payload(payload)
        async with self.pool.acquire() as conn:
            row = await get_reaction_role(conn, payload.guild_id, payload.message_id, emoji_key)
        if row is None or row["is_custom"] != is_custom:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        role = guild.get_role(row["role_id"])
        if role is None:
            async with self.pool.acquire() as conn:
                await remove_reaction_role(conn, payload.guild_id, payload.message_id, emoji_key)
            return
        if not guild.me.guild_permissions.manage_roles or role >= guild.me.top_role:
            return
        if role in payload.member.roles:
            return
        try:
            await payload.member.add_roles(role, reason="Reaction role")
        except discord.Forbidden:
            pass

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        if payload.guild_id is None:
            return
        emoji_key, is_custom = emoji_key_from_payload(payload)
        async with self.pool.acquire() as conn:
            row = await get_reaction_role(conn, payload.guild_id, payload.message_id, emoji_key)
        if row is None or row["is_custom"] != is_custom:
            return

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        member = guild.get_member(payload.user_id)
        if member is None or member.bot:
            return
        role = guild.get_role(row["role_id"])
        if role is None:
            async with self.pool.acquire() as conn:
                await remove_reaction_role(conn, payload.guild_id, payload.message_id, emoji_key)
            return
        if not guild.me.guild_permissions.manage_roles or role >= guild.me.top_role:
            return
        if role not in member.roles:
            return
        try:
            await member.remove_roles(role, reason="Reaction role removed")
        except discord.Forbidden:
            pass

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        async with self.pool.acquire() as conn:
            await delete_reaction_roles_for_role(conn, role.guild.id, role.id)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.guild_id is None:
            return
        async with self.pool.acquire() as conn:
            await clear_reaction_roles_for_message(conn, payload.guild_id, payload.message_id)

    # ── Commands ──

    @commands.group(name="reactionroles", aliases=["reactionrole"], invoke_without_command=True)
    @commands.has_permissions(manage_roles=True)
    async def reactionroles(self, ctx: commands.Context):
        """Admin: manage reaction roles. Subcommands: add, remove, list, clear."""
        await ctx.send_help(ctx.command)

    @reactionroles.command(name="add")
    @commands.has_permissions(manage_roles=True)
    @commands.bot_has_permissions(manage_roles=True, add_reactions=True)
    async def rr_add(self, ctx: commands.Context, message: discord.Message, emoji: str, *, role: discord.Role):
        """Bind an emoji on a message to a role.
        Usage: .rr add <message_link_or_id> <emoji> <role>"""
        if message.guild is None or message.guild.id != ctx.guild.id:
            await ctx.send("That message isn't in this server.")
            return

        if role.is_default():
            await ctx.send("You can't bind @everyone.")
            return
        if role.managed:
            await ctx.send("You can't bind a managed/integration role.")
            return
        if role >= ctx.guild.me.top_role:
            await ctx.send("That role is above my highest role — I can't assign it.")
            return
        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("You can't bind a role equal to or above your own top role.")
            return

        parsed = parse_emoji(self.bot, ctx.guild, emoji)
        if parsed is None:
            await ctx.send("Invalid emoji, or custom emoji not available to me.")
            return
        partial, key, is_custom = parsed

        try:
            await message.add_reaction(partial)
        except discord.HTTPException:
            await ctx.send("I couldn't react on that message. Check my permissions or the emoji.")
            return

        async with self.pool.acquire() as conn:
            row = await add_reaction_role(
                conn, ctx.guild.id, message.channel.id, message.id, key, is_custom, role.id, ctx.author.id,
            )
        if row is None:
            await ctx.send("That emoji is already bound on this message. Remove the existing binding first.")
            return

        embed = discord.Embed(
            title="Reaction role bound",
            description=f"Reacting with {format_emoji(ctx.guild, key, is_custom)} on [this message]({message.jump_url}) grants {role.mention}.",
            color=discord.Color.fuchsia(),
        )
        await ctx.send(embed=embed)

    @reactionroles.command(name="remove", aliases=["rm", "delete"])
    @commands.has_permissions(manage_roles=True)
    async def rr_remove(self, ctx: commands.Context, message: discord.Message, emoji: str):
        """Remove a reaction role binding.
        Usage: .rr remove <message_link_or_id> <emoji>"""
        parsed = parse_emoji(self.bot, ctx.guild, emoji)
        if parsed is None:
            await ctx.send("Invalid emoji.")
            return
        partial, key, _is_custom = parsed

        async with self.pool.acquire() as conn:
            row = await remove_reaction_role(conn, ctx.guild.id, message.id, key)
        if row is None:
            await ctx.send("No binding found for that emoji on that message.")
            return

        try:
            await message.clear_reaction(partial)
        except (discord.Forbidden, discord.HTTPException):
            pass

        await ctx.send(f"Removed reaction role binding for {emoji}.")

    @reactionroles.command(name="list")
    @commands.has_permissions(manage_roles=True)
    async def rr_list(self, ctx: commands.Context, message: discord.Message = None):
        """List reaction roles for a message, or all bindings in this server."""
        async with self.pool.acquire() as conn:
            if message is None:
                rows = await list_reaction_roles_for_guild(conn, ctx.guild.id)
            else:
                rows = await list_reaction_roles_for_message(conn, ctx.guild.id, message.id)

        if not rows:
            await ctx.send("No reaction roles configured.")
            return

        embed = discord.Embed(
            title="Reaction Roles",
            color=discord.Color.fuchsia(),
        )
        # Group by message_id for readability
        by_message: dict[int, list] = {}
        for row in rows:
            by_message.setdefault(row["message_id"], []).append(row)

        for msg_id, entries in by_message.items():
            first = entries[0]
            channel = ctx.guild.get_channel(first["channel_id"])
            header = f"Message `{msg_id}`"
            if channel:
                header = f"{channel.mention} • `{msg_id}`"
            lines = []
            for row in entries:
                role = ctx.guild.get_role(row["role_id"])
                role_str = role.mention if role else f"*(deleted role {row['role_id']})*"
                lines.append(f"{format_emoji(ctx.guild, row['emoji'], row['is_custom'])} → {role_str}")
            embed.add_field(name=header, value="\n".join(lines), inline=False)

        await ctx.send(embed=embed)

    @reactionroles.command(name="clear")
    @commands.has_permissions(manage_roles=True)
    async def rr_clear(self, ctx: commands.Context, message: discord.Message):
        """Remove all reaction role bindings on a message."""
        async with self.pool.acquire() as conn:
            deleted = await clear_reaction_roles_for_message(conn, ctx.guild.id, message.id)

        if deleted == 0:
            await ctx.send("No bindings on that message.")
            return

        try:
            await message.clear_reactions()
        except (discord.Forbidden, discord.HTTPException):
            pass

        await ctx.send(f"Cleared {deleted} binding(s) on that message.")

    # ── Error handler ──

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.MissingPermissions):
            await ctx.send("You need Manage Roles permission to use this command.")
        elif isinstance(error, commands.BotMissingPermissions):
            await ctx.send("I'm missing a required permission (Manage Roles / Add Reactions).")
        elif isinstance(error, commands.MessageNotFound):
            await ctx.send("Couldn't find that message. Use a message link or ID from this channel.")
        elif isinstance(error, commands.RoleNotFound):
            await ctx.send("Couldn't find that role.")
        elif isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Missing argument. Check `.help {ctx.command.qualified_name}`.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `.help {ctx.command.qualified_name}`.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(ReactionRoles(bot))
