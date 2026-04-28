from __future__ import annotations
import re
import discord
from discord.ext import commands

from cogs.reaction_roles.db import (
    add_reaction_role,
    get_reaction_role,
    remove_reaction_role,
    list_reaction_roles_for_message,
    list_reaction_roles_for_guild,
    clear_reaction_roles_for_message,
    delete_reaction_roles_for_role,
    set_default_role,
    get_default_role,
    remove_default_role,
    delete_default_role_for_role,
    clear_default_role_for_message,
    get_defaults_for_guild,
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

    async def _member_has_mapped_reaction(
        self, payload: discord.RawReactionActionEvent, mapped_keys: set[str]
    ) -> bool:
        """Return True if the user still has at least one mapped reaction on the message."""
        channel = self.bot.get_channel(payload.channel_id)
        if not isinstance(channel, discord.TextChannel):
            return False
        try:
            message = await channel.fetch_message(payload.message_id)
        except discord.NotFound:
            return False
        for reaction in message.reactions:
            emoji = reaction.emoji
            if isinstance(emoji, str):
                key = emoji
            elif emoji.id:
                key = str(emoji.id)
            else:
                key = emoji.name
            if key not in mapped_keys:
                continue
            async for user in reaction.users():
                if user.id == payload.user_id:
                    return True
        return False

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
        if role not in payload.member.roles:
            try:
                await payload.member.add_roles(role, reason="Reaction role")
            except discord.Forbidden:
                pass

        # Remove default role now that the member has a mapped reaction
        async with self.pool.acquire() as conn:
            default_row = await get_default_role(conn, payload.guild_id, payload.message_id)
        if default_row is None:
            return
        default_role = guild.get_role(default_row["role_id"])
        if default_role is None:
            async with self.pool.acquire() as conn:
                await remove_default_role(conn, payload.guild_id, payload.message_id)
            return
        if not guild.me.guild_permissions.manage_roles or default_role >= guild.me.top_role:
            return
        if default_role not in payload.member.roles:
            return
        try:
            await payload.member.remove_roles(default_role, reason="Reaction role added — removing default role")
        except discord.Forbidden:
            pass

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        if payload.guild_id is None:
            return
        emoji_key, is_custom = emoji_key_from_payload(payload)
        async with self.pool.acquire() as conn:
            row = await get_reaction_role(conn, payload.guild_id, payload.message_id, emoji_key)
            default_row = await get_default_role(conn, payload.guild_id, payload.message_id)

        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return
        member = guild.get_member(payload.user_id)
        if member is None or member.bot:
            return

        # Remove the reaction role for this emoji
        if row is not None and row["is_custom"] == is_custom:
            role = guild.get_role(row["role_id"])
            if role is None:
                async with self.pool.acquire() as conn:
                    await remove_reaction_role(conn, payload.guild_id, payload.message_id, emoji_key)
            elif guild.me.guild_permissions.manage_roles and role < guild.me.top_role and role in member.roles:
                try:
                    await member.remove_roles(role, reason="Reaction role removed")
                except discord.Forbidden:
                    pass

        # Restore default role if the member has no more mapped reactions
        if default_row is None:
            return
        default_role = guild.get_role(default_row["role_id"])
        if default_role is None:
            async with self.pool.acquire() as conn:
                await remove_default_role(conn, payload.guild_id, payload.message_id)
            return
        if not guild.me.guild_permissions.manage_roles or default_role >= guild.me.top_role:
            return
        if default_role in member.roles:
            return

        async with self.pool.acquire() as conn:
            all_rows = await list_reaction_roles_for_message(conn, payload.guild_id, payload.message_id)
        mapped_keys = {r["emoji"] for r in all_rows}
        if mapped_keys and await self._member_has_mapped_reaction(payload, mapped_keys):
            return

        try:
            await member.add_roles(default_role, reason="No more reactions — default role restored")
        except discord.Forbidden:
            pass

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot:
            return
        async with self.pool.acquire() as conn:
            default_rows = await get_defaults_for_guild(conn, member.guild.id)
        if not default_rows:
            return
        guild = member.guild
        if not guild.me.guild_permissions.manage_roles:
            return
        for row in default_rows:
            role = guild.get_role(row["role_id"])
            if role is None or role >= guild.me.top_role:
                continue
            try:
                await member.add_roles(role, reason="Default reaction role on join")
            except discord.Forbidden:
                pass

    @commands.Cog.listener()
    async def on_guild_role_delete(self, role: discord.Role):
        async with self.pool.acquire() as conn:
            await delete_reaction_roles_for_role(conn, role.guild.id, role.id)
            await delete_default_role_for_role(conn, role.guild.id, role.id)

    @commands.Cog.listener()
    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        if payload.guild_id is None:
            return
        async with self.pool.acquire() as conn:
            await clear_reaction_roles_for_message(conn, payload.guild_id, payload.message_id)
            await clear_default_role_for_message(conn, payload.guild_id, payload.message_id)

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
                default_rows = await get_defaults_for_guild(conn, ctx.guild.id)
            else:
                rows = await list_reaction_roles_for_message(conn, ctx.guild.id, message.id)
                default_row = await get_default_role(conn, ctx.guild.id, message.id)
                default_rows = [default_row] if default_row else []

        defaults_by_msg: dict[int, int] = {r["message_id"]: r["role_id"] for r in default_rows}

        # Collect all message IDs (union of reaction roles and defaults)
        msg_ids_with_channel: dict[int, int] = {}
        by_message: dict[int, list] = {}
        for row in rows:
            by_message.setdefault(row["message_id"], []).append(row)
            msg_ids_with_channel[row["message_id"]] = row["channel_id"]
        for row in default_rows:
            msg_ids_with_channel.setdefault(row["message_id"], row["channel_id"])

        if not msg_ids_with_channel:
            await ctx.send("No reaction roles configured.")
            return

        embed = discord.Embed(title="Reaction Roles", color=discord.Color.fuchsia())

        for msg_id, channel_id in msg_ids_with_channel.items():
            channel = ctx.guild.get_channel(channel_id)
            header = f"{channel.mention} • `{msg_id}`" if channel else f"Message `{msg_id}`"
            lines = []
            for row in by_message.get(msg_id, []):
                role = ctx.guild.get_role(row["role_id"])
                role_str = role.mention if role else f"*(deleted role {row['role_id']})*"
                lines.append(f"{format_emoji(ctx.guild, row['emoji'], row['is_custom'])} → {role_str}")
            if msg_id in defaults_by_msg:
                default_role = ctx.guild.get_role(defaults_by_msg[msg_id])
                default_str = default_role.mention if default_role else f"*(deleted role {defaults_by_msg[msg_id]})*"
                lines.append(f"**Default (no reactions):** {default_str}")
            embed.add_field(name=header, value="\n".join(lines), inline=False)

        await ctx.send(embed=embed)

    @reactionroles.command(name="clear")
    @commands.has_permissions(manage_roles=True)
    async def rr_clear(self, ctx: commands.Context, message: discord.Message):
        """Remove all reaction role bindings (including default) on a message."""
        async with self.pool.acquire() as conn:
            deleted = await clear_reaction_roles_for_message(conn, ctx.guild.id, message.id)
            await clear_default_role_for_message(conn, ctx.guild.id, message.id)

        if deleted == 0:
            await ctx.send("No bindings on that message.")
            return

        try:
            await message.clear_reactions()
        except (discord.Forbidden, discord.HTTPException):
            pass

        await ctx.send(f"Cleared {deleted} binding(s) on that message.")

    @reactionroles.command(name="default")
    @commands.has_permissions(manage_roles=True)
    @commands.bot_has_permissions(manage_roles=True)
    async def rr_default(self, ctx: commands.Context, message: discord.Message, *, role: discord.Role):
        """Set a default role for a message. Members with no reactions receive it; reacting removes it.
        Usage: .reactionroles default <message> <role>"""
        if message.guild is None or message.guild.id != ctx.guild.id:
            await ctx.send("That message isn't in this server.")
            return
        if role.is_default():
            await ctx.send("You can't use @everyone as a default role.")
            return
        if role.managed:
            await ctx.send("You can't use a managed/integration role.")
            return
        if role >= ctx.guild.me.top_role:
            await ctx.send("That role is above my highest role — I can't assign it.")
            return
        if role >= ctx.author.top_role and ctx.author != ctx.guild.owner:
            await ctx.send("You can't set a role equal to or above your own top role.")
            return

        async with self.pool.acquire() as conn:
            await set_default_role(conn, ctx.guild.id, message.channel.id, message.id, role.id, ctx.author.id)

        embed = discord.Embed(
            title="Default role set",
            description=(
                f"{role.mention} will be given to members who have no reactions on "
                f"[this message]({message.jump_url}), and removed when they react."
            ),
            color=discord.Color.fuchsia(),
        )
        await ctx.send(embed=embed)

    @reactionroles.command(name="defaultremove", aliases=["rmdefault", "removedefault"])
    @commands.has_permissions(manage_roles=True)
    async def rr_defaultremove(self, ctx: commands.Context, message: discord.Message):
        """Remove the default role binding for a message.
        Usage: .reactionroles defaultremove <message>"""
        async with self.pool.acquire() as conn:
            row = await remove_default_role(conn, ctx.guild.id, message.id)
        if row is None:
            await ctx.send("No default role is set for that message.")
            return
        await ctx.send("Default role removed.")

    @reactionroles.command(name="defaultsync")
    @commands.has_permissions(manage_roles=True)
    @commands.bot_has_permissions(manage_roles=True)
    async def rr_defaultsync(self, ctx: commands.Context, message: discord.Message):
        """Assign the default role to all members who have no mapped reactions on this message.
        Usage: .reactionroles defaultsync <message>"""
        async with self.pool.acquire() as conn:
            default_row = await get_default_role(conn, ctx.guild.id, message.id)
            all_rows = await list_reaction_roles_for_message(conn, ctx.guild.id, message.id)

        if default_row is None:
            await ctx.send("No default role is set for that message. Use `.reactionroles default` first.")
            return
        default_role = ctx.guild.get_role(default_row["role_id"])
        if default_role is None:
            await ctx.send("The default role no longer exists.")
            return
        if not ctx.guild.me.guild_permissions.manage_roles or default_role >= ctx.guild.me.top_role:
            await ctx.send("I can't assign that role — it's at or above my highest role.")
            return

        # Re-fetch the message so reactions reflect current state, not the stale cache
        try:
            message = await message.channel.fetch_message(message.id)
        except discord.NotFound:
            await ctx.send("Couldn't fetch that message.")
            return

        # Build the set of user IDs who have reacted to any mapped emoji
        mapped_keys = {r["emoji"] for r in all_rows}
        reacted_ids: set[int] = set()
        for reaction in message.reactions:
            emoji = reaction.emoji
            if isinstance(emoji, str):
                key = emoji
            elif emoji.id:
                key = str(emoji.id)
            else:
                key = emoji.name
            if mapped_keys and key not in mapped_keys:
                continue
            async for user in reaction.users():
                reacted_ids.add(user.id)

        status = await ctx.send("Syncing default role to eligible members...")

        # Ensure the member list is fully loaded before iterating
        if not ctx.guild.chunked:
            await ctx.guild.chunk()

        count = 0
        for member in ctx.guild.members:
            if member.bot or member.id in reacted_ids or default_role in member.roles:
                continue
            try:
                await member.add_roles(default_role, reason="Default role sync")
                count += 1
            except discord.Forbidden:
                pass

        await status.edit(content=f"Done. Assigned {default_role.mention} to **{count}** member(s).")

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
