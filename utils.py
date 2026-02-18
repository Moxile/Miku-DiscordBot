from discord.ext import commands

PREFIX = "."
VALID_CATEGORIES = {"economy", "gambling", "shop", "market", "missions", "waifu"}


def is_guild_owner():
    """Check that the command invoker is the server owner."""
    async def predicate(ctx: commands.Context):
        return ctx.author == ctx.guild.owner
    return commands.check(predicate)


async def check_channel_allowed(db, guild_id: int, category: str, channel_id: int,
                                command_name: str = None) -> bool:
    """Check if a command category is allowed in a channel.
    Returns True if no restrictions are configured, the channel is in the allow list,
    or the specific command has been unrestricted."""
    # Check if this specific command is unrestricted
    if command_name:
        async with db.execute(
            "SELECT 1 FROM unrestricted_commands WHERE guild_id = ? AND command = ?",
            (guild_id, command_name),
        ) as cur:
            if await cur.fetchone():
                return True
    # Check category restrictions
    async with db.execute(
        "SELECT 1 FROM allowed_channels WHERE guild_id = ? AND category = ? LIMIT 1",
        (guild_id, category),
    ) as cur:
        has_restrictions = await cur.fetchone()
    if not has_restrictions:
        return True  # No restrictions configured for this category
    async with db.execute(
        "SELECT 1 FROM allowed_channels WHERE guild_id = ? AND category = ? AND channel_id = ?",
        (guild_id, category, channel_id),
    ) as cur:
        allowed = await cur.fetchone()
    if not allowed:
        raise commands.CheckFailure("channel_restricted")
    return True
