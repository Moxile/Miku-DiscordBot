from discord.ext import commands


class WrongChannel(commands.CheckFailure):
    """Raised when a command is used outside its configured channel."""
    pass


# (guild_id, setting_key) -> channel_id
_channel_cache: dict[tuple[int, str], int] = {}


def require_channel(setting_key: str):
    """Check that the command is used in the channel configured for setting_key.
    If no channel has been set, the command is allowed everywhere.
    """
    async def predicate(ctx) -> bool:
        channel_id = _channel_cache.get((ctx.guild.id, setting_key))
        if channel_id is None:
            row = await ctx.bot.pool.fetchrow(
                "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = $2",
                ctx.guild.id, setting_key,
            )
            if row:
                channel_id = int(row["value"])
                _channel_cache[(ctx.guild.id, setting_key)] = channel_id
            else:
                return True  # Not configured — allow everywhere
        if ctx.channel.id == channel_id:
            return True
        channel = ctx.guild.get_channel(channel_id)
        mention = channel.mention if channel else f"<#{channel_id}>"
        raise WrongChannel(f"This command can only be used in {mention}.")
    return commands.check(predicate)


def invalidate(guild_id: int, setting_key: str) -> None:
    """Remove a cached channel entry (call after updating guild_settings)."""
    _channel_cache.pop((guild_id, setting_key), None)
