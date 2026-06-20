from discord.ext import commands


class WrongChannel(commands.CheckFailure):
    """Raised when a command is used outside its configured channel."""
    pass


class UserLocked(commands.CheckFailure):
    """Raised when a locked user attempts an economy command."""
    pass


# (guild_id, setting_key) -> channel_id
_channel_cache: dict[tuple[int, str], int] = {}

# (guild_id, user_id) -> is_locked
_lock_cache: dict[tuple[int, int], bool] = {}


def guild_or_bot_owner():
    """Check that the invoker is the guild owner or the bot owner.

    Unlike commands.is_owner(), this does NOT pass for owner-role holders — it
    gates the commands that grant owner access in the first place.
    """
    async def predicate(ctx) -> bool:
        if ctx.guild and ctx.author.id == ctx.guild.owner_id:
            return True
        return await ctx.bot.is_bot_owner(ctx.author)
    return commands.check(predicate)


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


def require_not_locked():
    """Check that the invoking user is not economy-locked. Raises UserLocked if they are."""
    async def predicate(ctx) -> bool:
        if not ctx.guild:
            return True
        key = (ctx.guild.id, ctx.author.id)
        locked = _lock_cache.get(key)
        if locked is None:
            locked = await ctx.bot.pool.fetchval(
                "SELECT EXISTS(SELECT 1 FROM locked_users WHERE guild_id = $1 AND user_id = $2)",
                ctx.guild.id, ctx.author.id,
            )
            _lock_cache[key] = locked
        if locked:
            raise UserLocked()
        return True
    return commands.check(predicate)


async def user_is_locked(pool, guild_id: int, user_id: int) -> bool:
    """Return True if the user is economy-locked. Shares the same cache as require_not_locked."""
    key = (guild_id, user_id)
    locked = _lock_cache.get(key)
    if locked is None:
        locked = await pool.fetchval(
            "SELECT EXISTS(SELECT 1 FROM locked_users WHERE guild_id = $1 AND user_id = $2)",
            guild_id, user_id,
        )
        _lock_cache[key] = locked
    return locked


def invalidate_lock(guild_id: int, user_id: int) -> None:
    """Clear the lock cache for a user (call after locking or unlocking)."""
    _lock_cache.pop((guild_id, user_id), None)
