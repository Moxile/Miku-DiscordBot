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
    """Check that the invoker is the guild owner, the bot owner, or has the
    Administrator permission.

    Unlike commands.is_owner(), this does NOT pass for owner-role holders — it
    gates the commands that grant owner access in the first place. Administrator
    is allowed because it's already owner-equivalent control over the guild.
    """
    async def predicate(ctx) -> bool:
        if ctx.guild:
            if ctx.author.id == ctx.guild.owner_id:
                return True
            permissions = getattr(ctx.author, "guild_permissions", None)
            if permissions is not None and permissions.administrator:
                return True
        return await ctx.bot.is_bot_owner(ctx.author)
    return commands.check(predicate)


def has_permissions_or_owner(**perms):
    """Like commands.has_permissions, but also passes for the bot owner, the
    guild owner, anyone with Administrator, or an owner-role holder (see
    Bot.is_owner) regardless of whether they hold the listed permission(s).
    """
    async def predicate(ctx) -> bool:
        if await ctx.bot.is_owner(ctx.author):
            return True
        permissions = ctx.author.guild_permissions
        missing = [perm for perm, value in perms.items() if getattr(permissions, perm) != value]
        if missing:
            raise commands.MissingPermissions(missing)
        return True
    return commands.check(predicate)


async def get_required_channel(pool, guild_id: int, setting_key: str):
    """The channel id configured for setting_key, or None if unrestricted.
    Shares the same cache as require_channel."""
    channel_id = _channel_cache.get((guild_id, setting_key))
    if channel_id is None:
        row = await pool.fetchrow(
            "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = $2",
            guild_id, setting_key,
        )
        if row is None:
            return None  # Not configured — allowed everywhere
        channel_id = int(row["value"])
        _channel_cache[(guild_id, setting_key)] = channel_id
    return channel_id


def require_channel(setting_key: str):
    """Check that the command is used in the channel configured for setting_key.
    If no channel has been set, the command is allowed everywhere.
    """
    async def predicate(ctx) -> bool:
        channel_id = await get_required_channel(ctx.bot.pool, ctx.guild.id, setting_key)
        if channel_id is None or ctx.channel.id == channel_id:
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
