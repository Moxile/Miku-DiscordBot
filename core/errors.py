"""Central command-error policy: one consistent usage hint for every command.

Any wrong use of any command (missing/invalid arguments) replies with the same
shape — a **Layout** line (the signature) plus a concrete **Example**. Examples
are auto-generated from each parameter's name/annotation, and a command can
override its example via ``@commands.command(..., extras={"example": ".cmd …"})``.

Permission/owner-check failures are silently ignored, so a member misusing an
admin-only command (e.g. ``.mute``) gets no response at all.
"""

from __future__ import annotations

import discord
from discord.ext import commands

from core.checks import UserLocked, WrongChannel

# Map a parameter *name* to a realistic example value. Matched case-insensitively;
# the first key that is a substring of the parameter name wins (order matters, so
# more specific keys come first).
_NAME_HINTS: list[tuple[tuple[str, ...], str]] = [
    (("lichess", "url", "link"), "https://lichess.org/7o8NKKnL"),
    (("hex", "colour", "color"), "#ff0066"),
    (("duration", "time", "interval", "expiry", "expires"), "10m"),
    (("percent", "pct", "rate"), "50"),
    (("member", "user", "target", "recipient", "winner", "opponent"), "@user"),
    (("role",), "@role"),
    (("channel", "stock"), "#stock"),
    (("reason",), "spamming"),
    (("page",), "2"),
    (("emoji",), "🎉"),
    (("message", "msg", "text", "content", "note", "description"), "hello"),
    (("amount", "bet", "price", "cost", "wager", "bid", "ask", "quantity",
      "qty", "shares", "value"), "100"),
    (("id", "number", "num", "count", "n"), "5"),
    (("name", "title", "item", "query", "q", "username", "waifu"), "miku"),
]

# Friendly one-line reason for common argument errors, prepended to the hint.
_REASONS = {
    commands.MemberNotFound: "Couldn't find that member — try mentioning them.",
    commands.UserNotFound: "Couldn't find that user — try mentioning them.",
    commands.RoleNotFound: "Couldn't find that role.",
    commands.ChannelNotFound: "Couldn't find that channel.",
    commands.MessageNotFound: "Couldn't find that message — use a link or ID.",
    commands.EmojiNotFound: "Couldn't find that emoji.",
    commands.TooManyArguments: "Too many arguments.",
}


def _annotation_token(annotation, types) -> str | None:
    """First matching value in ``types`` (an iterable of (type, value)) for the
    annotation, or None."""
    for typ, value in types:
        try:
            if annotation is typ or (isinstance(annotation, type) and issubclass(annotation, typ)):
                return value
        except TypeError:
            continue
    return None


# Discord-object annotations always win over the parameter name — a Member is
# best shown as @user no matter what the parameter is called.
_DISCORD_TYPES = [
    (discord.Member, "@user"),
    (discord.User, "@user"),
    (discord.Role, "@role"),
    (discord.TextChannel, "#channel"),
    (discord.abc.GuildChannel, "#channel"),
]
_SCALAR_TYPES = [(int, "5"), (float, "1.5")]


def _placeholder(param: commands.Parameter) -> str:
    """A realistic example token for one parameter."""
    token = _annotation_token(param.annotation, _DISCORD_TYPES)
    if token:
        return token
    name = param.name.lower()
    for keys, value in _NAME_HINTS:
        if any(k in name for k in keys):
            return value
    token = _annotation_token(param.annotation, _SCALAR_TYPES)
    if token:
        return token
    return param.name


def signature_line(ctx: commands.Context, command: commands.Command) -> str:
    """The layout, e.g. ``.transfer <member> <amount>``."""
    sig = command.signature
    base = f"{ctx.clean_prefix}{command.qualified_name}"
    return f"{base} {sig}".rstrip()


def build_example(ctx: commands.Context, command: commands.Command) -> str:
    """A copy-pasteable example, honouring an ``extras['example']`` override."""
    override = command.extras.get("example")
    if override:
        return override
    parts = [f"{ctx.clean_prefix}{command.qualified_name}"]
    for param in command.clean_params.values():
        if param.required:
            parts.append(_placeholder(param))
        else:
            # Only include optionals when we have a meaningful token for them.
            token = _placeholder(param)
            if token != param.name:
                parts.append(token)
    return " ".join(parts)


def usage_hint_embed(ctx: commands.Context, error: commands.CommandError) -> discord.Embed:
    """Build the consistent ⚠️ usage hint for an input error."""
    command = ctx.command
    embed = discord.Embed(
        title=f"⚠️ Usage: {ctx.clean_prefix}{command.qualified_name}",
        color=discord.Color.orange(),
    )
    reason = None
    if isinstance(error, commands.MissingRequiredArgument):
        reason = f"You're missing the `{error.param.name}` argument."
    elif isinstance(error, commands.BadUnionArgument):
        reason = f"Couldn't understand the `{error.param.name}` argument."
    else:
        for typ, text in _REASONS.items():
            if isinstance(error, typ):
                reason = text
                break
        if reason is None and isinstance(error, commands.BadArgument):
            reason = "One of the arguments was invalid."
    if reason:
        embed.description = reason
    embed.add_field(name="Layout", value=f"`{signature_line(ctx, command)}`", inline=False)
    embed.add_field(name="Example", value=f"`{build_example(ctx, command)}`", inline=False)
    if command.help:
        embed.set_footer(text=f"Use {ctx.clean_prefix}help {command.qualified_name} for full details.")
    return embed


async def handle_command_error(ctx: commands.Context, error: commands.CommandError) -> None:
    """The single error policy for all commands (see module docstring)."""
    # Commands that don't exist / are disabled: stay quiet.
    if isinstance(error, (commands.CommandNotFound, commands.DisabledCommand)):
        return

    # Locked users are ignored silently (a moderation lock — preserve old behavior).
    if isinstance(error, UserLocked):
        return
    # Wrong-channel is a helpful, user-facing failure — keep its message.
    if isinstance(error, WrongChannel):
        await ctx.send(str(error), delete_after=15)
        return

    # The bot itself lacks a Discord permission — tell the user what's missing.
    if isinstance(error, commands.BotMissingPermissions):
        perms = ", ".join(p.replace("_", " ").title() for p in error.missing_permissions)
        await ctx.send(f"I'm missing the **{perms}** permission to do that.")
        return

    # Any other permission/owner/check failure is silently ignored, so admin-only
    # commands don't leak their existence to members who can't use them.
    if isinstance(error, commands.CheckFailure):
        return

    # Wrong arguments → the consistent usage hint.
    if isinstance(error, commands.UserInputError):
        if ctx.command is None:
            return
        await ctx.send(embed=usage_hint_embed(ctx, error))
        return

    # Runtime failure inside a command: surface a brief note and re-raise so the
    # traceback still reaches the logs (preserves the old Market behaviour).
    if isinstance(error, commands.CommandInvokeError):
        await ctx.send(f"Something went wrong running that command: `{error.original}`")
        raise error.original

    raise error
