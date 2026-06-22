from __future__ import annotations

from discord.ext import commands

from core.errors import handle_command_error


class Errors(commands.Cog):
    """Global command-error handler: routes every command error through the one
    consistent policy in ``core.errors`` (usage hints, silent permission fails)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError) -> None:
        await handle_command_error(ctx, error)
