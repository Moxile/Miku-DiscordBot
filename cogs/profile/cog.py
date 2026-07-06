from __future__ import annotations

from typing import Optional

import discord
from discord.ext import commands

from cogs.profile import service


class Profile(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    @commands.command(name="profileinfo", aliases=["pinfo", "pi"], extras={"example": ".profileinfo @user 30d"})
    async def profileinfo(self, ctx: commands.Context, member: Optional[discord.Member] = None, period: str = None):
        """Show a player's economic profile: balances, gambling stats, holdings, and a
        wallet+bank history graph.

        `period` widens (or narrows) the graph's time window, e.g. `7d`, `30d`, `90d`, `all`.
        Without it, the graph shows the last 60 balance-changing transactions.
        Usage: .profileinfo [@user] [period]"""
        member = member or ctx.author
        embed, file = await service.build_profile(self.bot, ctx.guild, member, period)
        await ctx.send(embed=embed, file=file)
