from __future__ import annotations

from typing import Optional

import discord
from discord.ext import commands

from cogs.profile import service


class ProfileChartView(discord.ui.View):
    """Recent / 7d / 30d / 90d / All toggles for a profile's wallet+bank chart."""

    def __init__(self, bot, guild: discord.Guild, member: discord.Member, *,
                 current: str | None = None, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.bot = bot
        self.guild = guild
        self.member = member
        self.current = current
        self.message: discord.Message | None = None
        self._sync_buttons()

    def _sync_buttons(self):
        self.recent_btn.disabled = self.current is None
        self.week_btn.disabled = self.current == "7d"
        self.month_btn.disabled = self.current == "30d"
        self.quarter_btn.disabled = self.current == "90d"
        self.all_btn.disabled = self.current == "all"

    async def _switch(self, interaction: discord.Interaction, period: str | None):
        self.current = period
        self._sync_buttons()
        embed, file = await service.build_profile(self.bot, self.guild, self.member, period)
        await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

    @discord.ui.button(label="Recent", style=discord.ButtonStyle.secondary)
    async def recent_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, None)

    @discord.ui.button(label="7d", style=discord.ButtonStyle.secondary)
    async def week_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "7d")

    @discord.ui.button(label="30d", style=discord.ButtonStyle.secondary)
    async def month_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "30d")

    @discord.ui.button(label="90d", style=discord.ButtonStyle.secondary)
    async def quarter_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "90d")

    @discord.ui.button(label="All", style=discord.ButtonStyle.primary)
    async def all_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._switch(interaction, "all")

    async def on_timeout(self):
        for child in self.children:
            child.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


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
        Without it, the graph shows the last 60 balance-changing transactions. Use the buttons
        below the graph to switch it afterwards.
        Usage: .profileinfo [@user] [period]"""
        member = member or ctx.author
        embed, file = await service.build_profile(self.bot, ctx.guild, member, period)
        view = ProfileChartView(self.bot, ctx.guild, member, current=period.lower() if period else None)
        view.message = await ctx.send(embed=embed, file=file, view=view)
