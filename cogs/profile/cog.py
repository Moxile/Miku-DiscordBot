from __future__ import annotations

from typing import Optional

import discord
from discord.ext import commands, tasks

from cogs.profile import service
from config import PROFILE_SNAPSHOT_MINUTES


class ProfileChartView(discord.ui.View):
    """Recent / 7d / 30d / 90d / All period toggles, plus a Wallet+Bank / Net Worth /
    Gambling graph-type row, for a profile's history chart."""

    def __init__(self, bot, guild: discord.Guild, member: discord.Member, *,
                 period: str | None = None, graph: str = "wallet", timeout: int = 180):
        super().__init__(timeout=timeout)
        self.bot = bot
        self.guild = guild
        self.member = member
        self.period = period
        self.graph = graph
        self.message: discord.Message | None = None
        self._sync_buttons()

    def _sync_buttons(self):
        self.recent_btn.disabled = self.period is None
        self.week_btn.disabled = self.period == "7d"
        self.month_btn.disabled = self.period == "30d"
        self.quarter_btn.disabled = self.period == "90d"
        self.all_btn.disabled = self.period == "all"
        self.wallet_btn.disabled = self.graph == "wallet"
        self.networth_btn.disabled = self.graph == "networth"
        self.gambling_btn.disabled = self.graph == "gambling"

    async def _refresh(self, interaction: discord.Interaction):
        self._sync_buttons()
        embed, file = await service.build_profile(self.bot, self.guild, self.member, self.period, self.graph)
        await interaction.response.edit_message(embed=embed, attachments=[file], view=self)

    @discord.ui.button(label="Recent", style=discord.ButtonStyle.secondary, row=0)
    async def recent_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.period = None
        await self._refresh(interaction)

    @discord.ui.button(label="7d", style=discord.ButtonStyle.secondary, row=0)
    async def week_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.period = "7d"
        await self._refresh(interaction)

    @discord.ui.button(label="30d", style=discord.ButtonStyle.secondary, row=0)
    async def month_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.period = "30d"
        await self._refresh(interaction)

    @discord.ui.button(label="90d", style=discord.ButtonStyle.secondary, row=0)
    async def quarter_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.period = "90d"
        await self._refresh(interaction)

    @discord.ui.button(label="All", style=discord.ButtonStyle.primary, row=0)
    async def all_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.period = "all"
        await self._refresh(interaction)

    @discord.ui.button(label="Wallet + Bank", style=discord.ButtonStyle.secondary, row=1)
    async def wallet_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.graph = "wallet"
        await self._refresh(interaction)

    @discord.ui.button(label="Net Worth", style=discord.ButtonStyle.secondary, row=1)
    async def networth_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.graph = "networth"
        await self._refresh(interaction)

    @discord.ui.button(label="Gambling", style=discord.ButtonStyle.secondary, row=1)
    async def gambling_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.graph = "gambling"
        await self._refresh(interaction)

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
        self.record_snapshots.start()

    def cog_unload(self):
        self.record_snapshots.cancel()

    @property
    def pool(self):
        return self.bot.pool

    @tasks.loop(minutes=PROFILE_SNAPSHOT_MINUTES)
    async def record_snapshots(self):
        """Periodically record every member's net worth for the "Net Worth" graph — see
        service.record_net_worth_snapshots for why this can't just be reconstructed on demand."""
        await service.record_net_worth_snapshots(self.pool)

    @record_snapshots.before_loop
    async def before_record_snapshots(self):
        await self.bot.wait_until_ready()

    @commands.command(name="profileinfo", aliases=["pinfo", "pi"], extras={"example": ".profileinfo @user 30d"})
    async def profileinfo(self, ctx: commands.Context, member: Optional[discord.Member] = None, period: str = None):
        """Show a player's economic profile: balances, gambling stats, holdings, and a
        history graph.

        `period` widens (or narrows) the graph's time window, e.g. `7d`, `30d`, `90d`, `all`.
        Without it, the graph shows recent activity. Use the buttons below the graph to
        switch the time window or the graph itself (Wallet+Bank / Net Worth / Gambling).
        Usage: .profileinfo [@user] [period]"""
        member = member or ctx.author
        embed, file = await service.build_profile(self.bot, ctx.guild, member, period)
        view = ProfileChartView(self.bot, ctx.guild, member, period=period.lower() if period else None)
        view.message = await ctx.send(embed=embed, file=file, view=view)
