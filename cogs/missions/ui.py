from __future__ import annotations

"""Missions page for the Miku Menu: progress bars, pagination, and a fund flow
(pick a mission from the select, enter the amount in the popup)."""

import math

import discord

from cogs.missions import service
from cogs.missions.cog import MISSIONS_PER_PAGE, _mission_field
from cogs.missions.db import get_missions
from core.ui import AmountModal, Page


class MissionsPage(Page):
    def __init__(self, hub):
        super().__init__(hub)
        self.page = 0
        self._max_page = 0

    async def build(self):
        missions = list(await get_missions(self.pool, self.guild.id))
        self._max_page = max(0, math.ceil(len(missions) / MISSIONS_PER_PAGE) - 1)
        self.page = min(self.page, self._max_page)
        cur = self.currency

        embed = discord.Embed(title="🎯 Active Missions", color=discord.Color.from_rgb(255, 140, 0))
        if not missions:
            embed.description = "No active missions right now."
            return embed, []

        start = self.page * MISSIONS_PER_PAGE
        for m in missions[start:start + MISSIONS_PER_PAGE]:
            name, value = _mission_field(m, cur.emoji)
            embed.add_field(name=name, value=value, inline=False)
        embed.set_footer(text=f"Page {self.page + 1}/{self._max_page + 1} — {len(missions)} mission(s)")

        select = discord.ui.Select(
            placeholder="Fund a mission…",
            options=[
                discord.SelectOption(
                    label=f"#{m['id']} {m['name']}"[:100],
                    value=str(m["id"]),
                    description=f"{m['funded']:,} / {m['goal']:,} funded"[:100],
                )
                for m in missions[:25]
            ],
            row=0,
        )
        select.callback = self._pick_mission
        self._select = select

        items = [
            select,
            self.button("◀ Prev", self._prev, row=1, disabled=self.page == 0),
            self.button("Next ▶", self._next, row=1, disabled=self.page >= self._max_page),
        ]
        return embed, items

    async def _pick_mission(self, interaction: discord.Interaction):
        mission_id = int(self._select.values[0])

        async def _do_fund(modal_interaction: discord.Interaction, raw: str):
            result = await service.fund_mission(
                self.pool, self.guild.id, self.user.id, raw,
                self.session.channel_id, mission_id=mission_id,
            )
            cur = self.currency
            if result.completed:
                notice = (f"🎉 You contributed **{result.amount:,}**{cur.emoji} — "
                          f"**{result.mission['name']} is fully funded!**")
            else:
                remaining = max(result.mission["goal"] - result.mission["funded"], 0)
                notice = (f"🎯 You contributed **{result.amount:,}**{cur.emoji} to "
                          f"**{result.mission['name']}** — {remaining:,}{cur.emoji} to go.")
            await self.hub.refresh(modal_interaction, notice=notice)

        await interaction.response.send_modal(
            AmountModal(self.hub, title="Fund mission", handler=_do_fund))

    async def _prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        await self.hub.refresh(interaction)

    async def _next(self, interaction: discord.Interaction):
        self.page = min(self._max_page, self.page + 1)
        await self.hub.refresh(interaction)
