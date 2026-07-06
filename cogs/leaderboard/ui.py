from __future__ import annotations

"""Leaderboard page for the Miku Menu: a mode picker plus pagination."""

import discord

from cogs.leaderboard import db, service
from core.ui import Page


class LeaderboardPage(Page):
    def __init__(self, hub):
        super().__init__(hub)
        self.mode = "net"
        self.page = 0
        self._max_page = 0

    async def build(self):
        title, rows, score_label = await service.get_leaderboard(
            self.pool, self.guild.id, self.mode)
        self._max_page = service.max_page(rows)
        self.page = min(self.page, self._max_page)
        embed = service.build_lb_embed(
            self.guild, title, rows, self.page, self.user.id,
            score_label or self.currency.emoji)

        modes = dict(service.MODE_LABELS)
        # No tracked emoji → no reaction board; hide the option instead of erroring.
        if await db.get_reaction_config(self.pool, self.guild.id) is None:
            modes.pop("emoji", None)

        select = discord.ui.Select(
            placeholder="Leaderboard…",
            options=[
                discord.SelectOption(label=label, value=key, default=key == self.mode)
                for key, label in modes.items()
            ],
            row=0,
        )
        select.callback = self._pick_mode
        self._select = select

        items = [
            select,
            self.button("◀ Prev", self._prev, row=1, disabled=self.page == 0),
            self.button("Next ▶", self._next, row=1, disabled=self.page >= self._max_page),
        ]
        return embed, items

    async def _pick_mode(self, interaction: discord.Interaction):
        self.mode = self._select.values[0]
        self.page = 0
        await self.hub.refresh(interaction)

    async def _prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        await self.hub.refresh(interaction)

    async def _next(self, interaction: discord.Interaction):
        self.page = min(self._max_page, self.page + 1)
        await self.hub.refresh(interaction)
