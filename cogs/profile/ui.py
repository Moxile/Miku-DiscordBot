from __future__ import annotations

"""Profile page for the Miku Menu: your profile card with a time-window picker,
plus a user picker to view someone else's."""

import discord

from cogs.profile import service
from core.ui import Page

PERIODS = [
    (None, "Recent activity"),
    ("7d", "Past 7 days"),
    ("30d", "Past 30 days"),
    ("90d", "Past 90 days"),
    ("all", "All time"),
]


class ProfilePage(Page):
    def __init__(self, hub):
        super().__init__(hub)
        self.member = hub.session.user
        self.period: str | None = None

    async def build(self):
        embed, file = await service.build_profile(self.bot, self.guild, self.member, self.period)

        period_select = discord.ui.Select(
            placeholder="Graph window…",
            options=[
                discord.SelectOption(label=label, value=value or "recent",
                                     default=value == self.period)
                for value, label in PERIODS
            ],
            row=0,
        )
        period_select.callback = self._pick_period
        self._period_select = period_select

        member_select = discord.ui.UserSelect(placeholder="View someone else…", row=1)
        member_select.callback = self._pick_member
        self._member_select = member_select

        items = [period_select, member_select]
        if self.member.id != self.user.id:
            items.append(self.button("My profile", self._back_to_self, emoji="👤", row=2))
        return embed, items, [file]

    async def _pick_period(self, interaction: discord.Interaction):
        picked = self._period_select.values[0]
        self.period = None if picked == "recent" else picked
        await self.hub.refresh(interaction)

    async def _pick_member(self, interaction: discord.Interaction):
        self.member = self._member_select.values[0]
        await self.hub.refresh(interaction)

    async def _back_to_self(self, interaction: discord.Interaction):
        self.member = self.user
        await self.hub.refresh(interaction)
