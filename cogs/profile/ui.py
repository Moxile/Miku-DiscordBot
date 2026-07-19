from __future__ import annotations

"""Profile page for the Miku Menu: your profile card with a time-window picker,
plus a user picker to view someone else's."""

import discord

from cogs.profile import service
from core.ui import Page

PERIODS = [
    (None, "Recent"),
    ("7d", "7d"),
    ("30d", "30d"),
    ("90d", "90d"),
    ("all", "All"),
]


class ProfilePage(Page):
    def __init__(self, hub):
        super().__init__(hub)
        self.member = hub.session.user
        self.period: str | None = None

    async def build(self):
        embed, file = await service.build_profile(self.bot, self.guild, self.member, self.period)

        period_buttons = [
            self.button(label, self._make_pick(value), row=0, disabled=value == self.period)
            for value, label in PERIODS
        ]

        member_select = discord.ui.UserSelect(placeholder="View someone else…", row=1)
        member_select.callback = self._pick_member
        self._member_select = member_select

        items = [*period_buttons, member_select]
        if self.member.id != self.user.id:
            items.append(self.button("My profile", self._back_to_self, emoji="👤", row=2))
        return embed, items, [file]

    def _make_pick(self, value: str | None):
        async def _pick(interaction: discord.Interaction):
            self.period = value
            await self.hub.refresh(interaction)
        return _pick

    async def _pick_member(self, interaction: discord.Interaction):
        self.member = self._member_select.values[0]
        await self.hub.refresh(interaction)

    async def _back_to_self(self, interaction: discord.Interaction):
        self.member = self.user
        await self.hub.refresh(interaction)
