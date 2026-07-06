from __future__ import annotations

"""The Miku Menu home screen: one button per registered feature page."""

import discord

from core.ui.hub import Page
from core.ui.registry import PageEntry, page_entries

MAX_ENTRIES = 20  # rows 0–3 × 5 buttons; row 4 is the nav bar


class HomePage(Page):
    async def build(self):
        visible: list[PageEntry] = []
        for entry in page_entries():
            if entry.cog_name and await self.bot._is_cog_disabled(self.guild.id, entry.cog_name):
                continue
            if entry.owner_only and not await self.bot.is_owner(self.user):
                continue
            visible.append(entry)
        visible = visible[:MAX_ENTRIES]

        embed = discord.Embed(title="🎀 Miku Menu", color=discord.Color.pink())
        if visible:
            embed.description = "\n".join(
                f"{e.emoji} **{e.label}** — {e.description}" for e in visible
            )
        else:
            embed.description = "Nothing here yet."
        embed.set_footer(text="Only you can see this menu.")

        items = [
            self.button(entry.label, self._opener(entry), emoji=entry.emoji,
                        style=discord.ButtonStyle.primary, row=i // 5)
            for i, entry in enumerate(visible)
        ]
        return embed, items

    def _opener(self, entry: PageEntry):
        async def _open(interaction: discord.Interaction):
            await self.hub.push(interaction, entry.factory(self.hub))
        return _open
