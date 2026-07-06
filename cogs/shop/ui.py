from __future__ import annotations

"""Shop pages for the Miku Menu: browse items, buy from a picker, and view
your inventory."""

import math

import discord

from cogs.shop import service
from cogs.shop.db import get_inventory, get_item_by_id, get_shop_items
from core.checks import user_is_locked
from core.errors import UserError
from core.names import format_name
from core.time_utils import humanize_duration
from core.ui import Page

SHOP_COLOR = discord.Color.from_rgb(57, 197, 187)  # Miku teal
PER_PAGE = 6


def _is_role_item(item) -> bool:
    return item["item_type"] == "role" and bool(item["role_given"])


class ShopPage(Page):
    def __init__(self, hub):
        super().__init__(hub)
        self.page = 0
        self._max_page = 0

    async def build(self):
        items = await get_shop_items(self.pool, self.guild.id)
        # Roles first, then other goods — same grouping as the .shop store.
        items = [i for i in items if _is_role_item(i)] + [i for i in items if not _is_role_item(i)]
        cur = self.currency

        embed = discord.Embed(title=f"🛍️ {self.guild.name} Shop", color=SHOP_COLOR)
        if not items:
            embed.description = "The shop is empty!"
            return embed, []

        self._max_page = max(0, math.ceil(len(items) / PER_PAGE) - 1)
        self.page = min(self.page, self._max_page)
        for item in items[self.page * PER_PAGE:(self.page + 1) * PER_PAGE]:
            if _is_role_item(item):
                role = self.guild.get_role(item["role_given"])
                bits = ["🎭"]
                if role:
                    bits.append(role.mention)
                bits.append(f"⏳ {humanize_duration(item['role_duration'])}" if item["role_duration"] else "Permanent")
                extra = " · ".join(bits) + "\n"
            else:
                extra = ""
            embed.add_field(
                name=f"{item['name']} — {item['price']:,}{cur.emoji}",
                value=f"{extra}{item['description'] or 'No description'}",
                inline=False,
            )
        embed.set_footer(text=f"Page {self.page + 1}/{self._max_page + 1} — {len(items)} item(s)")

        select = discord.ui.Select(
            placeholder="Buy an item…",
            options=[
                discord.SelectOption(
                    label=f"{item['name']}"[:100],
                    value=str(item["id"]),
                    description=f"{item['price']:,} {cur.name}"[:100],
                )
                for item in items[:25]
            ],
            row=0,
        )
        select.callback = self._buy
        self._select = select

        items_row = [
            select,
            self.button("◀ Prev", self._prev, row=1, disabled=self.page == 0),
            self.button("Next ▶", self._next, row=1, disabled=self.page >= self._max_page),
            self.button("My Inventory", self._inventory, emoji="🎒",
                        style=discord.ButtonStyle.primary, row=1),
        ]
        return embed, items_row

    async def _buy(self, interaction: discord.Interaction):
        item = await get_item_by_id(self.pool, self.guild.id, int(self._select.values[0]))
        if not item:
            raise UserError("That item is no longer available.")
        if await user_is_locked(self.pool, self.guild.id, self.user.id):
            raise UserError("You're locked out of the economy.")
        success, message = await service.purchase(self.bot, self.guild, self.user, item)
        prefix = "🛍️" if success else "⚠️"
        await self.hub.refresh(interaction, notice=f"{prefix} {message}")

    async def _prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        await self.hub.refresh(interaction)

    async def _next(self, interaction: discord.Interaction):
        self.page = min(self._max_page, self.page + 1)
        await self.hub.refresh(interaction)

    async def _inventory(self, interaction: discord.Interaction):
        await self.hub.push(interaction, InventoryPage(self.hub))


class InventoryPage(Page):
    async def build(self):
        items = await get_inventory(self.pool, self.guild.id, self.user.id)
        embed = discord.Embed(title=f"🎒 {format_name(self.user)}'s Inventory", color=SHOP_COLOR)
        if not items:
            embed.description = "You have no items."
        for item in items:
            embed.add_field(
                name=f"{item['name']} x{item['quantity']}",
                value=item["description"] or "No description",
                inline=False,
            )
        return embed, []
