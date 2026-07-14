from __future__ import annotations

"""Waifu pages for the Miku Menu: viewing harem, buying, gifting."""

from datetime import datetime, timezone

import discord

from cogs.waifu import service
from cogs.waifu.db import ensure_waifu, get_harem, get_waifu
from core.errors import UserError
from core.names import format_name
from core.ui import HubModal, Page, QuantityModal


class WaifuAmountModal(HubModal):
    """Waifu purchase amount prompt."""

    def __init__(self, hub, *, handler):
        super().__init__(hub, title="Waifu Purchase")
        self.handler = handler
        self.amount = discord.ui.TextInput(
            label="Amount to pay",
            placeholder="(optional) or leave blank for minimum",
            max_length=12,
            required=False,
        )
        self.add_item(self.amount)

    async def on_submit(self, interaction: discord.Interaction):
        amount = None
        if self.amount.value.strip():
            try:
                amount = int(self.amount.value.strip().replace(",", "").replace("_", ""))
            except ValueError:
                raise UserError("Amount must be a number.")
            if amount <= 0:
                raise UserError("Amount must be positive.")
        await self.handler(interaction, amount)


class WaifuPage(Page):
    async def build(self):
        async with self.pool.acquire() as conn:
            my_waifu = await get_waifu(conn, self.guild.id, self.user.id)
            my_harem = await get_harem(conn, self.guild.id, self.user.id)

        cur = self.currency
        embed = discord.Embed(title="💕 Waifu", color=discord.Color.from_rgb(255, 105, 180))

        if my_waifu and my_waifu["owner_id"]:
            owner_name = await self._get_owner_name(my_waifu["owner_id"])
            embed.add_field(name="Your Owner", value=owner_name, inline=True)
            if my_waifu["spouse_id"]:
                spouse_name = await self._get_owner_name(my_waifu["spouse_id"])
                embed.add_field(name="Status", value=f"💍 Married to {spouse_name}", inline=True)
            elif my_waifu["engaged_since"]:
                days = (datetime.now(timezone.utc) - my_waifu["engaged_since"]).days
                embed.add_field(name="Status", value=f"💕 Engaged ({days}d)", inline=True)
        else:
            embed.add_field(name="Your Status", value="Single 🥺", inline=True)

        if my_waifu:
            embed.add_field(name="Your Value", value=f"{cur.emoji} {my_waifu['value']:,}", inline=True)

        if my_harem:
            total_value = sum(r["value"] for r in my_harem)
            embed.add_field(
                name="Your Harem",
                value=f"**{len(my_harem)}** waifu(s) — {cur.emoji} {total_value:,} total",
                inline=False,
            )

        items = [
            self.button("👀 View Harem", self._view_harem, emoji="🏠", row=0),
            self.button("🛍️ Buy Waifu", self._buy, emoji="💰", style=discord.ButtonStyle.success, row=0),
            self.button("🎁 Gift", self._gift, emoji="🎁", style=discord.ButtonStyle.primary, row=1),
        ]
        return embed, items

    async def _get_owner_name(self, user_id: int) -> str:
        try:
            user = await self.bot.fetch_user(user_id)
            return format_name(user, self.guild)
        except Exception:
            return f"User {user_id}"

    async def _view_harem(self, interaction: discord.Interaction):
        await self.hub.push(interaction, WaifuHaremPage(self.hub))

    async def _buy(self, interaction: discord.Interaction):
        # Get all members in the guild
        await self.hub.push(interaction, WaifuBuySelectionPage(self.hub))

    async def _gift(self, interaction: discord.Interaction):
        await self.hub.push(interaction, WaifuGiftSelectionPage(self.hub))


class WaifuHaremPage(Page):
    async def build(self):
        async with self.pool.acquire() as conn:
            harem = await get_harem(conn, self.guild.id, self.user.id)

        cur = self.currency
        embed = discord.Embed(
            title=f"💕 {format_name(self.user)}'s Harem",
            color=discord.Color.from_rgb(255, 105, 180),
        )

        if not harem:
            embed.description = "You don't own any waifus yet."
            return embed, []

        total_value = sum(r["value"] for r in harem)
        lines = []
        for i, row in enumerate(harem[:25], 1):
            name = await self._get_display_name(row["user_id"])
            status = "💍 Married" if row["spouse_id"] else ("💕 Engaged" if row["engaged_since"] and row["owner_id"] == self.user.id else "")
            lines.append(f"`{i}.` **{name}** — {cur.emoji} {row['value']:,} {status}")

        embed.description = "\n".join(lines) if lines else "No waifus."
        embed.add_field(name="Total Value", value=f"{cur.emoji} {total_value:,}", inline=False)
        return embed, []

    async def _get_display_name(self, user_id: int) -> str:
        member = self.guild.get_member(user_id)
        if member:
            return format_name(member, self.guild)
        try:
            user = await self.bot.fetch_user(user_id)
            return format_name(user, self.guild)
        except Exception:
            return f"User {user_id}"


class WaifuBuySelectionPage(Page):
    async def build(self):
        # Get members list for select dropdown
        members = sorted([m for m in self.guild.members if not m.bot], key=lambda m: m.name)[:25]

        embed = discord.Embed(title="🛍️ Buy a Waifu", color=discord.Color.from_rgb(255, 105, 180))
        embed.description = "Pick someone to buy as your waifu."

        select = discord.ui.Select(
            placeholder="Choose a waifu…",
            options=[
                discord.SelectOption(label=format_name(m, self.guild)[:100], value=str(m.id))
                for m in members
            ],
            row=0,
        )
        select.callback = self._pick_member
        self._select = select

        items = [select]
        return embed, items

    async def _pick_member(self, interaction: discord.Interaction):
        target_id = int(self._select.values[0])
        target_member = self.guild.get_member(target_id)
        if target_member is None:
            raise UserError("That member is no longer in the server.")
        if target_id == self.user.id:
            raise UserError("You can't buy yourself.")
        if target_member.bot:
            raise UserError("You can't buy a bot.")
        await self.hub.push(interaction, WaifuBuyConfirmPage(self.hub, target_id))


class WaifuBuyConfirmPage(Page):
    def __init__(self, hub, target_id: int):
        super().__init__(hub)
        self.target_id = target_id

    async def build(self):
        target_member = self.guild.get_member(self.target_id)
        if target_member is None:
            raise UserError("That member is no longer in the server.")

        async with self.pool.acquire() as conn:
            target_waifu = await ensure_waifu(conn, self.guild.id, self.target_id)

        if target_waifu["spouse_id"]:
            raise UserError("This user is married and cannot be bought.")

        cur = self.currency
        embed = discord.Embed(
            title=f"💕 Buy {format_name(target_member, self.guild)}",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.description = f"Buying costs at least {cur.emoji} **{target_waifu['value']:,}** (their current value)."

        items = [
            self.button("Pay Minimum", self._pay_minimum, emoji="🏷️", row=0),
            self.button("Custom Amount", self._custom_amount, emoji="💰", row=0),
        ]
        return embed, items

    def _buy_notice(self, result) -> str:
        msg = f"💕 Bought **{format_name(self.guild.get_member(result.target_id), self.guild)}** for {self.currency.emoji} {result.paid:,}!"
        if result.payout:
            prev = self.guild.get_member(result.prev_owner_id)
            prev_name = format_name(prev, self.guild) if prev else "the previous owner"
            msg += f" **{prev_name}** got {self.currency.emoji} {result.payout:,}."
        if result.engaged:
            msg += " 💍 You're now engaged!"
        return msg

    async def _pay_minimum(self, interaction: discord.Interaction):
        result = await service.buy_waifu(self.pool, self.guild.id, self.user.id, self.target_id, None)
        await self.hub.pop(interaction)
        await self.hub.refresh(interaction, notice=self._buy_notice(result))

    async def _custom_amount(self, interaction: discord.Interaction):
        async def _do(modal_interaction, amount):
            result = await service.buy_waifu(self.pool, self.guild.id, self.user.id, self.target_id, amount)
            await self.hub.pop(modal_interaction)
            await self.hub.refresh(modal_interaction, notice=self._buy_notice(result))

        await interaction.response.send_modal(WaifuAmountModal(self.hub, handler=_do))


class WaifuGiftSelectionPage(Page):
    async def build(self):
        async with self.pool.acquire() as conn:
            harem = await get_harem(conn, self.guild.id, self.user.id)

        embed = discord.Embed(title="🎁 Gift a Waifu", color=discord.Color.from_rgb(255, 105, 180))

        if not harem:
            embed.description = "You don't own any waifus to gift."
            return embed, []

        embed.description = "Pick someone from your harem to gift."

        select = discord.ui.Select(
            placeholder="Choose who to gift…",
            options=[
                discord.SelectOption(label=self._member_label(m)[:100], value=str(m["user_id"]))
                for m in harem[:25]
            ],
            row=0,
        )
        select.callback = self._pick_waifu
        self._select = select

        items = [select]
        return embed, items

    def _member_label(self, row) -> str:
        member = self.guild.get_member(row["user_id"])
        if member:
            return format_name(member, self.guild)
        return f"User {row['user_id']}"

    async def _pick_waifu(self, interaction: discord.Interaction):
        waifu_id = int(self._select.values[0])
        await self.hub.push(interaction, WaifuGiftRecipientPage(self.hub, waifu_id))


class WaifuGiftRecipientPage(Page):
    def __init__(self, hub, waifu_id: int):
        super().__init__(hub)
        self.waifu_id = waifu_id

    async def build(self):
        waifu_member = self.guild.get_member(self.waifu_id)
        if waifu_member is None:
            raise UserError("That member is no longer in the server.")

        members = sorted([m for m in self.guild.members if not m.bot and m.id != self.user.id], key=lambda m: m.name)[:25]

        embed = discord.Embed(
            title=f"🎁 Gift {format_name(waifu_member, self.guild)}",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.description = "Pick who to gift them to."

        select = discord.ui.Select(
            placeholder="Choose a recipient…",
            options=[
                discord.SelectOption(label=format_name(m, self.guild)[:100], value=str(m.id))
                for m in members
            ],
            row=0,
        )
        select.callback = self._pick_recipient
        self._select = select

        items = [select]
        return embed, items

    async def _pick_recipient(self, interaction: discord.Interaction):
        recipient_id = int(self._select.values[0])
        result = await service.gift_waifu(self.pool, self.guild.id, self.user.id, self.waifu_id, recipient_id)
        waifu_member = self.guild.get_member(self.waifu_id)
        recipient_member = self.guild.get_member(recipient_id)
        msg = f"🎁 Gifted **{format_name(waifu_member, self.guild) if waifu_member else 'User'}** " \
              f"to **{format_name(recipient_member, self.guild) if recipient_member else 'User'}** ({self.currency.emoji} {result.value:,})!"
        if result.engaged:
            msg += " 💍 They're now engaged!"
        await self.hub.pop(interaction)
        await self.hub.pop(interaction)
        await self.hub.refresh(interaction, notice=msg)
