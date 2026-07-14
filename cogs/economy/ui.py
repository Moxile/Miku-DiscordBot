from __future__ import annotations

"""Economy pages for the Miku Menu (see core.ui). All money logic lives in
cogs.economy.service — these classes only render and collect input."""

import math

import discord

from cogs.economy import service
from core.names import format_name
from core.time_utils import humanize_duration
from core.ui import AmountModal, Page

PER_PAGE = 10


class EconomyPage(Page):
    async def build(self):
        bal = await service.get_balance(self.pool, self.guild.id, self.user.id)
        cur = self.currency
        embed = discord.Embed(title=f"💰 Economy — {format_name(self.user)}",
                              color=discord.Color.green())
        embed.add_field(name=f"Wallet ({cur.name})", value=f"{bal['wallet']:,}{cur.emoji}")
        embed.add_field(name=f"Bank ({cur.name})", value=f"{bal['bank']:,}{cur.emoji}")
        embed.add_field(name="Total", value=f"{bal['wallet'] + bal['bank']:,}{cur.emoji}")
        embed.set_thumbnail(url=self.user.display_avatar.url)
        embed.set_footer(text="Amounts accept 100, 5k, 2.5m, all, half.")

        items = [
            self.button("Deposit", self._deposit, emoji="🏦",
                        style=discord.ButtonStyle.primary, row=0),
            self.button("Withdraw", self._withdraw, emoji="💵",
                        style=discord.ButtonStyle.primary, row=0),
            self.button("Gift", self._gift, emoji="🎁", row=0),
            self.button("Work", self._work, emoji="🔨",
                        style=discord.ButtonStyle.success, row=1),
            self.button("Crime", self._crime, emoji="🕵️",
                        style=discord.ButtonStyle.danger, row=1),
            self.button("Collect Salary", self._collect, emoji="💼",
                        style=discord.ButtonStyle.success, row=1),
            self.button("Transactions", self._transactions, emoji="📜", row=2),
        ]
        return embed, items

    # ── wallet <-> bank ──

    async def _deposit(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            AmountModal(self.hub, title="Deposit into bank", handler=self._do_deposit))

    async def _do_deposit(self, interaction: discord.Interaction, raw: str):
        amount = await service.deposit(self.pool, self.guild.id, self.user.id, raw)
        await self.hub.refresh(
            interaction,
            notice=f"🏦 Deposited **{amount:,}**{self.currency.emoji} into your bank account.")

    async def _withdraw(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            AmountModal(self.hub, title="Withdraw from bank", handler=self._do_withdraw))

    async def _do_withdraw(self, interaction: discord.Interaction, raw: str):
        amount = await service.withdraw(self.pool, self.guild.id, self.user.id, raw)
        await self.hub.refresh(
            interaction,
            notice=f"💵 Withdrew **{amount:,}**{self.currency.emoji} from your bank account.")

    # ── earning ──

    async def _work(self, interaction: discord.Interaction):
        earnings = await service.work(self.pool, self.guild.id, self.user.id,
                                      self.session.channel_id)
        await self.hub.refresh(
            interaction,
            notice=f"🔨 You earned **{earnings:,}**{self.currency.emoji} from your work!")

    async def _crime(self, interaction: discord.Interaction):
        result = await service.crime(self.pool, self.guild.id, self.user.id,
                                     self.session.channel_id)
        cur = self.currency
        if result.success:
            notice = f"🤑 Crime success! You got away with **{result.payout:,}**{cur.emoji}."
        elif result.loss > 0:
            notice = (f"🚔 Busted! You lost **{result.loss:,}**{cur.emoji} "
                      f"({result.penalty_pct}% of your total wallet + bank).")
        else:
            notice = "🚔 You got caught — lucky for you, you had nothing worth taking."
        await self.hub.refresh(interaction, notice=notice)

    async def _collect(self, interaction: discord.Interaction):
        result = await service.collect(self.pool, self.guild, self.user,
                                       self.session.channel_id)
        cur = self.currency
        if result.collected:
            lines = ", ".join(f"**{name}** +{amt:,}{cur.emoji}" for name, amt in result.collected)
            notice = f"💼 Salary collected: {lines}."
        else:
            soonest = min(result.on_cooldown, key=lambda x: x[1])
            notice = (f"💼 All salaries already collected. Next up: **{soonest[0]}** in "
                      f"*{humanize_duration(soonest[1], short=True)}*.")
        await self.hub.refresh(interaction, notice=notice)

    # ── sub-pages ──

    async def _gift(self, interaction: discord.Interaction):
        await self.hub.push(interaction, GiftPage(self.hub))

    async def _transactions(self, interaction: discord.Interaction):
        await self.hub.push(interaction, TransactionsPage(self.hub))


class GiftPage(Page):
    async def build(self):
        embed = discord.Embed(
            title="🎁 Gift",
            description="Pick who to send money to — you'll be asked for the amount next.",
            color=discord.Color.green(),
        )
        select = discord.ui.UserSelect(placeholder="Choose a member…", row=0)
        select.callback = self._picked
        self._select = select
        return embed, [select]

    async def _picked(self, interaction: discord.Interaction):
        target = self._select.values[0]

        async def _do_gift(modal_interaction: discord.Interaction, raw: str):
            amount = await service.gift(self.pool, self.guild.id, self.user, target, raw)
            self.bot.dispatch("money_gift", self.guild.id, self.user.id, target.id, amount)
            await self.hub.pop(
                modal_interaction,
                notice=f"🎁 Gifted **{amount:,}**{self.currency.emoji} to **{format_name(target)}**.")

        await interaction.response.send_modal(
            AmountModal(self.hub, title=f"Gift to {target.display_name}", handler=_do_gift))


class TransactionsPage(Page):
    def __init__(self, hub):
        super().__init__(hub)
        self.page = 0
        self._max_page = 0

    async def build(self):
        rows, has_counting = await service.fetch_transactions(
            self.pool, self.guild.id, self.user.id)
        self._max_page = max(0, math.ceil(len(rows) / PER_PAGE) - 1)
        self.page = min(self.page, self._max_page)
        cur = self.currency

        embed = discord.Embed(title=f"📜 {format_name(self.user)}'s Transactions",
                              color=discord.Color.green())
        page_rows = rows[self.page * PER_PAGE:(self.page + 1) * PER_PAGE]
        lines = []
        for row in page_rows:
            date = row["created_at"].strftime("%Y-%m-%d %H:%M")
            sign = "+" if row["amount"] >= 0 else ""
            desc = row["description"] or row["tx_type"]
            lines.append(f"`{date}` **{sign}{row['amount']:,}**{cur.emoji} — {desc}")
        embed.description = "\n".join(lines) if lines else "No transactions."
        footer = f"Page {self.page + 1}/{self._max_page + 1} — {len(rows)} total"
        if has_counting:
            footer += " | counting entries collapsed"
        embed.set_footer(text=footer)

        items = [
            self.button("◀ Prev", self._prev, row=0, disabled=self.page == 0),
            self.button("Next ▶", self._next, row=0, disabled=self.page >= self._max_page),
        ]
        return embed, items

    async def _prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        await self.hub.refresh(interaction)

    async def _next(self, interaction: discord.Interaction):
        self.page = min(self._max_page, self.page + 1)
        await self.hub.refresh(interaction)
