from __future__ import annotations

"""Prediction pages for the Miku Menu (see core.ui). All logic lives in
cogs.predictions.service — these classes only render and collect input.

Flow: PredictionsPage → View Active (PredictionsListPage) → pick one
(PredictionDetailPage), where players bet and the creator closes/resolves.
"""

import discord

from cogs.predictions import service
from core.names import format_name
from core.ui import HubModal, Page

COLOR = discord.Color.teal()


def _option_lines(options, totals, pool_total, emoji) -> str:
    lines = []
    for o in options:
        total = totals.get(o["id"], 0)
        pct = f" ({total * 100 // pool_total}%)" if pool_total > 0 else ""
        lines.append(f"{o['option_index']}. **{o['label']}** — {total}{emoji}{pct}")
    return "\n".join(lines)


# ── main page ──

class PredictionsPage(Page):
    async def build(self):
        preds = await service.list_active(self.pool, self.guild.id)
        can_create = await service.can_create(self.pool, self.guild, self.user)

        embed = discord.Embed(
            title="🔮 Predictions",
            description=(
                "Pool betting. The house opens a question; everyone bets into a shared "
                "pool, and winners split the **whole pool** in proportion to their stake.\n\n"
                f"**{len(preds)}** active prediction(s) right now."
            ),
            color=COLOR,
        )
        embed.set_footer(text="Amounts accept 100, 5k, all, half.")

        items = [
            self.button("View Predictions", self._view, emoji="📋",
                        style=discord.ButtonStyle.primary, row=0, disabled=not preds),
        ]
        if can_create:
            items.append(self.button("Create Prediction", self._create, emoji="➕",
                                     style=discord.ButtonStyle.success, row=1))
        return embed, items

    async def _view(self, interaction: discord.Interaction):
        await self.hub.push(interaction, PredictionsListPage(self.hub))

    async def _create(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreatePredictionModal(self.hub))


# ── browse ──

class PredictionsListPage(Page):
    async def build(self):
        cur = self.currency
        preds = await service.list_active(self.pool, self.guild.id)
        embed = discord.Embed(title="📋 Active Predictions", color=COLOR)
        if not preds:
            embed.description = "No active predictions right now."
            return embed, []

        lines, options = [], []
        for pred in preds:
            _, opts, totals = await service.get_detail(self.pool, self.guild.id, pred["id"])
            pool_total = sum(totals.values())
            status = pred["status"].capitalize()
            lines.append(f"**#{pred['id']}** [{status}] {pred['question']} — pool {pool_total}{cur.emoji}")
            options.append(discord.SelectOption(
                label=f"#{pred['id']} — {pred['question'][:80]}",
                value=str(pred["id"]),
                description=f"{status} · pool {pool_total} · {len(opts)} options",
            ))
        embed.description = "\n".join(lines)

        select = discord.ui.Select(placeholder="Open a prediction…", options=options[:25], row=0)
        select.callback = self._picked
        self._select = select
        return embed, [select]

    async def _picked(self, interaction: discord.Interaction):
        pred_id = int(self._select.values[0])
        await self.hub.push(interaction, PredictionDetailPage(self.hub, pred_id))


class PredictionDetailPage(Page):
    def __init__(self, hub, prediction_id: int):
        super().__init__(hub)
        self.prediction_id = prediction_id

    async def build(self):
        cur = self.currency
        pred, options, totals = await service.get_detail(self.pool, self.guild.id, self.prediction_id)
        pool_total = sum(totals.values())

        embed = discord.Embed(
            title=f"🔮 Prediction #{pred['id']} [{pred['status'].capitalize()}]",
            description=pred["question"],
            color=COLOR,
        )
        embed.add_field(name="Total Pool", value=f"{pool_total}{cur.emoji}", inline=True)
        embed.add_field(name="Options",
                        value=_option_lines(options, totals, pool_total, cur.emoji) or "None",
                        inline=False)

        items = []
        is_open = pred["status"] == "open"
        can_manage = service.can_manage(self.user, pred)

        if is_open:
            items.append(self.button("Place Bet", self._bet, emoji="💰",
                                     style=discord.ButtonStyle.primary, row=0))
        if can_manage:
            if is_open:
                items.append(self.button("Close (stop bets)", self._close, emoji="🔒", row=1))
            if pred["status"] != "resolved":
                items.append(self.button("Resolve", self._resolve, emoji="🏁",
                                         style=discord.ButtonStyle.success, row=1))
        return embed, items

    async def _bet(self, interaction: discord.Interaction):
        _, options, _ = await service.get_detail(self.pool, self.guild.id, self.prediction_id)
        await self.hub.push(interaction, PickOptionPage(
            self.hub, self.prediction_id, options, mode="bet"))

    async def _resolve(self, interaction: discord.Interaction):
        _, options, _ = await service.get_detail(self.pool, self.guild.id, self.prediction_id)
        await self.hub.push(interaction, PickOptionPage(
            self.hub, self.prediction_id, options, mode="resolve"))

    async def _close(self, interaction: discord.Interaction):
        result = await service.close(self.pool, self.guild, self.user, self.prediction_id)
        await self.hub.refresh(
            interaction,
            notice=f"🔒 Prediction #{self.prediction_id} closed — no more bets. "
                   f"Pool is **{result.pool_total}**{self.currency.emoji}.")


class PickOptionPage(Page):
    """Pick an option — to bet on (mode='bet') or to declare the winner
    (mode='resolve')."""

    def __init__(self, hub, prediction_id: int, options, *, mode: str):
        super().__init__(hub)
        self.prediction_id = prediction_id
        self.options = options
        self.mode = mode

    async def build(self):
        if self.mode == "bet":
            title, prompt = "💰 Pick an option", "Choose what to bet on — you'll enter your amount next."
        else:
            title, prompt = "🏁 Pick the winner", "Choose the winning option to pay out the pool."
        embed = discord.Embed(title=title, description=prompt, color=COLOR)
        select = discord.ui.Select(
            placeholder="Choose an option…",
            options=[
                discord.SelectOption(label=f"{o['option_index']}. {o['label'][:80]}",
                                     value=str(o["option_index"]))
                for o in self.options[:25]
            ],
            row=0,
        )
        select.callback = self._picked
        self._select = select
        return embed, [select]

    async def _picked(self, interaction: discord.Interaction):
        idx = int(self._select.values[0])
        if self.mode == "bet":
            await interaction.response.send_modal(
                BetAmountModal(self.hub, self.prediction_id, idx))
            return
        result = await service.resolve(self.pool, self.guild, self.user, self.prediction_id, idx)
        # The prediction is settled; rebuild back to a fresh active list.
        del self.hub.stack[1:]
        self.hub.stack.append(PredictionsPage(self.hub))
        self.hub.stack.append(PredictionsListPage(self.hub))
        await self.hub.refresh(interaction, notice=self._summary(result))

    def _summary(self, result) -> str:
        cur = self.currency
        if result.payouts:
            total = sum(p for _, _, p in result.payouts)
            return (f"🏁 Resolved — **{result.winner['label']}** won. "
                    f"{len(result.payouts)} winner(s) split **{total}**{cur.emoji}.")
        return (f"🏁 Resolved — **{result.winner['label']}** won, "
                f"but there were no winning bets.")


# ── modals ──

class BetAmountModal(HubModal):
    def __init__(self, hub, prediction_id: int, option_idx: int):
        super().__init__(hub, title=f"Bet on prediction #{prediction_id}")
        self.prediction_id = prediction_id
        self.option_idx = option_idx
        self.amount = discord.ui.TextInput(
            label="Amount", placeholder="e.g. 100, 5k, all, half", max_length=20)
        self.add_item(self.amount)

    async def on_submit(self, interaction: discord.Interaction):
        result = await service.place_bet(
            self.hub.session.pool, self.hub.session.guild, self.hub.session.user,
            self.prediction_id, self.option_idx, self.amount.value)
        cur = self.hub.session.currency
        self.hub.stack.pop()  # drop the option-picker page
        await self.hub.refresh(
            interaction,
            notice=f"💰 Bet **{result.amount}**{cur.emoji} on **{result.option_label}**!")


class CreatePredictionModal(HubModal):
    def __init__(self, hub):
        super().__init__(hub, title="Create a prediction")
        self.question = discord.ui.TextInput(
            label="Question", placeholder="Who wins the match?", max_length=200)
        self.options = discord.ui.TextInput(
            label="Options — one per line (2–20)",
            placeholder="Team A\nTeam B\nDraw",
            style=discord.TextStyle.paragraph, max_length=500)
        self.add_item(self.question)
        self.add_item(self.options)

    async def on_submit(self, interaction: discord.Interaction):
        pred, opt_rows = await service.create(
            self.hub.session.pool, self.hub.session.guild, self.hub.session.user,
            self.question.value, self.options.value)
        await self.hub.refresh(
            interaction,
            notice=f"➕ Created prediction **#{pred['id']}** with {len(opt_rows)} options. "
                   f"It's now open under **View Predictions**.")
