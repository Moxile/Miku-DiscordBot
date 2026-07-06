from __future__ import annotations

"""Reusable input dialogs for Miku Menu pages."""

import discord

from core.errors import UserError
from core.ui.hub import HubModal

AMOUNT_PLACEHOLDER = "e.g. 100, 5k, 2.5m, all, half"


def parse_quantity(raw: str) -> int:
    """Parse a whole-number quantity; raises UserError on anything else."""
    try:
        quantity = int(raw.strip().replace(",", "").replace("_", ""))
    except ValueError:
        raise UserError(f"`{raw}` is not a valid quantity — use a whole number.")
    if quantity <= 0:
        raise UserError("Quantity must be positive.")
    return quantity


class AmountModal(HubModal):
    """A one-field amount prompt. `handler(interaction, raw_value)` does the
    actual work; UserError from it lands back on the page as a ⚠️ notice."""

    def __init__(self, hub, *, title: str, handler, label: str = "Amount"):
        super().__init__(hub, title=title[:45])
        self.handler = handler
        self.amount = discord.ui.TextInput(
            label=label, placeholder=AMOUNT_PLACEHOLDER, max_length=20,
        )
        self.add_item(self.amount)

    async def on_submit(self, interaction: discord.Interaction):
        await self.handler(interaction, self.amount.value)


class QuantityModal(HubModal):
    """One-field whole-number quantity prompt; handler(interaction, quantity: int)."""

    def __init__(self, hub, *, title: str, handler):
        super().__init__(hub, title=title[:45])
        self.handler = handler
        self.quantity = discord.ui.TextInput(label="Quantity", placeholder="e.g. 10", max_length=10)
        self.add_item(self.quantity)

    async def on_submit(self, interaction: discord.Interaction):
        await self.handler(interaction, parse_quantity(self.quantity.value))
