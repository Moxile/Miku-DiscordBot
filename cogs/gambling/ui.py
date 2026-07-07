from __future__ import annotations

"""Gambling pages for the Miku Menu: game selection and simple games like betflip."""

import discord

from cogs.gambling import service
from core.errors import UserError
from core.ui import HubModal, Page


class BetAmountModal(HubModal):
    """Bet amount + tries prompt for betflip."""

    def __init__(self, hub, *, handler):
        super().__init__(hub, title="Betflip Setup")
        self.handler = handler
        self.bet = discord.ui.TextInput(label="Bet per flip", placeholder="e.g. 10", max_length=10)
        self.tries = discord.ui.TextInput(label="Number of flips", placeholder="e.g. 5", value="1", max_length=2)
        self.add_item(self.bet)
        self.add_item(self.tries)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            bet = int(self.bet.value.strip())
            tries = int(self.tries.value.strip())
        except ValueError:
            raise UserError("Bet and tries must be numbers.")
        if bet <= 0 or tries <= 0:
            raise UserError("Bet and tries must be positive.")
        await self.handler(interaction, bet, tries)


def _get_gambling_cog(page: Page):
    cog = page.bot.get_cog("Gambling")
    if cog is None:
        raise UserError("Gambling is currently unavailable.")
    return cog


class GamblingPage(Page):
    async def build(self):
        embed = discord.Embed(title="🎰 Games", color=discord.Color.purple())
        embed.description = "Pick a game to play."

        items = [
            self.button("🪙 Betflip", self._betflip, style=discord.ButtonStyle.primary, row=0),
            self.button("🎲 Blackjack", self._blackjack, style=discord.ButtonStyle.primary, row=0),
            self.button("✋ Rock-Paper-Scissors", self._rps, style=discord.ButtonStyle.primary, row=1),
            self.button("🃏 Higher-Lower", self._highlow, style=discord.ButtonStyle.primary, row=1),
            self.button("🎡 Roulette", self._roulette, style=discord.ButtonStyle.primary, row=2),
        ]
        return embed, items

    async def _betflip(self, interaction: discord.Interaction):
        """Launch into betflip flow."""
        await self.hub.push(interaction, BetflipChoicePage(self.hub))

    async def _blackjack(self, interaction: discord.Interaction):
        """Launch traditional blackjack (outside menu)."""
        # For now, just send a notice that they should use .blackjack
        await self.hub.refresh(interaction, notice="ℹ️ Use `.blackjack <bet>` in the gambling channel to play.")

    async def _rps(self, interaction: discord.Interaction):
        """Launch traditional RPS (outside menu)."""
        await self.hub.refresh(interaction, notice="ℹ️ Use `.rps <bet>` in the gambling channel to play.")

    async def _highlow(self, interaction: discord.Interaction):
        """Launch traditional higher-lower (outside menu)."""
        await self.hub.refresh(interaction, notice="ℹ️ Use `.hl <bet>` in the gambling channel to play.")

    async def _roulette(self, interaction: discord.Interaction):
        """Launch traditional roulette (outside menu)."""
        await self.hub.refresh(interaction, notice="ℹ️ Use `.roulette <option> <bet>` in the gambling channel to play.")


class BetflipChoicePage(Page):
    """Choose heads or tails for betflip."""

    async def build(self):
        embed = discord.Embed(title="🪙 Betflip — Choose", color=discord.Color.blue())
        embed.description = "Pick heads or tails, then set your bet and number of flips."

        items = [
            self.button("Heads", self._heads, emoji="H", style=discord.ButtonStyle.success, row=0),
            self.button("Tails", self._tails, emoji="T", style=discord.ButtonStyle.success, row=0),
        ]
        return embed, items

    async def _heads(self, interaction: discord.Interaction):
        await self.hub.push(interaction, BetflipBetPage(self.hub, "h"))

    async def _tails(self, interaction: discord.Interaction):
        await self.hub.push(interaction, BetflipBetPage(self.hub, "t"))


class BetflipBetPage(Page):
    def __init__(self, hub, choice: str):
        super().__init__(hub)
        self.choice = choice

    async def build(self):
        choice_label = "Heads" if self.choice == "h" else "Tails"
        embed = discord.Embed(title=f"🪙 Betflip — {choice_label}", color=discord.Color.blue())
        embed.description = "Set your bet and how many times to flip."

        items = [
            self.button("Set Bet", self._set_bet, emoji="💰", style=discord.ButtonStyle.primary, row=0),
        ]
        return embed, items

    async def _set_bet(self, interaction: discord.Interaction):
        async def _do(modal_interaction, bet, tries):
            cog = _get_gambling_cog(self)
            max_bet = await cog.get_max_bet(self.guild.id)
            result = await service.betflip(self.pool, self.guild.id, self.user.id, self.choice, bet, tries, max_bet)

            await self.hub.pop(modal_interaction)
            await self.hub.pop(modal_interaction)
            await self.hub.refresh(
                modal_interaction,
                notice=(
                    f"🪙 **{result.wins}W/{result.losses}L** — "
                    f"Net: **{'+' if result.net >= 0 else ''}{result.net}**"
                ),
            )

        await interaction.response.send_modal(BetAmountModal(self.hub, handler=_do))
