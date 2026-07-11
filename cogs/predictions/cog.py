from __future__ import annotations

import discord
from discord.ext import commands

from cogs.predictions.db import remove_member_data
from cogs.predictions.ui import PredictionsPage
from core.ui import HomePage, HubView


class OpenPredictionsView(discord.ui.View):
    """Public launcher button for `.predictions`.

    Persistent (timeout=None + fixed custom_id, re-attached via bot.add_view on
    startup). Clicking it opens the clicker's own private menu straight on the
    Predictions page.
    """

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Open Predictions", emoji="🔮",
                       style=discord.ButtonStyle.primary, custom_id="miku:open:predictions")
    async def open_predictions(self, interaction: discord.Interaction, button: discord.ui.Button):
        hub = HubView(interaction.client, interaction.guild, interaction.user,
                      channel_id=interaction.channel_id)
        hub.stack.append(HomePage(hub))
        hub.stack.append(PredictionsPage(hub))
        await hub.open(interaction)


class Predictions(commands.Cog):
    """Parimutuel pool predictions, driven entirely through the Miku Menu."""

    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    async def cog_load(self):
        self.bot.add_view(OpenPredictionsView())

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Clean up a member's prediction bets when they leave, are kicked, or banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    @commands.command()
    @commands.is_owner()
    async def setpredictorrole(self, ctx, role: discord.Role):
        """Set which role can create predictions and bets (shared setting)."""
        await self.pool.execute(
            """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'predictor_role', $2)
               ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
            ctx.guild.id, str(role.id),
        )
        await ctx.send(f"Predictor role set to {role.mention}.")

    @commands.command(aliases=["preds"])
    @commands.guild_only()
    async def predictions(self, ctx):
        """Open the Predictions menu — view, bet on, or create predictions."""
        embed = discord.Embed(
            title="🔮 Predictions",
            description="Press the button to open your private Predictions menu — only you will see it.",
            color=discord.Color.teal(),
        )
        await ctx.send(embed=embed, view=OpenPredictionsView())
