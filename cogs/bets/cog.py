from __future__ import annotations

import discord
from discord.ext import commands

from cogs.bets.db import remove_member_data
from cogs.bets.ui import BetsPage
from core.ui import HomePage, HubView


class OpenBetsView(discord.ui.View):
    """Public launcher button for `.bets`.

    Persistent (timeout=None + fixed custom_id, re-attached via bot.add_view on
    startup). Clicking it opens the clicker's own private menu straight on the
    Bets page — the only way a prefix command can reach an ephemeral message.
    """

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Open Bets", emoji="🎲",
                       style=discord.ButtonStyle.primary, custom_id="miku:open:bets")
    async def open_bets(self, interaction: discord.Interaction, button: discord.ui.Button):
        hub = HubView(interaction.client, interaction.guild, interaction.user,
                      channel_id=interaction.channel_id)
        hub.stack.append(HomePage(hub))
        hub.stack.append(BetsPage(hub))
        await hub.open(interaction)


class Bets(commands.Cog):
    """Bookmaker-style fixed-odds bets, driven entirely through the Miku Menu."""

    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    async def cog_load(self):
        self.bot.add_view(OpenBetsView())

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Clean up a member's bet takes when they leave, are kicked, or banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    @commands.command()
    @commands.guild_only()
    async def bets(self, ctx):
        """Open the Bets menu — view open bets, place one, or create your own."""
        embed = discord.Embed(
            title="🎲 Bets",
            description="Press the button to open your private Bets menu — only you will see it.",
            color=discord.Color.dark_gold(),
        )
        await ctx.send(embed=embed, view=OpenBetsView())
