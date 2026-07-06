from __future__ import annotations

import discord
from discord.ext import commands

from core.ui import HomePage, HubView


class OpenMenuView(discord.ui.View):
    """The public 'Open Menu' button.

    Persistent (timeout=None + fixed custom_id, re-attached via bot.add_view on
    startup), so buttons from before a restart keep working. Anyone can click
    it — each clicker gets their own private, ephemeral menu, which is the only
    way to reach an ephemeral message from a prefix command.
    """

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Open Menu", emoji="🎀",
                       style=discord.ButtonStyle.primary, custom_id="miku:open")
    async def open_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        hub = HubView(interaction.client, interaction.guild, interaction.user,
                      channel_id=interaction.channel_id)
        hub.stack.append(HomePage(hub))
        await hub.open(interaction)


class Hub(commands.Cog):
    """The Miku Menu — drive the whole bot with buttons instead of commands."""

    def __init__(self, bot):
        self.bot = bot

    async def cog_load(self):
        self.bot.add_view(OpenMenuView())

    @commands.command(aliases=["menu"])
    @commands.guild_only()
    async def miku(self, ctx):
        """Open the Miku menu — browse and use every feature with buttons.
        The menu itself is private: only you see yours."""
        embed = discord.Embed(
            title="🎀 Miku Menu",
            description="Press the button to open your private menu — only you will see it.",
            color=discord.Color.pink(),
        )
        await ctx.send(embed=embed, view=OpenMenuView())
