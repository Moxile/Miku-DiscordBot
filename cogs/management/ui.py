from __future__ import annotations

"""Admin/management pages for the Miku Menu: server settings, cog toggles, channel ignoring."""

import discord

from cogs.management import service
from core.errors import UserError
from core.ui import HubModal, Page


class AdminPage(Page):
    """Main admin hub."""

    async def build(self):
        embed = discord.Embed(title="⚙️ Server Settings", color=discord.Color.gold())
        embed.description = "Manage this server's configuration."

        items = [
            self.button("Cogs", self._cogs, emoji="📦", style=discord.ButtonStyle.primary, row=0),
            self.button("Channels", self._channels, emoji="📢", style=discord.ButtonStyle.primary, row=0),
            self.button("Currency", self._currency, emoji="💰", style=discord.ButtonStyle.primary, row=0),
        ]
        return embed, items

    async def _cogs(self, interaction: discord.Interaction):
        await self.hub.push(interaction, CogManagementPage(self.hub))

    async def _channels(self, interaction: discord.Interaction):
        await self.hub.push(interaction, ChannelManagementPage(self.hub))

    async def _currency(self, interaction: discord.Interaction):
        await self.hub.push(interaction, CurrencyPage(self.hub))


class CogManagementPage(Page):
    """View and toggle disabled cogs."""

    async def build(self):
        disabled = await service.list_disabled_cogs(self.pool, self.guild.id)

        embed = discord.Embed(title="📦 Cog Management", color=discord.Color.gold())
        if not disabled:
            embed.description = "All cogs are enabled."
        else:
            embed.description = f"**{len(disabled)}** cog(s) disabled:\n" + "\n".join(f"• {name}" for name in disabled)

        items = [
            self.button("Re-enable Cogs", self._reenable, emoji="✅", style=discord.ButtonStyle.success, row=0),
        ]
        return embed, items

    async def _reenable(self, interaction: discord.Interaction):
        disabled = await service.list_disabled_cogs(self.pool, self.guild.id)
        if not disabled:
            raise UserError("No cogs are disabled.")
        await self.hub.push(interaction, CogTogglePage(self.hub, disabled))


class CogTogglePage(Page):
    def __init__(self, hub, cog_names: list[str]):
        super().__init__(hub)
        self.cog_names = cog_names

    async def build(self):
        embed = discord.Embed(title="📦 Re-enable Cog", color=discord.Color.gold())
        embed.description = "Pick a cog to re-enable."

        select = discord.ui.Select(
            placeholder="Choose a cog…",
            options=[
                discord.SelectOption(label=name, value=name)
                for name in self.cog_names[:25]
            ],
            row=0,
        )
        select.callback = self._pick_cog
        self._select = select

        items = [select]
        return embed, items

    async def _pick_cog(self, interaction: discord.Interaction):
        cog_name = self._select.values[0]
        await service.enable_cog(self.pool, self.guild.id, cog_name, self.bot._disabled_cogs_cache)
        await self.hub.pop(interaction)
        await self.hub.refresh(interaction, notice=f"✅ **{cog_name}** re-enabled.")


class ChannelManagementPage(Page):
    """View and toggle ignored channels."""

    async def build(self):
        ignored_ids = await service.list_ignored_channels(self.pool, self.guild.id)
        ignored_channels = [self.guild.get_channel(cid) for cid in ignored_ids]
        ignored_channels = [c for c in ignored_channels if c is not None]

        embed = discord.Embed(title="📢 Channel Management", color=discord.Color.gold())
        if not ignored_channels:
            embed.description = "No channels are currently ignored."
        else:
            embed.description = f"**{len(ignored_channels)}** channel(s) ignored:\n" + \
                              "\n".join(f"• {c.mention}" for c in ignored_channels)

        items = []
        if ignored_channels:
            items.append(self.button("Un-ignore Channels", self._unignore, emoji="✅", style=discord.ButtonStyle.success, row=0))
        items.append(self.button("Ignore a Channel", self._ignore, emoji="🚫", style=discord.ButtonStyle.danger, row=0))

        return embed, items

    async def _ignore(self, interaction: discord.Interaction):
        channels = sorted([c for c in self.guild.text_channels if not c.name.startswith("⚫")])
        if not channels:
            raise UserError("No text channels available.")
        await self.hub.push(interaction, ChannelSelectPage(self.hub, channels, mode="ignore"))

    async def _unignore(self, interaction: discord.Interaction):
        ignored_ids = await service.list_ignored_channels(self.pool, self.guild.id)
        ignored_channels = [self.guild.get_channel(cid) for cid in ignored_ids]
        ignored_channels = [c for c in ignored_channels if c is not None]
        if not ignored_channels:
            raise UserError("No ignored channels.")
        await self.hub.push(interaction, ChannelSelectPage(self.hub, ignored_channels, mode="unignore"))


class ChannelSelectPage(Page):
    def __init__(self, hub, channels: list, mode: str):
        super().__init__(hub)
        self.channels = channels
        self.mode = mode

    async def build(self):
        action = "Un-ignore" if self.mode == "unignore" else "Ignore"
        embed = discord.Embed(title=f"📢 {action} Channel", color=discord.Color.gold())
        embed.description = f"Pick a channel to {self.mode}."

        select = discord.ui.Select(
            placeholder="Choose a channel…",
            options=[
                discord.SelectOption(label=c.name[:100], value=str(c.id))
                for c in self.channels[:25]
            ],
            row=0,
        )
        select.callback = self._pick_channel
        self._select = select

        items = [select]
        return embed, items

    async def _pick_channel(self, interaction: discord.Interaction):
        channel_id = int(self._select.values[0])
        if self.mode == "ignore":
            await service.ignore_channel(self.pool, self.guild.id, channel_id, self.bot._ignored_channels_cache)
            msg = f"🚫 Channel ignored."
        else:
            await service.unignore_channel(self.pool, self.guild.id, channel_id, self.bot._ignored_channels_cache)
            msg = f"✅ Channel un-ignored."
        await self.hub.pop(interaction)
        await self.hub.refresh(interaction, notice=msg)


class CurrencyPage(Page):
    """View and set server currency."""

    async def build(self):
        cur_info = await service.get_currency(self.pool, self.guild.id)

        embed = discord.Embed(title="💰 Currency Settings", color=discord.Color.gold())
        embed.description = f"Current currency: **{cur_info.name}** {cur_info.emoji}"

        items = [
            self.button("Change Currency", self._change, emoji="✏️", style=discord.ButtonStyle.primary, row=0),
            self.button("Reset to Default", self._reset, emoji="↺", style=discord.ButtonStyle.secondary, row=0),
        ]
        return embed, items

    async def _change(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CurrencyModal(self.hub))

    async def _reset(self, interaction: discord.Interaction):
        await service.reset_currency(self.pool, self.guild.id, self.bot._currency_cache)
        cur_info = await service.get_currency(self.pool, self.guild.id)
        await self.hub.refresh(interaction, notice=f"↺ Currency reset to **{cur_info.name}** {cur_info.emoji}.")


class CurrencyModal(HubModal):
    """Set currency emoji and name."""

    def __init__(self, hub):
        super().__init__(hub, title="Set Currency")
        self.emoji_field = discord.ui.TextInput(label="Emoji", placeholder="e.g. 🪙", max_length=10)
        self.name_field = discord.ui.TextInput(label="Name", placeholder="e.g. Coins", max_length=50)
        self.add_item(self.emoji_field)
        self.add_item(self.name_field)

    async def on_submit(self, interaction: discord.Interaction):
        emoji = self.emoji_field.value.strip()
        name = self.name_field.value.strip()
        if not emoji or not name:
            raise UserError("Emoji and name cannot be empty.")
        await service.set_currency(self.pool, self.bot, self.guild.id, emoji, name, self.bot._currency_cache)
        await self.hub.refresh(interaction, notice=f"💰 Currency set to **{name}** {emoji}.")
