from __future__ import annotations

"""The Miku Menu framework: an ephemeral, button-driven window onto the bot.

A hub is one ephemeral message owned by one user. It renders a stack of Pages
(screens); every button, select, and modal edits that same message in place,
so the whole thing behaves like a single private window. Feature cogs
contribute pages via core.ui.registry and never send messages themselves —
they call hub.refresh()/push()/pop() and raise core.errors.UserError for
anything the user did wrong.
"""

import discord

from core.currency import Currency
from core.errors import UserError

NAV_ROW = 4  # bottom row is reserved for the Back / Home / Close bar
HUB_TIMEOUT = 600  # seconds of inactivity; must stay under Discord's 15-minute
                   # interaction-token lifetime so the expiry edit still works


class HubSession:
    """Per-window state shared by every page of one open hub."""

    def __init__(self, bot, guild: discord.Guild, user: discord.Member, channel_id: int):
        self.bot = bot
        self.guild = guild
        self.user = user
        self.channel_id = channel_id  # where the menu was opened; channel-locked
                                      # features (e.g. .work) check against this

    @property
    def pool(self):
        return self.bot.pool

    @property
    def currency(self) -> Currency:
        return self.bot.get_currency(self.guild.id)


class Page:
    """One screen of the hub. Subclasses implement build().

    A page may keep its own state (pagination cursor, picked target, …); it is
    rebuilt on every refresh, so anything shown is always live data.
    """

    def __init__(self, hub: HubView):
        self.hub = hub

    @property
    def session(self) -> HubSession:
        return self.hub.session

    @property
    def bot(self):
        return self.hub.session.bot

    @property
    def pool(self):
        return self.hub.session.pool

    @property
    def guild(self) -> discord.Guild:
        return self.hub.session.guild

    @property
    def user(self) -> discord.Member:
        return self.hub.session.user

    @property
    def currency(self) -> Currency:
        return self.hub.session.currency

    async def build(self) -> tuple:
        """Return (embed, items) — or (embed, items, files) to attach images —
        for this screen. Items go on rows 0–3."""
        raise NotImplementedError

    def button(self, label: str, callback, *, emoji: str = None,
               style: discord.ButtonStyle = discord.ButtonStyle.secondary,
               row: int = 0, disabled: bool = False) -> discord.ui.Button:
        """A button wired to `callback(interaction)`."""
        btn = discord.ui.Button(label=label, emoji=emoji, style=style, row=row, disabled=disabled)
        btn.callback = callback
        return btn


class HubModal(discord.ui.Modal):
    """Base for hub prompt dialogs: routes UserError back onto the page."""

    def __init__(self, hub: HubView, **kwargs):
        super().__init__(**kwargs)
        self.hub = hub

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        if isinstance(error, UserError):
            await self.hub.refresh(interaction, notice=f"⚠️ {error}")
            return
        await super().on_error(interaction, error)


class HubView(discord.ui.View):
    """The single view behind one open menu window."""

    def __init__(self, bot, guild: discord.Guild, user: discord.Member, channel_id: int):
        super().__init__(timeout=HUB_TIMEOUT)
        self.session = HubSession(bot, guild, user, channel_id)
        self.stack: list[Page] = []
        self.message: discord.InteractionMessage | None = None
        self._notice: str | None = None

    @property
    def page(self) -> Page:
        return self.stack[-1]

    # ── navigation ──

    async def open(self, interaction: discord.Interaction):
        """Send the hub as the ephemeral response to `interaction`."""
        embed, files = await self._rebuild()
        await interaction.response.send_message(embed=embed, files=files, view=self, ephemeral=True)
        self.message = await interaction.original_response()

    async def push(self, interaction: discord.Interaction, page: Page, *, notice: str = None):
        self.stack.append(page)
        await self.refresh(interaction, notice=notice)

    async def pop(self, interaction: discord.Interaction, *, notice: str = None):
        if len(self.stack) > 1:
            self.stack.pop()
        await self.refresh(interaction, notice=notice)

    async def go_home(self, interaction: discord.Interaction):
        del self.stack[1:]
        await self.refresh(interaction)

    async def refresh(self, interaction: discord.Interaction, *, notice: str = None):
        """Re-render the current page onto the hub message.

        `notice` is a one-shot status line (✅/⚠️/…) shown above the page —
        it disappears on the next interaction.
        """
        self._notice = notice
        embed, files = await self._rebuild()
        # `attachments` replaces existing files, so an empty list also clears
        # a previous page's image.
        if not interaction.response.is_done():
            await interaction.response.edit_message(embed=embed, attachments=files, view=self)
        elif self.message:
            await self.message.edit(embed=embed, attachments=files, view=self)

    # ── rendering ──

    async def _rebuild(self) -> tuple[discord.Embed, list[discord.File]]:
        built = await self.page.build()
        embed, items = built[0], built[1]
        files = list(built[2]) if len(built) > 2 else []
        if self._notice:
            embed.description = f"{self._notice}\n\n{embed.description or ''}".rstrip()
            self._notice = None
        self.clear_items()
        for item in items:
            self.add_item(item)
        for item in self._nav_items():
            self.add_item(item)
        return embed, files

    def _nav_items(self) -> list[discord.ui.Item]:
        at_home = len(self.stack) <= 1
        back = discord.ui.Button(label="Back", emoji="◀️", row=NAV_ROW,
                                 style=discord.ButtonStyle.secondary, disabled=at_home)
        back.callback = self._nav_back
        home = discord.ui.Button(label="Home", emoji="🏠", row=NAV_ROW,
                                 style=discord.ButtonStyle.secondary, disabled=at_home)
        home.callback = self._nav_home
        close = discord.ui.Button(label="Close", emoji="✖️", row=NAV_ROW,
                                  style=discord.ButtonStyle.secondary)
        close.callback = self._nav_close
        return [back, home, close]

    async def _nav_back(self, interaction: discord.Interaction):
        await self.pop(interaction)

    async def _nav_home(self, interaction: discord.Interaction):
        await self.go_home(interaction)

    async def _nav_close(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content="✖️ Menu closed.", embed=None, view=None,
        )
        self.stop()

    # ── plumbing ──

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        # The message is ephemeral so only the owner should ever reach this;
        # kept as a safety net.
        if interaction.user.id != self.session.user.id:
            await interaction.response.send_message("This menu isn't yours.", ephemeral=True)
            return False
        return True

    async def on_error(self, interaction: discord.Interaction, error: Exception, item):
        if isinstance(error, UserError):
            try:
                await self.refresh(interaction, notice=f"⚠️ {error}")
            except UserError as render_error:
                # The current page itself can no longer render (its data went
                # away mid-session) — fall back to home with the error shown.
                del self.stack[1:]
                await self.refresh(interaction, notice=f"⚠️ {render_error}")
            return
        await super().on_error(interaction, error, item)  # log the traceback
        try:
            note = f"Something went wrong: `{error}`"
            if interaction.response.is_done():
                await interaction.followup.send(note, ephemeral=True)
            else:
                await interaction.response.send_message(note, ephemeral=True)
        except discord.HTTPException:
            pass

    async def on_timeout(self):
        if self.message:
            try:
                await self.message.edit(
                    content="⏱️ Menu expired — press **Open Menu** to start again.",
                    embed=None, view=None,
                )
            except discord.HTTPException:
                pass  # the 15-minute interaction token may already be dead
