from __future__ import annotations

import discord


class ConfirmView(discord.ui.View):
    """An Agree/Decline confirmation gated to a single user.

    Prefer the `confirm()` helper below; use this class directly only when you
    need custom control over the prompt message.

    After the view stops, `value` is:
        True  — invoker pressed Confirm
        False — invoker pressed Cancel
        None  — timed out
    """

    def __init__(self, invoker_id: int, *, prompt: str = None,
                 embed: discord.Embed = None, timeout: float = 30.0):
        super().__init__(timeout=timeout)
        self.invoker_id = invoker_id
        self.prompt = prompt
        self.embed = embed
        self.value = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.invoker_id:
            await interaction.response.send_message("This confirmation isn't yours.", ephemeral=True)
            return False
        return True

    async def _finish(self, interaction: discord.Interaction, value: bool, note: str):
        self.value = value
        for child in self.children:
            child.disabled = True
        content = f"{self.prompt}\n{note}" if self.prompt else note
        await interaction.response.edit_message(content=content, embed=self.embed, view=self)
        self.stop()

    @discord.ui.button(label="Confirm", emoji="✅", style=discord.ButtonStyle.danger)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._finish(interaction, True, "✅ **Confirmed.**")

    @discord.ui.button(label="Cancel", emoji="✖️", style=discord.ButtonStyle.secondary)
    async def cancel_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._finish(interaction, False, "✖️ **Cancelled.**")


async def confirm(ctx, prompt: str = None, *, embed: discord.Embed = None,
                  timeout: float = 30.0) -> bool:
    """Send a prompt with Confirm/Cancel buttons and wait for the invoker's choice.

    Returns True only if the invoker pressed Confirm; Cancel and timeout return False.
    """
    view = ConfirmView(ctx.author.id, prompt=prompt, embed=embed, timeout=timeout)
    msg = await ctx.send(content=prompt, embed=embed, view=view)
    timed_out = await view.wait()
    if timed_out:
        for child in view.children:
            child.disabled = True
        note = "⏱️ *Timed out — cancelled.*"
        content = f"{prompt}\n{note}" if prompt else note
        try:
            await msg.edit(content=content, embed=embed, view=view)
        except discord.HTTPException:
            pass
        return False
    return view.value is True
