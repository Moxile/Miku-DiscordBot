from __future__ import annotations

"""CFD pages for the Miku Menu: a leveraged-positions dashboard with live P/L,
a picker to close a position, and a modal to open a new one."""

import discord

from cogs.cfd import service
from cogs.cfd.cog import DIRECTION_ALIASES, _dir_emoji
from core.errors import UserError
from core.names import format_name
from core.ui import HubModal, Page, parse_quantity


def _quotes(page: Page):
    cog = page.bot.get_cog("RealStocks")
    if cog is None:
        raise UserError("CFD trading is currently unavailable (real stocks are offline).")
    return cog.quotes


class CFDOpenModal(HubModal):
    """Collects the four inputs needed to open a CFD, then delegates to the service."""

    def __init__(self, hub, page: CFDPage):
        super().__init__(hub, title="Open CFD Position")
        self.page = page
        self.symbol = discord.ui.TextInput(label="Ticker", placeholder="e.g. NVDA", max_length=15)
        self.direction = discord.ui.TextInput(label="Direction", placeholder="long or short", max_length=6)
        self.notional = discord.ui.TextInput(label="Notional (exposure in coins)", placeholder="e.g. 1000", max_length=12)
        self.leverage = discord.ui.TextInput(label="Leverage", placeholder="e.g. 5", max_length=3, default="1")
        for item in (self.symbol, self.direction, self.notional, self.leverage):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        symbol = self.symbol.value.upper().strip()
        direction = DIRECTION_ALIASES.get(self.direction.value.lower().strip())
        if direction is None:
            raise UserError("Direction must be `long` or `short`.")
        notional = parse_quantity(self.notional.value)
        leverage = parse_quantity(self.leverage.value)

        result = await service.open_position(
            self.page.pool, _quotes(self.page), self.page.guild.id, self.page.user.id,
            symbol, direction, notional, leverage, self.page.session.channel_id)
        cur = self.page.currency
        await self.hub.refresh(interaction, notice=(
            f"{_dir_emoji(direction)} Opened **#{result.position_id}** — {result.leverage}x "
            f"{direction} {result.symbol}, margin **{result.margin:,}**{cur.emoji} "
            f"(liq ${result.liquidation_price:,.2f})."))


class CFDPage(Page):
    """Open-positions dashboard: live P/L per position, a picker to close one, and
    an Open button."""

    async def build(self):
        cur = self.currency
        quotes = _quotes(self)
        rows = await service.list_open_positions(self.pool, quotes, self.guild.id, self.user.id)

        embed = discord.Embed(title=f"📊 {format_name(self.user)}'s CFD Positions",
                              color=discord.Color.blurple())
        if not rows:
            embed.description = ("You have no open CFD positions.\n\n"
                                 "Press **Open Position** to place a leveraged long or short "
                                 "on a real stock.")
            return embed, [self.button("Open Position", self._open, emoji="➕",
                                       style=discord.ButtonStyle.success, row=0)]

        total_equity = 0
        options = []
        for r in rows:
            head = (f"**#{r['id']}** {_dir_emoji(r['direction'])} {r['direction'].title()} "
                    f"{r['leverage']}x {r['symbol']}")
            if r["price"] is None:
                embed.add_field(name=head, value="Mark price unavailable", inline=False)
            else:
                total_equity += r["equity"]
                pl_str = f"+{r['pl']:,}" if r["pl"] >= 0 else f"{r['pl']:,}"
                fin = f" · fin -{r['financing']:,}" if r["financing"] else ""
                embed.add_field(
                    name=head,
                    value=(f"{r['notional']:,}{cur.emoji} notional · entry ${r['entry_price']:,.2f} "
                           f"→ mark ${r['price']:,.2f}\n"
                           f"Margin {r['margin']:,}{cur.emoji}{fin} · liq ${r['liquidation_price']:,.2f} · "
                           f"P/L **{pl_str}**{cur.emoji}"),
                    inline=False)
            options.append(discord.SelectOption(
                label=f"#{r['id']} {r['direction']} {r['leverage']}x {r['symbol']}"[:100],
                value=str(r["id"])))
        embed.set_footer(text=f"Total equity across positions: {total_equity:,}")

        close_select = discord.ui.Select(placeholder="Close a position…", options=options, row=0)
        close_select.callback = self._pick_close
        self._close_select = close_select

        items = [
            close_select,
            self.button("Open Position", self._open, emoji="➕",
                        style=discord.ButtonStyle.success, row=1),
        ]
        return embed, items

    async def _open(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CFDOpenModal(self.hub, self))

    async def _pick_close(self, interaction: discord.Interaction):
        position_id = int(self._close_select.values[0])
        result = await service.close_position(self.pool, _quotes(self), self.guild.id,
                                               self.user.id, position_id, self.session.channel_id)
        cur = self.currency
        pl_str = f"+{result.realized_pl:,}" if result.realized_pl >= 0 else f"{result.realized_pl:,}"
        await self.hub.refresh(interaction, notice=(
            f"Closed **#{position_id}** {result.direction} {result.symbol} @ "
            f"${result.close_price:,.2f} — payout {result.payout:,}{cur.emoji} "
            f"(P/L **{pl_str}**{cur.emoji})."))
