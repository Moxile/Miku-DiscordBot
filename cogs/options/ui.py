from __future__ import annotations

"""Options pages for the Miku Menu: an open-positions dashboard with live value,
a picker to close a position early, and a modal to buy a new option."""

import discord

from cogs.options import service
from cogs.options.cog import TYPE_ALIASES, _type_emoji
from core.errors import UserError
from core.names import format_name
from core.ui import HubModal, Page, parse_quantity


def _quotes(page: Page):
    cog = page.bot.get_cog("RealStocks")
    if cog is None:
        raise UserError("Options trading is currently unavailable (real stocks are offline).")
    return cog.quotes


def _parse_strike(raw: str) -> float:
    try:
        strike = float(raw.strip().replace(",", ""))
    except ValueError:
        raise UserError(f"`{raw}` is not a valid strike price.")
    if strike <= 0:
        raise UserError("Strike must be positive.")
    return strike


class OptionBuyModal(HubModal):
    """Collects the five inputs needed to buy an option, then delegates to the service."""

    def __init__(self, hub, page: OptionsPage):
        super().__init__(hub, title="Buy Option")
        self.page = page
        self.symbol = discord.ui.TextInput(label="Ticker", placeholder="e.g. NVDA", max_length=15)
        self.opt_type = discord.ui.TextInput(label="Type", placeholder="call or put", max_length=4)
        self.strike = discord.ui.TextInput(label="Strike (USD)", placeholder="e.g. 200", max_length=12)
        self.days = discord.ui.TextInput(label="Days to expiry", placeholder="e.g. 30", max_length=3)
        self.contracts = discord.ui.TextInput(label="Contracts", placeholder="e.g. 1", max_length=5, default="1")
        for item in (self.symbol, self.opt_type, self.strike, self.days, self.contracts):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        symbol = self.symbol.value.upper().strip()
        opt_type = TYPE_ALIASES.get(self.opt_type.value.lower().strip())
        if opt_type is None:
            raise UserError("Option type must be `call` or `put`.")
        strike = _parse_strike(self.strike.value)
        days = parse_quantity(self.days.value)
        contracts = parse_quantity(self.contracts.value)

        result = await service.buy(
            self.page.pool, _quotes(self.page), self.page.guild.id, self.page.user.id,
            symbol, opt_type, strike, days, contracts, self.page.session.channel_id)
        cur = self.page.currency
        await self.hub.refresh(interaction, notice=(
            f"{_type_emoji(opt_type)} Bought **#{result.position_id}** — {result.contracts}x "
            f"{result.symbol} {result.strike:g} {opt_type}, premium "
            f"**{result.total_cost:,}**{cur.emoji}."))


class OptionsPage(Page):
    """Open-options dashboard: live value per position, a picker to close one early,
    and a Buy button."""

    async def build(self):
        cur = self.currency
        quotes = _quotes(self)
        rows = await service.list_open_positions(self.pool, quotes, self.guild.id, self.user.id)

        embed = discord.Embed(title=f"🎟️ {format_name(self.user)}'s Options",
                              color=discord.Color.gold())
        if not rows:
            embed.description = ("You have no open option positions.\n\n"
                                 "Press **Buy Option** to buy a European call or put on a real "
                                 "stock — the premium is priced with Black-Scholes.")
            return embed, [self.button("Buy Option", self._buy, emoji="➕",
                                       style=discord.ButtonStyle.success, row=0)]

        total_value = 0
        options = []
        for r in rows:
            head = (f"**#{r['id']}** {_type_emoji(r['opt_type'])} {r['contracts']}x {r['symbol']} "
                    f"{r['strike']:g} {r['opt_type'].title()}")
            expiry_txt = f"<t:{int(r['expiry'].timestamp())}:R>"
            if r["value"] is None:
                embed.add_field(name=head, value=f"exp {expiry_txt} · mark unavailable", inline=False)
            else:
                total_value += r["value"]
                pl_str = f"+{r['pl']:,}" if r["pl"] >= 0 else f"{r['pl']:,}"
                embed.add_field(
                    name=head,
                    value=(f"spot ${r['spot']:,.2f} · exp {expiry_txt}\n"
                           f"Paid {r['premium_paid']:,}{cur.emoji} · value {r['value']:,}{cur.emoji} · "
                           f"P/L **{pl_str}**{cur.emoji}"),
                    inline=False)
            options.append(discord.SelectOption(
                label=f"#{r['id']} {r['contracts']}x {r['symbol']} {r['strike']:g} {r['opt_type']}"[:100],
                value=str(r["id"])))
        embed.set_footer(text=f"Total mark value: {total_value:,}")

        close_select = discord.ui.Select(placeholder="Close a position early…", options=options, row=0)
        close_select.callback = self._pick_close
        self._close_select = close_select

        items = [
            close_select,
            self.button("Buy Option", self._buy, emoji="➕",
                        style=discord.ButtonStyle.success, row=1),
        ]
        return embed, items

    async def _buy(self, interaction: discord.Interaction):
        await interaction.response.send_modal(OptionBuyModal(self.hub, self))

    async def _pick_close(self, interaction: discord.Interaction):
        position_id = int(self._close_select.values[0])
        result = await service.close(self.pool, _quotes(self), self.guild.id,
                                     self.user.id, position_id, self.session.channel_id)
        cur = self.currency
        pl_str = f"+{result.realized_pl:,}" if result.realized_pl >= 0 else f"{result.realized_pl:,}"
        await self.hub.refresh(interaction, notice=(
            f"Closed **#{position_id}** {result.symbol} {result.strike:g} {result.opt_type} — "
            f"payout {result.payout:,}{cur.emoji} (P/L **{pl_str}**{cur.emoji})."))
