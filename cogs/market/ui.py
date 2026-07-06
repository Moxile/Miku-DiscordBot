from __future__ import annotations

"""Market pages for the Miku Menu: exchange overview → stock detail (info,
price chart, buy/sell/limit orders, gifts, order book) plus the portfolio
page with order cancellation."""

import discord

from cogs.market import service
from cogs.market.db import get_company, get_last_trade_price, get_open_orders, get_shareholders
from core.errors import UserError
from core.names import format_name
from core.ui import HubModal, Page, QuantityModal, parse_quantity


class QuantityPriceModal(HubModal):
    """Quantity + limit price prompt; handler(interaction, quantity, raw_price)."""

    def __init__(self, hub, *, title: str, price_label: str, handler):
        super().__init__(hub, title=title[:45])
        self.handler = handler
        self.quantity = discord.ui.TextInput(label="Quantity", placeholder="e.g. 10", max_length=10)
        self.price = discord.ui.TextInput(label=price_label, placeholder="e.g. 100, 5k", max_length=20)
        self.add_item(self.quantity)
        self.add_item(self.price)

    async def on_submit(self, interaction: discord.Interaction):
        await self.handler(interaction, parse_quantity(self.quantity.value), self.price.value)


class MarketPage(Page):
    """The exchange: all listed companies, with a picker into the detail page."""

    async def build(self):
        entries = await service.exchange_overview(self.pool, self.guild.id)
        cur = self.currency

        embed = discord.Embed(title="📈 Stock Exchange", color=discord.Color.blue())
        if not entries:
            embed.description = "No companies are listed yet."
        for e in entries:
            channel = self.guild.get_channel(e["stock_channel_id"])
            name = channel.mention if channel else e["name"]
            best_bid = f"{e['best_bid']:,}{cur.emoji}" if e["best_bid"] is not None else "None"
            best_ask = f"{e['best_ask']:,}{cur.emoji}" if e["best_ask"] is not None else "None"
            ipo_status = f" | IPO: {e['available_ipo_shares']:,}/{e['total_shares']:,} @ {e['ipo_price']:,}{cur.emoji}" if e["available_ipo_shares"] > 0 else ""
            embed.add_field(name=f"{e['name']} ({name})",
                            value=f"Bid: {best_bid} / Ask: {best_ask}{ipo_status}", inline=False)

        items = [self.button("My Portfolio", self._portfolio, emoji="💼",
                             style=discord.ButtonStyle.primary, row=1)]
        if entries:
            select = discord.ui.Select(
                placeholder="View a stock…",
                options=[
                    discord.SelectOption(label=e["name"][:100], value=str(e["stock_channel_id"]))
                    for e in entries[:25]
                ],
                row=0,
            )
            select.callback = self._pick_stock
            self._select = select
            items.insert(0, select)
        return embed, items

    async def _pick_stock(self, interaction: discord.Interaction):
        await self.hub.push(interaction, StockPage(self.hub, int(self._select.values[0])))

    async def _portfolio(self, interaction: discord.Interaction):
        await self.hub.push(interaction, PortfolioPage(self.hub))


class StockPage(Page):
    """One company: info card, price chart with window picker, and all trading actions."""

    def __init__(self, hub, stock_channel_id: int):
        super().__init__(hub)
        self.stock_channel_id = stock_channel_id
        self.window = "all"

    async def build(self):
        company = await get_company(self.pool, self.guild.id, self.stock_channel_id)
        if not company:
            raise UserError("This company is no longer listed.")
        cur = self.currency

        last_price = await get_last_trade_price(self.pool, self.guild.id, self.stock_channel_id)
        buy_orders = await get_open_orders(self.pool, self.guild.id, self.stock_channel_id, "buy")
        sell_orders = await get_open_orders(self.pool, self.guild.id, self.stock_channel_id, "sell")
        shareholders = await get_shareholders(self.pool, self.guild.id, self.stock_channel_id)

        best_bid = f"{buy_orders[0]['price']:,}{cur.emoji}" if buy_orders else "None"
        best_ask = f"{sell_orders[0]['price']:,}{cur.emoji}" if sell_orders else "None"

        total_shares = company["total_shares"]
        top_holders = sorted(shareholders, key=lambda r: r["quantity"], reverse=True)[:5]
        if top_holders:
            owners_lines = []
            for row in top_holders:
                member = self.guild.get_member(row["user_id"])
                name = format_name(member, self.guild, fallback=f"<@{row['user_id']}>")
                pct = row["quantity"] / total_shares * 100
                owners_lines.append(f"{name} — {row['quantity']:,} ({pct:.1f}%)")
            owners_value = "\n".join(owners_lines)
        else:
            owners_value = "No shareholders yet"

        channel = self.guild.get_channel(self.stock_channel_id)
        embed = discord.Embed(title=f"{company['name']} - Company Info", color=discord.Color.blue())
        embed.add_field(name="Channel", value=channel.mention if channel else str(self.stock_channel_id), inline=True)
        embed.add_field(name="Total Shares", value=f"{total_shares:,}", inline=True)
        embed.add_field(name="IPO Price", value=f"{company['ipo_price']:,}{cur.emoji}", inline=True)
        embed.add_field(name="IPO Shares Left", value=f"{company['available_ipo_shares']:,}", inline=True)
        embed.add_field(name="Last Trade", value=f"{last_price:,}{cur.emoji}" if last_price else "No trades yet", inline=True)
        embed.add_field(name="Best Bid / Ask", value=f"{best_bid} / {best_ask}", inline=True)
        embed.add_field(name="Treasury", value=f"{company['treasury']:,}{cur.emoji}", inline=True)
        embed.add_field(name="Level", value=str(company["company_level"]), inline=True)
        embed.add_field(name="Top Shareholders", value=owners_value, inline=False)

        files = []
        file = await service.render_window(self.pool, self.guild.id, self.stock_channel_id, company, self.window)
        if file is not None:
            embed.set_image(url=f"attachment://price_{self.window}.png")
            files.append(file)

        window_select = discord.ui.Select(
            placeholder="Chart window…",
            options=[
                discord.SelectOption(label=label, value=key, default=key == self.window)
                for key, (label, _subtitle, _days) in service.CHART_WINDOWS.items()
            ] + [discord.SelectOption(label="All time", value="all", default=self.window == "all")],
            row=0,
        )
        window_select.callback = self._pick_window
        self._window_select = window_select

        items = [
            window_select,
            self.button("Buy", self._buy, emoji="🟢", style=discord.ButtonStyle.success, row=1),
            self.button("Sell", self._sell, emoji="🔴", style=discord.ButtonStyle.danger, row=1),
            self.button("Limit Buy", self._limit_buy, row=1),
            self.button("Limit Sell", self._limit_sell, row=1),
            self.button("Order Book", self._orderbook, emoji="📖", row=2),
            self.button("Gift Shares", self._gift, emoji="🎁", row=2),
        ]
        return embed, items, files

    async def _pick_window(self, interaction: discord.Interaction):
        self.window = self._window_select.values[0]
        await self.hub.refresh(interaction)

    # ── trading actions ──

    async def _buy(self, interaction: discord.Interaction):
        async def _do(modal_interaction, quantity):
            result = await service.market_buy(
                self.pool, self.guild.id, self.user.id, self.stock_channel_id,
                quantity, self.session.channel_id)
            await self.hub.refresh(modal_interaction, notice=(
                f"🟢 Bought **{result.filled:,}/{result.quantity:,}x {result.company_name}** "
                f"for **{result.total:,}**{self.currency.emoji} (avg {result.avg_price:,}{self.currency.emoji})."))
        await interaction.response.send_modal(QuantityModal(self.hub, title="Market buy", handler=_do))

    async def _sell(self, interaction: discord.Interaction):
        async def _do(modal_interaction, quantity):
            result = await service.market_sell(
                self.pool, self.guild.id, self.user.id, self.stock_channel_id,
                quantity, self.session.channel_id)
            await self.hub.refresh(modal_interaction, notice=(
                f"🔴 Sold **{result.filled:,}/{result.quantity:,}x {result.company_name}** "
                f"for **{result.total:,}**{self.currency.emoji} (avg {result.avg_price:,}{self.currency.emoji})."))
        await interaction.response.send_modal(QuantityModal(self.hub, title="Market sell", handler=_do))

    async def _limit_buy(self, interaction: discord.Interaction):
        async def _do(modal_interaction, quantity, raw_price):
            result = await service.place_buy_order(
                self.pool, self.guild.id, self.user.id, self.stock_channel_id,
                quantity, raw_price, self.session.channel_id)
            await self.hub.refresh(modal_interaction, notice=self._order_notice("Buy", result))
        await interaction.response.send_modal(QuantityPriceModal(
            self.hub, title="Limit buy order", price_label="Max price per share", handler=_do))

    async def _limit_sell(self, interaction: discord.Interaction):
        async def _do(modal_interaction, quantity, raw_price):
            result = await service.place_sell_order(
                self.pool, self.guild.id, self.user.id, self.stock_channel_id,
                quantity, raw_price, self.session.channel_id)
            await self.hub.refresh(modal_interaction, notice=self._order_notice("Sell", result))
        await interaction.response.send_modal(QuantityPriceModal(
            self.hub, title="Limit sell order", price_label="Min price per share", handler=_do))

    def _order_notice(self, side: str, result) -> str:
        cur = self.currency
        if result.remaining > 0:
            notice = (f"📋 {side} order placed: **{result.remaining:,}x {result.company_name}** "
                      f"@ {result.price:,}{cur.emoji} (Order #{result.order_id}).")
            if result.filled > 0:
                notice += f" {result.filled:,} shares filled immediately."
            return notice
        verb = "Bought" if side == "Buy" else "Sold"
        return (f"📋 {side} order fully filled! {verb} **{result.filled:,}x "
                f"{result.company_name}** for **{result.total:,}**{cur.emoji}.")

    # ── sub-pages ──

    async def _orderbook(self, interaction: discord.Interaction):
        await self.hub.push(interaction, OrderBookPage(self.hub, self.stock_channel_id))

    async def _gift(self, interaction: discord.Interaction):
        await self.hub.push(interaction, GiftSharesPage(self.hub, self.stock_channel_id))


class OrderBookPage(Page):
    def __init__(self, hub, stock_channel_id: int):
        super().__init__(hub)
        self.stock_channel_id = stock_channel_id

    async def build(self):
        company = await get_company(self.pool, self.guild.id, self.stock_channel_id)
        if not company:
            raise UserError("This company is no longer listed.")
        buy_orders = await get_open_orders(self.pool, self.guild.id, self.stock_channel_id, "buy")
        sell_orders = await get_open_orders(self.pool, self.guild.id, self.stock_channel_id, "sell")

        cur = self.currency
        embed = discord.Embed(title=f"{company['name']} - Order Book", color=discord.Color.blue())

        sell_lines = []
        if company["available_ipo_shares"] > 0:
            sell_lines.append(f"{company['available_ipo_shares']:,}x @ {company['ipo_price']:,}{cur.emoji} — IPO")
        for o in sell_orders[:10]:
            member = self.guild.get_member(o["user_id"])
            name = format_name(member, self.guild, fallback=str(o["user_id"]))
            sell_lines.append(f"{o['remaining']:,}x @ {o['price']:,}{cur.emoji} — {name}")
        embed.add_field(name="Sell Orders (Asks)", value="\n".join(sell_lines) if sell_lines else "None", inline=False)

        buy_lines = []
        for o in buy_orders[:10]:
            member = self.guild.get_member(o["user_id"])
            name = format_name(member, self.guild, fallback=str(o["user_id"]))
            buy_lines.append(f"{o['remaining']:,}x @ {o['price']:,}{cur.emoji} — {name}")
        embed.add_field(name="Buy Orders (Bids)", value="\n".join(buy_lines) if buy_lines else "None", inline=False)

        return embed, []


class GiftSharesPage(Page):
    def __init__(self, hub, stock_channel_id: int):
        super().__init__(hub)
        self.stock_channel_id = stock_channel_id

    async def build(self):
        company = await get_company(self.pool, self.guild.id, self.stock_channel_id)
        if not company:
            raise UserError("This company is no longer listed.")
        embed = discord.Embed(
            title=f"🎁 Gift {company['name']} shares",
            description="Pick who to send shares to — you'll be asked for the quantity next.",
            color=discord.Color.purple(),
        )
        select = discord.ui.UserSelect(placeholder="Choose a member…", row=0)
        select.callback = self._picked
        self._select = select
        return embed, [select]

    async def _picked(self, interaction: discord.Interaction):
        target = self._select.values[0]

        async def _do(modal_interaction, quantity):
            company_name = await service.gift_stocks(
                self.pool, self.guild.id, self.user, target, self.stock_channel_id,
                quantity, self.session.channel_id)
            await self.hub.pop(modal_interaction, notice=(
                f"🎁 Gifted **{quantity:,}x {company_name}** to **{format_name(target)}**."))

        await interaction.response.send_modal(
            QuantityModal(self.hub, title=f"Gift to {target.display_name}", handler=_do))


class PortfolioPage(Page):
    async def build(self):
        overview = await service.portfolio_overview(self.pool, self.guild.id, self.user.id)
        cur = self.currency

        embed = discord.Embed(title=f"💼 {format_name(self.user)}'s Portfolio",
                              color=discord.Color.green())
        if not overview.holdings and not overview.orders:
            embed.description = "You have no holdings or open orders."
            return embed, []

        for h in overview.holdings:
            pl_str = f"+{h['pl']:,}" if h["pl"] >= 0 else f"{h['pl']:,}"
            embed.add_field(
                name=h["name"],
                value=f"{h['quantity']:,} shares @ {h['price']:,}{cur.emoji} = {h['value']:,}{cur.emoji}\n"
                      f"Avg cost: {h['avg_cost']:,}{cur.emoji} | P/L: {pl_str}{cur.emoji}\n"
                      f"Dividends received: {h['dividends']:,}{cur.emoji}",
                inline=False,
            )
        total_pl_str = f"+{overview.total_pl:,}" if overview.total_pl >= 0 else f"{overview.total_pl:,}"
        embed.add_field(
            name="Summary",
            value=f"Total value: {overview.total_value:,}{cur.emoji} | Total P/L: {total_pl_str}{cur.emoji} | Total dividends: {overview.total_dividends:,}{cur.emoji}",
            inline=False,
        )

        items = []
        if overview.orders:
            lines = []
            for o in overview.orders:
                side = "BUY" if o["side"] == "buy" else "SELL"
                lines.append(f"#{o['id']} {side} {o['remaining']:,}x {o['stock_name']} @ {o['price']:,}{cur.emoji}")
            embed.add_field(name="Open Orders", value="\n".join(lines), inline=False)

            select = discord.ui.Select(
                placeholder="Cancel an order…",
                options=[
                    discord.SelectOption(
                        label=f"#{o['id']} {o['side'].upper()} {o['remaining']:,}x {o['stock_name']}"[:100],
                        value=str(o["id"]),
                        description=f"@ {o['price']:,}"[:100],
                    )
                    for o in overview.orders[:25]
                ],
                row=0,
            )
            select.callback = self._cancel_order
            self._select = select
            items.append(select)
        return embed, items

    async def _cancel_order(self, interaction: discord.Interaction):
        order_id = int(self._select.values[0])
        order, refund = await service.cancel_user_order(
            self.pool, self.guild.id, self.user.id, order_id, self.session.channel_id)
        if order["side"] == "buy":
            notice = f"✖️ Buy order #{order_id} cancelled. Refunded **{refund:,}**{self.currency.emoji}."
        else:
            notice = f"✖️ Sell order #{order_id} cancelled. {order['remaining']} shares are available again."
        await self.hub.refresh(interaction, notice=notice)
