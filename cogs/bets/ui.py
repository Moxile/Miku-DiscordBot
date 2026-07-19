from __future__ import annotations

"""Bookmaker-bet pages for the Miku Menu (see core.ui). All logic lives in
cogs.bets.service — these classes only render and collect input.

Flow: BetsPage → View Open Bets (BetsListPage) → pick one (BetDetailPage),
where players place a bet and the host resolves or cancels it. Hosts create
new bets straight from BetsPage via a modal.
"""

import discord

from cogs.bets import service
from core.names import format_name
from core.ui import HubModal, Page

COLOR = discord.Color.dark_gold()


def _stake_range(bet) -> str:
    lo, hi = bet["min_stake"], bet["max_stake"]
    if lo is None and hi is None:
        return "any"
    if lo is None:
        return f"≤{hi}"
    if hi is None:
        return f"≥{lo}"
    if lo == hi:
        return f"{lo}"
    return f"{lo}-{hi}"


def _pool_label(bet, cur) -> str:
    if bet["bot_funded"]:
        return "🤖 bot-funded"
    return f"{bet['pool_remaining']}/{bet['pool']}{cur.emoji} left"


# ── main page ──

class BetsPage(Page):
    async def build(self):
        bets = await service.list_open_bets(self.pool, self.guild.id)
        can_create = await service.can_create(self.pool, self.guild, self.user, self.bot)

        embed = discord.Embed(
            title="🎲 Bets",
            description=(
                "Bookmaker-style fixed-odds bets. A host funds a pool and offers odds; "
                "players accept with a stake and win **stake × odds** if they're right.\n\n"
                f"**{len(bets)}** bet(s) still active (open or closed, awaiting resolution)."
            ),
            color=COLOR,
        )
        embed.set_footer(text="Amounts accept 100, 5k, 2.5m.")

        items = [
            self.button("View Open Bets", self._view, emoji="📋",
                        style=discord.ButtonStyle.primary, row=0,
                        disabled=not bets),
        ]
        if can_create:
            items.append(self.button("Create Bet", self._create, emoji="➕",
                                     style=discord.ButtonStyle.success, row=1))
            items.append(self.button("Create Multi-Bet", self._create_multi, emoji="🎯",
                                     style=discord.ButtonStyle.success, row=1))
        # Bot-funded bets create money on payout, so they're admin-only (bot
        # owner, guild owner, Administrator, or the owner role — Bot.is_owner).
        # The bot covers the pool, so no funding is deducted from the host.
        if await self.bot.is_owner(self.user):
            items.append(self.button("Create Bot Bet", self._create_bot, emoji="🤖",
                                     style=discord.ButtonStyle.secondary, row=2))
            items.append(self.button("Create Bot Multi-Bet", self._create_bot_multi, emoji="🤖",
                                     style=discord.ButtonStyle.secondary, row=2))
        return embed, items

    async def _view(self, interaction: discord.Interaction):
        await self.hub.push(interaction, BetsListPage(self.hub))

    async def _create(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateBetModal(self.hub))

    async def _create_multi(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateMultiBetModal(self.hub))

    async def _create_bot(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateBetModal(self.hub, bot_funded=True))

    async def _create_bot_multi(self, interaction: discord.Interaction):
        await interaction.response.send_modal(CreateMultiBetModal(self.hub, bot_funded=True))


# ── browse ──

class BetsListPage(Page):
    async def build(self):
        cur = self.currency
        bets = await service.list_open_bets(self.pool, self.guild.id)
        embed = discord.Embed(title="📋 Open Bets", color=COLOR)
        if not bets:
            embed.description = "No open bets right now."
            return embed, []

        lines, options = [], []
        for bet in bets[:25]:
            host = self.guild.get_member(bet["host_id"])
            host_name = format_name(host, self.guild, fallback=str(bet["host_id"]))
            if bet["is_multi"]:
                odds = "multiple options"
            else:
                odds = f"x{service.format_odds(bet['odds'])}"
            desc = bet["description"] or "(no description)"
            tag = " 🔒" if bet["status"] == "closed" else ""
            lines.append(
                f"**#{bet['id']}**{tag} — {desc}\n"
                f"　{host_name} · {odds} · stake {_stake_range(bet)}{cur.emoji} · "
                f"pool {_pool_label(bet, cur)}"
            )
            options.append(discord.SelectOption(
                label=f"#{bet['id']}{tag} — {desc[:80]}",
                value=str(bet["id"]),
                description=f"{odds} · stake {_stake_range(bet)}",
            ))
        embed.description = "\n".join(lines)

        select = discord.ui.Select(placeholder="Open a bet…", options=options, row=0)
        select.callback = self._picked
        self._select = select
        return embed, [select]

    async def _picked(self, interaction: discord.Interaction):
        bet_id = int(self._select.values[0])
        await self.hub.push(interaction, BetDetailPage(self.hub, bet_id))


class BetDetailPage(Page):
    def __init__(self, hub, bet_id: int):
        super().__init__(hub)
        self.bet_id = bet_id

    async def build(self):
        cur = self.currency
        bet, options, takes = await service.get_bet_detail(self.pool, self.guild.id, self.bet_id)
        host = self.guild.get_member(bet["host_id"])
        host_name = format_name(host, self.guild, fallback=str(bet["host_id"]))
        options_by_id = {o["id"]: o for o in options}

        embed = discord.Embed(
            title=f"Bet #{bet['id']} [{bet['status']}]",
            description=bet["description"] or "*no description*",
            color=COLOR,
        )
        embed.add_field(name="Host", value=host_name, inline=True)
        if bet["is_multi"]:
            totals = service.option_totals(takes)
            opt_lines = [
                f"{o['idx']}. {o['label']} — x{service.format_odds(o['odds'])} "
                f"({totals.get(o['id'], 0)}{cur.emoji} staked)"
                for o in options
            ]
            embed.add_field(name="Options", value="\n".join(opt_lines) or "None", inline=False)
        else:
            embed.add_field(name="Odds", value=f"x{service.format_odds(bet['odds'])}", inline=True)
        embed.add_field(name="Stake", value=f"{_stake_range(bet)}{cur.emoji}", inline=True)
        embed.add_field(name="Pool", value=_pool_label(bet, cur), inline=True)
        embed.add_field(name="Bets placed", value=str(len(takes)), inline=True)

        if takes:
            all_lines = []
            for t in takes:
                member = self.guild.get_member(t["user_id"])
                name = format_name(member, self.guild, fallback=str(t["user_id"]))
                if bet["is_multi"]:
                    opt = options_by_id.get(t["option_id"])
                    potential = service.payout_for(t["stake"], opt["odds"]) if opt else 0
                    all_lines.append(f"{name} → {opt['label'] if opt else '?'}: "
                                      f"{t['stake']}{cur.emoji} (wins {potential})")
                else:
                    potential = service.payout_for(t["stake"], bet["odds"])
                    all_lines.append(f"{name}: {t['stake']}{cur.emoji} (wins {potential})")

            lines = []
            shown = 0
            for line in all_lines[:15]:
                remaining = len(all_lines) - shown - 1
                more_suffix_len = len(f"\n… and {remaining} more") if remaining > 0 else 0
                budget = 1024 - more_suffix_len
                candidate = "\n".join(lines + [line])
                if len(candidate) > budget:
                    break
                lines.append(line)
                shown += 1
            if len(all_lines) > shown:
                lines.append(f"… and {len(all_lines) - shown} more")
            embed.add_field(name="Current bets", value="\n".join(lines) or "None", inline=False)

        items = []
        is_open = bet["status"] == "open"
        is_closed = bet["status"] == "closed"
        is_host = bet["host_id"] == self.user.id
        can_manage = is_host or self.user.guild_permissions.administrator

        if is_open and not is_host:
            items.append(self.button("Place Bet", self._place, emoji="💰",
                                     style=discord.ButtonStyle.primary, row=0))
        if is_open and can_manage:
            items.append(self.button("Close Betting", self._close, emoji="🔒",
                                     style=discord.ButtonStyle.secondary, row=1))
        if (is_open or is_closed) and can_manage:
            items.append(self.button("Resolve", self._resolve, emoji="🏁",
                                     style=discord.ButtonStyle.success, row=1))
            if not takes:
                items.append(self.button("Cancel Bet", self._cancel, emoji="🗑️",
                                         style=discord.ButtonStyle.danger, row=1))
        return embed, items

    async def _close(self, interaction: discord.Interaction):
        await service.close_bet(self.pool, self.guild, self.user, self.bet_id)
        await self.hub.refresh(
            interaction,
            notice=f"🔒 Bet #{self.bet_id} closed — no new bets can be placed. Resolve it when the result is in.")

    # placing a bet
    async def _place(self, interaction: discord.Interaction):
        bet, options, _ = await service.get_bet_detail(self.pool, self.guild.id, self.bet_id)
        if bet["is_multi"]:
            await self.hub.push(interaction, PickOptionPage(self.hub, self.bet_id, options))
        else:
            await interaction.response.send_modal(
                StakeModal(self.hub, self.bet_id, option_idx=None))

    # resolving
    async def _resolve(self, interaction: discord.Interaction):
        bet, options, _ = await service.get_bet_detail(self.pool, self.guild.id, self.bet_id)
        await self.hub.push(interaction, ResolvePage(self.hub, self.bet_id, bet, options))

    async def _cancel(self, interaction: discord.Interaction):
        refund = await service.cancel_bet(self.pool, self.guild, self.user, self.bet_id)
        tail = (f" Pool of **{refund}**{self.currency.emoji} refunded."
                if refund > 0 else "")
        await self.hub.pop(interaction, notice=f"🗑️ Bet #{self.bet_id} cancelled.{tail}")


class PickOptionPage(Page):
    """Choose which option of a multi-bet to back, then enter a stake."""

    def __init__(self, hub, bet_id: int, options):
        super().__init__(hub)
        self.bet_id = bet_id
        self.options = options

    async def build(self):
        cur = self.currency
        _, _, takes = await service.get_bet_detail(self.pool, self.guild.id, self.bet_id)
        totals = service.option_totals(takes)
        embed = discord.Embed(
            title="🎯 Pick an option",
            description="Choose the outcome you want to back — you'll enter your stake next.",
            color=COLOR,
        )
        select = discord.ui.Select(
            placeholder="Choose an option…",
            options=[
                discord.SelectOption(
                    label=f"{o['idx']}. {o['label'][:80]}",
                    value=str(o["idx"]),
                    description=f"x{service.format_odds(o['odds'])} · "
                                f"{totals.get(o['id'], 0)}{cur.emoji} staked",
                )
                for o in self.options[:25]
            ],
            row=0,
        )
        select.callback = self._picked
        self._select = select
        return embed, [select]

    async def _picked(self, interaction: discord.Interaction):
        idx = int(self._select.values[0])
        await interaction.response.send_modal(
            StakeModal(self.hub, self.bet_id, option_idx=idx, pop_first=True))


class ResolvePage(Page):
    """Host/admin picks the outcome. Single bets get Win/Lose buttons; multi
    bets get a select of the winning option."""

    def __init__(self, hub, bet_id: int, bet, options):
        super().__init__(hub)
        self.bet_id = bet_id
        self.bet = bet
        self.options = options

    async def build(self):
        embed = discord.Embed(
            title=f"🏁 Resolve Bet #{self.bet_id}",
            description=self.bet["description"] or "*no description*",
            color=COLOR,
        )
        if self.bet["is_multi"]:
            embed.add_field(name="Pick the winning option", value="​", inline=False)
            select = discord.ui.Select(
                placeholder="Winning option…",
                options=[
                    discord.SelectOption(label=f"{o['idx']}. {o['label'][:80]}",
                                         value=str(o["idx"]))
                    for o in self.options[:25]
                ],
                row=0,
            )
            select.callback = self._picked_multi
            self._select = select
            return embed, [select]

        embed.add_field(
            name="Who won?",
            value="**Players win** pays everyone out at the offered odds. "
                  "**Players lose** means the host keeps all stakes.",
            inline=False,
        )
        return embed, [
            self.button("Players Win", self._win, emoji="✅",
                        style=discord.ButtonStyle.success, row=0),
            self.button("Players Lose", self._lose, emoji="❌",
                        style=discord.ButtonStyle.danger, row=0),
        ]

    async def _win(self, interaction: discord.Interaction):
        await self._do(interaction, "win")

    async def _lose(self, interaction: discord.Interaction):
        await self._do(interaction, "lose")

    async def _picked_multi(self, interaction: discord.Interaction):
        await self._do(interaction, self._select.values[0])

    async def _do(self, interaction: discord.Interaction, outcome: str):
        result = await service.resolve_bet(self.pool, self.guild, self.user, self.bet_id, outcome)
        # The bet is now closed, so the resolve + detail pages are stale; rebuild the
        # stack back to a fresh open-bets list (Back → Bets → Home).
        del self.hub.stack[1:]
        self.hub.stack.append(BetsPage(self.hub))
        self.hub.stack.append(BetsListPage(self.hub))
        await self.hub.refresh(interaction, notice=self._summary(result))

    def _summary(self, result) -> str:
        cur = self.currency
        if result.payouts:
            total = sum(p for _, _, p in result.payouts)
            return (f"🏁 Bet #{self.bet_id} resolved — {len(result.payouts)} winner(s) "
                    f"paid **{total}**{cur.emoji}.")
        return f"🏁 Bet #{self.bet_id} resolved — no winners, the host keeps the pool and stakes."


# ── modals ──

class StakeModal(HubModal):
    def __init__(self, hub, bet_id: int, *, option_idx, pop_first: bool = False):
        super().__init__(hub, title=f"Place a bet on #{bet_id}")
        self.bet_id = bet_id
        self.option_idx = option_idx
        self.pop_first = pop_first
        self.stake = discord.ui.TextInput(label="Stake", placeholder="e.g. 100, 5k", max_length=20)
        self.add_item(self.stake)

    async def on_submit(self, interaction: discord.Interaction):
        result = await service.place_take(
            self.hub.session.pool, self.hub.session.guild, self.hub.session.user,
            self.bet_id, option_idx=self.option_idx, raw_stake=self.stake.value)
        cur = self.hub.session.currency
        where = f" on **{result.option_label}**" if result.option_label else ""
        notice = (f"💰 Bet placed{where}: staked **{result.stake}**{cur.emoji}, "
                  f"wins **{result.payout}**{cur.emoji} if correct.")
        # Return to the bet's detail view with the updated pool/takes.
        if self.pop_first:
            self.hub.stack.pop()  # drop the option-picker page
        await self.hub.refresh(interaction, notice=notice)


class CreateBetModal(HubModal):
    def __init__(self, hub, *, bot_funded: bool = False):
        super().__init__(hub, title="Create a bot-funded bet" if bot_funded else "Create a bet")
        self.bot_funded = bot_funded
        self.odds = discord.ui.TextInput(label="Odds (e.g. 2.5 or 10)", placeholder="10", max_length=10)
        self.min_stake = discord.ui.TextInput(label="Min stake (optional)", placeholder="no limit",
                                              required=False, max_length=20)
        self.max_stake = discord.ui.TextInput(label="Max stake (optional)", placeholder="no limit",
                                              required=False, max_length=20)
        self.add_item(self.odds)
        self.add_item(self.min_stake)
        self.add_item(self.max_stake)
        # A bot-funded bet has no host pool — the bot covers every payout.
        if not bot_funded:
            self.pool = discord.ui.TextInput(label="Pool (your funded exposure)",
                                             placeholder="5000", max_length=20)
            self.add_item(self.pool)
        self.description = discord.ui.TextInput(
            label="Description", placeholder="Pens win tonight",
            style=discord.TextStyle.paragraph, required=False, max_length=200)
        self.add_item(self.description)

    async def on_submit(self, interaction: discord.Interaction):
        bet = await service.create_single_bet(
            self.hub.session.pool, self.hub.session.guild, self.hub.session.user,
            self.hub.session.channel_id,
            raw_odds=self.odds.value, raw_min=self.min_stake.value,
            raw_max=self.max_stake.value,
            raw_pool=None if self.bot_funded else self.pool.value,
            description=self.description.value, bot_funded=self.bot_funded,
            bot=self.hub.session.bot)
        tag = " (🤖 bot-funded)" if self.bot_funded else ""
        await self.hub.refresh(
            interaction,
            notice=f"➕ Created bet **#{bet['id']}** at x{service.format_odds(bet['odds'])}{tag}. "
                   f"It's now open under **View Open Bets**.")


class CreateMultiBetModal(HubModal):
    def __init__(self, hub, *, bot_funded: bool = False):
        super().__init__(hub, title="Create a bot-funded multi-bet" if bot_funded
                         else "Create a multi-option bet")
        self.bot_funded = bot_funded
        self.min_stake = discord.ui.TextInput(label="Min stake (optional)", placeholder="no limit",
                                              required=False, max_length=20)
        self.max_stake = discord.ui.TextInput(label="Max stake (optional)", placeholder="no limit",
                                              required=False, max_length=20)
        self.add_item(self.min_stake)
        self.add_item(self.max_stake)
        if not bot_funded:
            self.pool = discord.ui.TextInput(label="Pool (your funded exposure)",
                                             placeholder="5000", max_length=20)
            self.add_item(self.pool)
        self.description = discord.ui.TextInput(
            label="Description", placeholder="Tournament winner",
            style=discord.TextStyle.paragraph, required=False, max_length=200)
        self.options = discord.ui.TextInput(
            label="Options — one 'label x<odds>' per line",
            placeholder="Alice x3\nBob x2.5\nCarol x4",
            style=discord.TextStyle.paragraph, max_length=500)
        self.add_item(self.description)
        self.add_item(self.options)

    async def on_submit(self, interaction: discord.Interaction):
        bet, options = await service.create_multi_bet(
            self.hub.session.pool, self.hub.session.guild, self.hub.session.user,
            self.hub.session.channel_id,
            raw_min=self.min_stake.value, raw_max=self.max_stake.value,
            raw_pool=None if self.bot_funded else self.pool.value,
            description=self.description.value,
            raw_options=self.options.value, bot_funded=self.bot_funded,
            bot=self.hub.session.bot)
        tag = " (🤖 bot-funded)" if self.bot_funded else ""
        await self.hub.refresh(
            interaction,
            notice=f"🎯 Created multi-option bet **#{bet['id']}** with {len(options)} options{tag}. "
                   f"It's now open under **View Open Bets**.")
