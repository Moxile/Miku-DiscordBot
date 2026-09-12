from __future__ import annotations

import asyncio
import itertools
import secrets
import time
from dataclasses import dataclass, field

import discord

from cogs.economy.db import add_transaction, ensure_wallet, lock_wallet, update_wallet
from core.names import format_name


POKER_MAX_PLAYERS = 8
POKER_LOBBY_TIMEOUT = 60
POKER_TURN_TIMEOUT = 60
POKER_MAX_AMOUNT = 1_000_000_000_000_000
DEFAULT_SETTINGS = {
    "buyin": 1_000,
    "fee": 100,
    "smallblind": 50,
    "bigblind": 100,
}
SETTING_KEYS = {name: f"poker_{name}" for name in DEFAULT_SETTINGS}

RANK_VALUE = {rank: value for value, rank in enumerate("23456789TJQKA", start=2)}
RANK_LABEL = {"T": "10"}


def card_text(card: tuple[str, str]) -> str:
    rank, suit = card
    return f"{RANK_LABEL.get(rank, rank)}{suit}"


def shuffled_deck() -> list[tuple[str, str]]:
    deck = [(rank, suit) for suit in ("♠️", "♥️", "♦️", "♣️") for rank in "23456789TJQKA"]
    secrets.SystemRandom().shuffle(deck)
    return deck


def evaluate_five(cards: tuple[tuple[str, str], ...]) -> tuple[int, ...]:
    """Return a directly comparable five-card poker rank."""
    values = sorted((RANK_VALUE[c[0]] for c in cards), reverse=True)
    counts = {value: values.count(value) for value in set(values)}
    groups = sorted(((count, value) for value, count in counts.items()), reverse=True)
    flush = len({c[1] for c in cards}) == 1
    unique = sorted(set(values), reverse=True)
    if 14 in unique:
        unique.append(1)
    straight_high = next(
        (unique[i] for i in range(len(unique) - 4) if unique[i] - unique[i + 4] == 4),
        None,
    )

    if flush and straight_high:
        return (8, straight_high)
    if groups[0][0] == 4:
        four = groups[0][1]
        return (7, four, max(v for v in values if v != four))
    triples = sorted((v for v, count in counts.items() if count == 3), reverse=True)
    pairs = sorted((v for v, count in counts.items() if count == 2), reverse=True)
    if triples and (len(triples) > 1 or pairs):
        return (6, triples[0], triples[1] if len(triples) > 1 else pairs[0])
    if flush:
        return (5, *values)
    if straight_high:
        return (4, straight_high)
    if triples:
        kickers = sorted((v for v in values if v != triples[0]), reverse=True)
        return (3, triples[0], *kickers)
    if len(pairs) >= 2:
        high, low = pairs[:2]
        return (2, high, low, max(v for v in values if v not in (high, low)))
    if pairs:
        kickers = sorted((v for v in values if v != pairs[0]), reverse=True)
        return (1, pairs[0], *kickers)
    return (0, *values)


def evaluate_hand(cards: list[tuple[str, str]]) -> tuple[int, ...]:
    if len(cards) < 5:
        raise ValueError("a poker hand needs at least five cards")
    return max(evaluate_five(combo) for combo in itertools.combinations(cards, 5))


HAND_NAMES = {
    8: "straight flush",
    7: "four of a kind",
    6: "full house",
    5: "flush",
    4: "straight",
    3: "three of a kind",
    2: "two pair",
    1: "one pair",
    0: "high card",
}


@dataclass
class PokerPlayer:
    user_id: int
    stack: int
    ready: bool = True
    playing: bool = False
    hand: list[tuple[str, str]] = field(default_factory=list)
    folded: bool = False
    all_in: bool = False
    street_bet: int = 0
    committed: int = 0
    hand_start_stack: int = 0


class RaiseModal(discord.ui.Modal, title="Raise"):
    amount = discord.ui.TextInput(
        label="Raise to (total for this betting round)",
        placeholder="Enter a chip amount",
        min_length=1,
        max_length=18,
    )

    def __init__(self, table: "PokerTable", version: int):
        super().__init__()
        self.table = table
        self.version = version

    async def on_submit(self, interaction: discord.Interaction):
        try:
            amount = int(self.amount.value.strip().replace(",", ""))
        except ValueError:
            await interaction.response.send_message("Enter a whole number of chips.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        error = await self.table.take_action(interaction.user.id, "raise", amount, self.version)
        if error:
            await interaction.followup.send(error, ephemeral=True)
        else:
            await interaction.followup.send("Raise accepted.", ephemeral=True)


class PokerLobbyView(discord.ui.View):
    def __init__(self, table: "PokerTable"):
        super().__init__(timeout=None)
        self.table = table
        self.version = table.version

    @discord.ui.button(label="Play Again", emoji="✅", style=discord.ButtonStyle.success)
    async def ready(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        error = await self.table.mark_ready(interaction.user.id, self.version)
        if error:
            await interaction.followup.send(error, ephemeral=True)
        else:
            await interaction.followup.send("You're ready for the next hand.", ephemeral=True)

    @discord.ui.button(label="Start Now", emoji="▶️", style=discord.ButtonStyle.primary)
    async def start(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        error = await self.table.request_start(interaction.user.id, self.version)
        if error:
            await interaction.followup.send(error, ephemeral=True)

    @discord.ui.button(label="Cash Out", emoji="🚪", style=discord.ButtonStyle.secondary)
    async def cash_out(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        amount, error = await self.table.cash_out(interaction.user.id, self.version)
        if error:
            await interaction.followup.send(error, ephemeral=True)
        else:
            cur = self.table.cog.bot.get_currency(self.table.guild_id)
            await interaction.followup.send(
                f"Cashed out **{amount:,}**{cur.emoji}.", ephemeral=True
            )


class PokerActionView(discord.ui.View):
    def __init__(self, table: "PokerTable"):
        super().__init__(timeout=None)
        self.table = table
        self.version = table.version

    async def _act(self, interaction: discord.Interaction, action: str):
        await interaction.response.defer(ephemeral=True)
        error = await self.table.take_action(interaction.user.id, action, None, self.version)
        if error:
            await interaction.followup.send(error, ephemeral=True)

    @discord.ui.button(label="View Cards", emoji="🃏", style=discord.ButtonStyle.secondary, row=0)
    async def cards(self, interaction: discord.Interaction, button: discord.ui.Button):
        player = self.table.get_player(interaction.user.id)
        if player is None or not player.playing:
            await interaction.response.send_message("You aren't playing in this hand.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Your cards: **" + "  ".join(card_text(c) for c in player.hand) + "**",
            ephemeral=True,
        )

    @discord.ui.button(label="Check / Call", emoji="✅", style=discord.ButtonStyle.success, row=0)
    async def check_call(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._act(interaction, "call")

    @discord.ui.button(label="Raise", emoji="⬆️", style=discord.ButtonStyle.primary, row=0)
    async def raise_bet(self, interaction: discord.Interaction, button: discord.ui.Button):
        error = self.table.action_error(interaction.user.id, self.version)
        if error:
            await interaction.response.send_message(error, ephemeral=True)
            return
        player = self.table.get_player(interaction.user.id)
        if player.user_id in self.table.acted_since_raise:
            await interaction.response.send_message(
                "Betting was not reopened by the last short all-in; you may only call or fold.",
                ephemeral=True,
            )
            return
        await interaction.response.send_modal(RaiseModal(self.table, self.version))

    @discord.ui.button(label="All In", emoji="🔥", style=discord.ButtonStyle.danger, row=1)
    async def all_in(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._act(interaction, "allin")

    @discord.ui.button(label="Fold", emoji="🏳️", style=discord.ButtonStyle.secondary, row=1)
    async def fold(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._act(interaction, "fold")


class PokerTable:
    def __init__(self, manager: "PokerManager", channel, host_id: int, settings: dict[str, int]):
        self.manager = manager
        self.cog = manager.cog
        self.channel = channel
        self.guild_id = channel.guild.id
        self.channel_id = channel.id
        self.host_id = host_id
        self.settings = settings
        self.players: list[PokerPlayer] = []
        self.message = None
        self.phase = "lobby"
        self.first_hand = True
        self.version = 0
        self.deadline = time.time() + POKER_LOBBY_TIMEOUT
        self.dealer_id: int | None = None
        self.community: list[tuple[str, str]] = []
        self.deck: list[tuple[str, str]] = []
        self.current_bet = 0
        self.min_raise = settings["bigblind"]
        self.actor_id: int | None = None
        self.pending: set[int] = set()
        self.acted_since_raise: set[int] = set()
        self.last_result = ""
        self.lock = asyncio.Lock()
        self.timer_task: asyncio.Task | None = None

    def get_player(self, user_id: int) -> PokerPlayer | None:
        return next((p for p in self.players if p.user_id == user_id), None)

    def active(self) -> list[PokerPlayer]:
        return [p for p in self.players if p.playing]

    def contenders(self) -> list[PokerPlayer]:
        return [p for p in self.players if p.playing and not p.folded]

    def actionable(self) -> list[PokerPlayer]:
        return [p for p in self.contenders() if not p.all_in]

    def ordered_after(self, user_id: int | None, *, candidates=None) -> list[PokerPlayer]:
        if not self.players:
            return []
        start = next((i for i, p in enumerate(self.players) if p.user_id == user_id), -1)
        ordered = self.players[start + 1:] + self.players[:start + 1]
        return [p for p in ordered if candidates is None or p.user_id in candidates]

    def next_player(self, after_id: int | None, candidates: set[int]) -> PokerPlayer | None:
        return next(iter(self.ordered_after(after_id, candidates=candidates)), None)

    async def send_initial(self):
        async with self.lock:
            if self.message is not None:
                return
            self.message = await self.channel.send(embed=self.build_embed(), view=PokerLobbyView(self))
            self._start_lobby_timer()

    def _start_lobby_timer(self):
        self._cancel_timer()
        version = self.version
        self.timer_task = asyncio.create_task(self._lobby_timeout(version))

    async def _lobby_timeout(self, version: int):
        try:
            await asyncio.sleep(max(0, self.deadline - time.time()))
            if self.version == version and self.phase == "lobby":
                await self.begin_hand()
        except asyncio.CancelledError:
            return

    def _start_turn_timer(self):
        self._cancel_timer()
        version, actor = self.version, self.actor_id
        self.timer_task = asyncio.create_task(self._turn_timeout(version, actor))

    def _cancel_timer(self):
        if self.timer_task and self.timer_task is not asyncio.current_task():
            self.timer_task.cancel()

    async def _turn_timeout(self, version: int, actor: int | None):
        try:
            await asyncio.sleep(POKER_TURN_TIMEOUT)
            async with self.lock:
                if self.version != version or self.phase != "betting" or self.actor_id != actor:
                    return
                player = self.get_player(actor)
                due = self.current_bet - player.street_bet
                await self._apply_action(player, "call" if due == 0 else "fold", None)
        except asyncio.CancelledError:
            return

    async def mark_ready(self, user_id: int, version: int) -> str | None:
        async with self.lock:
            if version != self.version or self.phase != "lobby":
                return "That lobby is no longer active."
            player = self.get_player(user_id)
            if player is None:
                return "Join with `.poker <amount>` first."
            if player.stack <= 0:
                return "You have no chips left. Cash out, then buy in again."
            player.ready = True
            await self.refresh()
            return None

    async def request_start(self, user_id: int, version: int) -> str | None:
        if user_id != self.host_id:
            return "Only the table host can start the hand early."
        if version != self.version or self.phase != "lobby":
            return "That lobby is no longer active."
        ready_others = sum(
            p.ready and p.stack > 0 for p in self.players if p.user_id != user_id
        )
        if ready_others < 1:
            return "At least two ready players are required to start a hand."
        await self.begin_hand(force_host_id=user_id)
        return None

    async def begin_hand(self, force_host_id: int | None = None):
        async with self.lock:
            if self.phase != "lobby":
                return
            if force_host_id is not None:
                host = self.get_player(force_host_id)
                if host and host.stack > 0:
                    host.ready = True
            # The replay window is opt-in: anyone who did not press Play Again
            # (and anyone who busted) leaves automatically when it closes.
            departing = [p for p in self.players if not p.ready or p.stack <= 0]
            for player in departing:
                await self.manager.release_escrow(self.guild_id, player.user_id)
                self.players.remove(player)
            if self.host_id not in {p.user_id for p in self.players} and self.players:
                self.host_id = self.players[0].user_id

            ready = [p for p in self.players if p.ready and p.stack > 0]
            payable: list[PokerPlayer] = []
            fee = self.settings["fee"]
            async with self.cog.pool.acquire() as conn:
                async with conn.transaction():
                    for player in ready:
                        await ensure_wallet(conn, self.guild_id, player.user_id)
                        wallet = await lock_wallet(conn, self.guild_id, player.user_id)
                        if wallet["wallet"] >= fee:
                            payable.append(player)
                    if len(payable) >= 2:
                        for player in payable:
                            if fee:
                                await update_wallet(conn, self.guild_id, player.user_id, -fee)
                                await conn.execute(
                                    "UPDATE poker_escrow SET fee_paid = $3 WHERE guild_id = $1 AND user_id = $2",
                                    self.guild_id, player.user_id, fee,
                                )

            if len(payable) < 2:
                self.last_result = "Not enough ready players could pay the hand fee. Table closed."
                await self._close_and_cash_all()
                return

            cannot_pay = [p for p in ready if p not in payable]
            for player in cannot_pay:
                await self.manager.release_escrow(self.guild_id, player.user_id)
                self.players.remove(player)
            if self.host_id not in {p.user_id for p in self.players}:
                self.host_id = self.players[0].user_id

            for player in self.players:
                player.ready = False
                player.playing = player in payable
                player.hand = []
                player.folded = False
                player.all_in = False
                player.street_bet = 0
                player.committed = 0
                player.hand_start_stack = player.stack

            self.phase = "betting"
            self.version += 1
            self.community = []
            self.deck = shuffled_deck()
            self.last_result = ""
            for _ in range(2):
                for player in self.active():
                    player.hand.append(self.deck.pop())

            active_ids = {p.user_id for p in self.active()}
            if self.dealer_id not in active_ids:
                self.dealer_id = secrets.choice(tuple(active_ids))
            elif not self.first_hand:
                self.dealer_id = self.next_player(self.dealer_id, active_ids).user_id
            self.first_hand = False

            active = self.active()
            if len(active) == 2:
                small = self.get_player(self.dealer_id)
                big = self.next_player(small.user_id, active_ids)
            else:
                small = self.next_player(self.dealer_id, active_ids)
                big = self.next_player(small.user_id, active_ids)
            self._post_blind(small, self.settings["smallblind"])
            self._post_blind(big, self.settings["bigblind"])
            # A short all-in big blind does not lower the table's pre-flop bring-in.
            self.current_bet = self.settings["bigblind"]
            self.min_raise = self.settings["bigblind"]
            self.pending = {p.user_id for p in self.actionable()}
            self.acted_since_raise = set()
            self.actor_id = self.next_player(big.user_id, self.pending).user_id if self.pending else None
            await self.refresh(publish=True)
            if self.actor_id is None:
                await self._advance_street()
            else:
                self._start_turn_timer()

    def _post_blind(self, player: PokerPlayer, amount: int):
        paid = min(player.stack, amount)
        player.stack -= paid
        player.street_bet += paid
        player.committed += paid
        player.all_in = player.stack == 0

    def action_error(self, user_id: int, version: int) -> str | None:
        if version != self.version or self.phase != "betting":
            return "That hand is no longer accepting actions."
        player = self.get_player(user_id)
        if player is None or not player.playing or player.folded or player.all_in:
            return "You cannot act in this hand."
        if self.actor_id != user_id:
            actor = self.channel.guild.get_member(self.actor_id)
            return f"It is {format_name(actor, self.channel.guild, fallback=str(self.actor_id))}'s turn."
        return None

    async def take_action(self, user_id: int, action: str, amount: int | None, version: int) -> str | None:
        async with self.lock:
            error = self.action_error(user_id, version)
            if error:
                return error
            player = self.get_player(user_id)
            return await self._apply_action(player, action, amount)

    async def _apply_action(self, player: PokerPlayer, action: str, amount: int | None) -> str | None:
        due = max(0, self.current_bet - player.street_bet)

        if action == "fold":
            player.folded = True
            self.pending.discard(player.user_id)
            self.acted_since_raise.add(player.user_id)
        elif action == "call":
            paid = min(player.stack, due)
            player.stack -= paid
            player.street_bet += paid
            player.committed += paid
            player.all_in = player.stack == 0
            self.pending.discard(player.user_id)
            # A check does not lose its right to raise if the first wager on the
            # street is a short all-in.
            if due:
                self.acted_since_raise.add(player.user_id)
        else:
            target = player.street_bet + player.stack if action == "allin" else amount
            if action == "allin" and target <= self.current_bet:
                paid = player.stack
                player.stack = 0
                player.street_bet += paid
                player.committed += paid
                player.all_in = True
                self.pending.discard(player.user_id)
                self.acted_since_raise.add(player.user_id)
                target = None
            elif target is None or target <= self.current_bet:
                return f"A raise must be above the current bet of {self.current_bet:,}."
            if target is None:
                pass
            elif target > player.street_bet + player.stack:
                return "You don't have that many chips."
            elif target - self.current_bet < self.min_raise and target != player.street_bet + player.stack:
                return f"The minimum raise-to amount is {self.current_bet + self.min_raise:,}."
            elif player.user_id in self.acted_since_raise:
                return "Betting has not been reopened; you may only call or fold."
            else:
                raise_size = target - self.current_bet
                paid = target - player.street_bet
                player.stack -= paid
                player.street_bet = target
                player.committed += paid
                player.all_in = player.stack == 0
                self.current_bet = target
                if raise_size >= self.min_raise:
                    self.min_raise = raise_size
                    self.pending = {p.user_id for p in self.actionable() if p.user_id != player.user_id}
                    self.acted_since_raise = {player.user_id}
                else:
                    self.pending.discard(player.user_id)
                    self.pending.update(
                        p.user_id for p in self.actionable()
                        if p.user_id != player.user_id and p.street_bet < self.current_bet
                    )
                    self.acted_since_raise.add(player.user_id)

        if len(self.contenders()) == 1:
            await self._settle_uncontested()
            return None

        self.pending.intersection_update(p.user_id for p in self.actionable())
        # A player remains pending until they have matched the bet (unless all-in).
        self.pending.update(
            p.user_id for p in self.actionable()
            if p.street_bet < self.current_bet
        )
        if not self.pending:
            await self._advance_street()
            return None

        self.actor_id = self.next_player(player.user_id, self.pending).user_id
        await self.refresh(publish=True)
        self._start_turn_timer()
        return None

    async def _advance_street(self):
        if len(self.community) == 0:
            self.deck.pop()  # burn
            self.community.extend((self.deck.pop(), self.deck.pop(), self.deck.pop()))
        elif len(self.community) < 5:
            self.deck.pop()
            self.community.append(self.deck.pop())
        else:
            await self._showdown()
            return

        for player in self.active():
            player.street_bet = 0
        self.current_bet = 0
        self.min_raise = self.settings["bigblind"]
        self.pending = {p.user_id for p in self.actionable()}
        self.acted_since_raise = set()
        if len(self.actionable()) <= 1:
            while len(self.community) < 5:
                self.deck.pop()
                self.community.append(self.deck.pop())
            await self._showdown()
            return
        self.actor_id = self.next_player(self.dealer_id, self.pending).user_id
        await self.refresh(publish=True)
        self._start_turn_timer()

    async def _settle_uncontested(self):
        winner = self.contenders()[0]
        pot = sum(p.committed for p in self.active())
        winner.stack += pot
        member = self.channel.guild.get_member(winner.user_id)
        self.last_result = f"{format_name(member, self.channel.guild, fallback=str(winner.user_id))} wins {pot:,} chips (everyone else folded)."
        await self._finish_hand()

    async def _showdown(self):
        ranks = {p.user_id: evaluate_hand(p.hand + self.community) for p in self.contenders()}
        payouts = {p.user_id: 0 for p in self.active()}
        levels = sorted({p.committed for p in self.active() if p.committed > 0})
        previous = 0
        result_parts = []
        for level in levels:
            contributors = [p for p in self.active() if p.committed >= level]
            pot = (level - previous) * len(contributors)
            eligible = [p for p in contributors if not p.folded]
            if not eligible:
                previous = level
                continue
            best = max(ranks[p.user_id] for p in eligible)
            winners = [p for p in eligible if ranks[p.user_id] == best]
            share, odd = divmod(pot, len(winners))
            ordered_winners = self.ordered_after(self.dealer_id, candidates={p.user_id for p in winners})
            for index, winner in enumerate(ordered_winners):
                payouts[winner.user_id] += share + (1 if index < odd else 0)
            names = ", ".join(
                format_name(self.channel.guild.get_member(p.user_id), self.channel.guild, fallback=str(p.user_id))
                for p in winners
            )
            result_parts.append(f"{names} win {pot:,} with {HAND_NAMES[best[0]]}")
            previous = level
        for player in self.active():
            player.stack += payouts[player.user_id]
        self.last_result = "; ".join(result_parts) + "."
        await self._finish_hand()

    async def _finish_hand(self):
        self._cancel_timer()
        async with self.cog.pool.acquire() as conn:
            async with conn.transaction():
                for player in self.active():
                    net = player.stack - player.hand_start_stack
                    await conn.execute(
                        "UPDATE poker_escrow SET chips = $3, fee_paid = 0 WHERE guild_id = $1 AND user_id = $2",
                        self.guild_id, player.user_id, player.stack,
                    )
                    result = net - self.settings["fee"]
                    tx_type = "poker_win" if result > 0 else ("poker_loss" if result < 0 else "poker_push")
                    await add_transaction(
                        conn, self.guild_id, player.user_id, result, tx_type,
                        f"Poker hand in channel {self.channel_id}",
                    )
        for player in self.players:
            player.playing = False
            player.ready = False
        self.phase = "lobby"
        self.actor_id = None
        self.version += 1
        self.deadline = time.time() + POKER_LOBBY_TIMEOUT
        await self.refresh(publish=True)
        self._start_lobby_timer()

    async def cash_out(self, user_id: int, version: int) -> tuple[int, str | None]:
        async with self.lock:
            if version != self.version or self.phase != "lobby":
                return 0, "You can only cash out between hands."
            player = self.get_player(user_id)
            if player is None:
                return 0, "You aren't seated at this table."
            amount = await self.manager.release_escrow(self.guild_id, user_id)
            self.players.remove(player)
            if self.host_id == user_id and self.players:
                self.host_id = self.players[0].user_id
            if not self.players:
                await self.close()
            else:
                await self.refresh()
            return amount, None

    async def _close_and_cash_all(self):
        self._cancel_timer()
        for player in list(self.players):
            await self.manager.release_escrow(self.guild_id, player.user_id)
        self.players.clear()
        await self.close()

    async def close(self):
        self.phase = "closed"
        self.manager.tables.pop((self.guild_id, self.channel_id), None)
        self._cancel_timer()
        await self.refresh()

    async def refresh(self, *, publish: bool = False):
        if self.message is None:
            return
        view = None
        if self.phase == "lobby":
            view = PokerLobbyView(self)
        elif self.phase == "betting":
            view = PokerActionView(self)
        embed = self.build_embed()
        if publish:
            previous = self.message
            try:
                self.message = await self.channel.send(embed=embed, view=view)
            except discord.HTTPException:
                # Keep the current controls usable if Discord rejects the new post.
                try:
                    await previous.edit(embed=embed, view=view)
                except discord.HTTPException:
                    pass
                return
            try:
                await previous.edit(view=None)
            except discord.HTTPException:
                pass
            return
        try:
            await self.message.edit(embed=embed, view=view)
        except discord.HTTPException:
            pass

    def build_embed(self) -> discord.Embed:
        cur = self.cog.bot.get_currency(self.guild_id)
        title = "Texas Hold'em — " + {
            "lobby": "Lobby",
            "betting": "Hand in Progress",
            "closed": "Table Closed",
        }[self.phase]
        embed = discord.Embed(title=title, color=discord.Color.dark_green())
        board = "  ".join(card_text(c) for c in self.community) or "No community cards yet"
        pot = sum(p.committed for p in self.active())
        lines = []
        for player in self.players:
            member = self.channel.guild.get_member(player.user_id)
            name = format_name(member, self.channel.guild, fallback=str(player.user_id))
            flags = []
            if player.user_id == self.host_id:
                flags.append("host")
            if player.user_id == self.dealer_id and player.playing:
                flags.append("dealer")
            if player.folded:
                flags.append("folded")
            elif player.all_in:
                flags.append("all-in")
            elif self.phase == "lobby":
                flags.append("ready" if player.ready else "not ready")
            if player.user_id == self.actor_id:
                flags.append("turn")
            suffix = f" — {', '.join(flags)}" if flags else ""
            wager = f" (+{player.street_bet:,})" if player.street_bet else ""
            lines.append(f"**{name}**: {player.stack:,}{wager} chips{suffix}")
        embed.description = f"**Board:** {board}\n**Pot:** {pot:,} chips\n\n" + ("\n".join(lines) or "No players seated.")
        if self.phase == "lobby" and self.players:
            embed.add_field(
                name="Next hand",
                value=f"Starts <t:{int(self.deadline)}:R> · Join with `.poker <amount>`",
                inline=False,
            )
        if self.phase == "betting" and self.actor_id is not None:
            due = self.current_bet - self.get_player(self.actor_id).street_bet
            embed.add_field(
                name="Action",
                value=f"Current bet: **{self.current_bet:,}** · To call: **{due:,}** · Turn expires in {POKER_TURN_TIMEOUT}s",
                inline=False,
            )
        if self.last_result:
            embed.add_field(name="Last hand", value=self.last_result[:1024], inline=False)
        embed.set_footer(
            text=(f"Min buy-in {self.settings['buyin']:,} · Fee {self.settings['fee']:,}{cur.name}/hand · "
                  f"Blinds {self.settings['smallblind']:,}/{self.settings['bigblind']:,}")
        )
        return embed


class PokerManager:
    def __init__(self, cog):
        self.cog = cog
        self.tables: dict[tuple[int, int], PokerTable] = {}
        self.lock = asyncio.Lock()

    def shutdown(self):
        """Stop live timers; escrow is refunded by recover_escrow on next load."""
        for table in self.tables.values():
            table._cancel_timer()
            table.phase = "closed"
        self.tables.clear()

    async def recover_escrow(self) -> int:
        """Refund tables left behind by a previous process."""
        async with self.cog.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    "SELECT guild_id, user_id, chips, fee_paid FROM poker_escrow FOR UPDATE"
                )
                for row in rows:
                    await ensure_wallet(conn, row["guild_id"], row["user_id"])
                    await update_wallet(
                        conn, row["guild_id"], row["user_id"], row["chips"] + row["fee_paid"]
                    )
                await conn.execute("DELETE FROM poker_escrow")
        return len(rows)

    async def settings_for(self, guild_id: int) -> dict[str, int]:
        rows = await self.cog.pool.fetch(
            "SELECT key, value FROM guild_settings WHERE guild_id = $1 AND key = ANY($2)",
            guild_id, list(SETTING_KEYS.values()),
        )
        stored = {row["key"]: int(row["value"]) for row in rows}
        return {name: stored.get(key, DEFAULT_SETTINGS[name]) for name, key in SETTING_KEYS.items()}

    async def join(self, channel, user_id: int, buyin: int) -> tuple[PokerTable | None, str | None]:
        async with self.lock:
            key = (channel.guild.id, channel.id)
            table = self.tables.get(key)
            settings = table.settings if table else await self.settings_for(channel.guild.id)
            if table is None:
                table = PokerTable(self, channel, user_id, settings)
            async with table.lock:
                if buyin < settings["buyin"]:
                    return None, f"The minimum poker buy-in is {settings['buyin']:,}."
                if buyin > POKER_MAX_AMOUNT:
                    return None, f"The maximum supported poker buy-in is {POKER_MAX_AMOUNT:,}."
                if table.phase != "lobby":
                    return None, "That table is in the middle of a hand. Join when it finishes."
                if table.get_player(user_id):
                    return None, "You are already seated at this table."
                if len(table.players) >= POKER_MAX_PLAYERS:
                    return None, "That poker table is full (8/8)."

                async with self.cog.pool.acquire() as conn:
                    async with conn.transaction():
                        await ensure_wallet(conn, channel.guild.id, user_id)
                        wallet = await lock_wallet(conn, channel.guild.id, user_id)
                        existing = await conn.fetchrow(
                            "SELECT channel_id FROM poker_escrow WHERE guild_id = $1 AND user_id = $2",
                            channel.guild.id, user_id,
                        )
                        if existing:
                            return None, "You already have chips at another poker table in this server."
                        total_needed = buyin + settings["fee"]
                        if wallet["wallet"] < total_needed:
                            return None, (
                                f"You need {total_needed:,} in your wallet for that buy-in "
                                "and the first hand fee."
                            )
                        await update_wallet(conn, channel.guild.id, user_id, -buyin)
                        await conn.execute(
                            "INSERT INTO poker_escrow (guild_id, channel_id, user_id, chips) VALUES ($1, $2, $3, $4)",
                            channel.guild.id, channel.id, user_id, buyin,
                        )

                self.tables.setdefault(key, table)
                table.players.append(PokerPlayer(user_id, buyin))
                await table.refresh()
                return table, None

    async def release_escrow(self, guild_id: int, user_id: int) -> int:
        async with self.cog.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "DELETE FROM poker_escrow WHERE guild_id = $1 AND user_id = $2 RETURNING chips",
                    guild_id, user_id,
                )
                amount = row["chips"] if row else 0
                if amount:
                    await ensure_wallet(conn, guild_id, user_id)
                    await update_wallet(conn, guild_id, user_id, amount)
                return amount
