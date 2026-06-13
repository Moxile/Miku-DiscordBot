import asyncio
import secrets
import time

import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, update_bank, add_transaction
from core.checks import require_channel, WrongChannel, invalidate, UserLocked, user_is_locked
from core.money import parse_amount, AmountError
from config import MAIN_CURRENCY_EMOJI, CURRENCY_NAME, PREFIX
from . import cards, coins, wheel, board


BLACKJACK_TIMEOUT = 120
BLACKJACK_SHOE_DECKS = 6  # number of 52-card decks in each guild's shared shoe


def _card_value(rank):
    return 10 if rank in ("10", "J", "Q", "K") else rank


def _is_pair(hand):
    return len(hand) == 2 and _card_value(hand[0][0]) == _card_value(hand[1][0])


def _result_meta(net):
    """Map a net outcome to an embed title + color."""
    if net > 0:
        return "Win", discord.Color.green()
    if net < 0:
        return "Loss", discord.Color.red()
    return "Push", discord.Color.dark_gray()


class BlackjackView(discord.ui.View):
    """Button-driven controls for a single blackjack game.

    Holds only a reference to the cog and the game key; all state lives in
    ``cog.games[key]`` and all money/rendering goes through cog helpers.
    """

    def __init__(self, cog, key, *, timeout=BLACKJACK_TIMEOUT):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.key = key
        self.player_id = key[1]
        self.message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return False
        return True

    async def _sync_buttons(self, game):
        """Grey out Double/Split when the action isn't available."""
        wallet = await ensure_wallet(self.cog.pool, *self.key)
        balance = wallet["wallet"]
        hand = game["player_hands"][game["current_hand"]]
        two_cards = len(hand) == 2
        self.double_btn.disabled = not (two_cards and balance >= game["bet"])
        self.split_btn.disabled = not (two_cards and _is_pair(hand) and balance >= game["bet"])

    async def _show_turn(self, interaction, title, color):
        game = self.cog.games[self.key]
        await self._sync_buttons(game)
        file, embed = await self.cog.build_state(game, title=title, color=color, hide_dealer=True)
        await interaction.response.edit_message(attachments=[file], embed=embed, view=self)

    async def _finish(self, interaction, game, net):
        for child in self.children:
            child.disabled = True
        title, color = _result_meta(net)
        file, embed = await self.cog.build_state(game, title=title, color=color, hide_dealer=False, result=net)
        await interaction.response.edit_message(attachments=[file], embed=embed, view=self)
        self.stop()

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.success)
    async def hit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = self.cog.games.get(self.key)
        if game is None or game["state"] != "player_turn":
            await interaction.response.defer()
            return
        busted = self.cog.deal_to_current(game)
        if not busted:
            await self._show_turn(interaction, "Hit", discord.Color.green())
            return
        phase, net = await self.cog.advance_hand(self.key, game)
        if phase == "continue":
            await self._show_turn(interaction, "Busted — next hand", discord.Color.orange())
        else:
            await self._finish(interaction, game, net)

    @discord.ui.button(label="Stand", style=discord.ButtonStyle.danger)
    async def stand_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = self.cog.games.get(self.key)
        if game is None or game["state"] != "player_turn":
            await interaction.response.defer()
            return
        if game["current_hand"] < len(game["player_hands"]) - 1:
            game["current_hand"] += 1
            await self._show_turn(interaction, f"Stand — Hand {game['current_hand'] + 1}", discord.Color.blue())
            return
        game["state"] = "dealer_turn"
        net = await self.cog.settle(self.key, game)
        await self._finish(interaction, game, net)

    @discord.ui.button(label="Double", style=discord.ButtonStyle.primary)
    async def double_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = self.cog.games.get(self.key)
        if game is None or game["state"] != "player_turn":
            await interaction.response.defer()
            return
        if len(game["player_hands"][game["current_hand"]]) != 2:
            await interaction.response.defer()
            return
        busted = await self.cog.double_current(self.key, game)
        if busted is None:
            await interaction.response.send_message(f"You don't have enough {CURRENCY_NAME} to double.", ephemeral=True)
            return
        phase, net = await self.cog.advance_hand(self.key, game)
        if phase == "continue":
            await self._show_turn(interaction, "Double Down — next hand", discord.Color.orange())
        else:
            await self._finish(interaction, game, net)

    @discord.ui.button(label="Split", style=discord.ButtonStyle.secondary)
    async def split_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = self.cog.games.get(self.key)
        if game is None or game["state"] != "player_turn":
            await interaction.response.defer()
            return
        ok = await self.cog.split_current(self.key, game)
        if not ok:
            await interaction.response.send_message(
                f"You can't split that hand (need a matching pair and enough {CURRENCY_NAME}).", ephemeral=True
            )
            return
        await self._show_turn(interaction, "Split — Hand 1", discord.Color.blue())

    async def on_timeout(self):
        game = self.cog.games.get(self.key)
        if game is None or self.message is None:
            return
        game["state"] = "dealer_turn"
        net = await self.cog.settle(self.key, game)
        for child in self.children:
            child.disabled = True
        title, color = _result_meta(net)
        file, embed = await self.cog.build_state(
            game, title=f"Timed out — {title}", color=color, hide_dealer=False, result=net
        )
        try:
            await self.message.edit(attachments=[file], embed=embed, view=self)
        except discord.HTTPException:
            pass


# ── Roulette ──

ROULETTE_WINDOW = 30  # seconds until the wheel spins; each new bet pushes the deadline back
OUTSIDE_BETS = [
    ("red", "Red"), ("black", "Black"), ("odd", "Odd"), ("even", "Even"),
    ("low", "Low (1–18)"), ("high", "High (19–36)"),
    ("dozen1", "1st Dozen (1–12)"), ("dozen2", "2nd Dozen (13–24)"), ("dozen3", "3rd Dozen (25–36)"),
    ("col1", "Column 1"), ("col2", "Column 2"), ("col3", "Column 3"),
]
_OUTSIDE_LABELS = dict(OUTSIDE_BETS)

# Which row each outside-bet button sits on in a player's panel, and its button colour.
_BET_ROWS = {
    "red": 0, "black": 0, "odd": 0, "even": 0,
    "low": 1, "high": 1,
    "dozen1": 2, "dozen2": 2, "dozen3": 2,
    "col1": 3, "col2": 3, "col3": 3,
}
_BET_STYLES = {
    "red": discord.ButtonStyle.danger,
    "black": discord.ButtonStyle.secondary,
}


def _bet_label(choice):
    if choice.isdigit():
        return f"Number {choice}"
    return _OUTSIDE_LABELS.get(choice, choice.title())


class RouletteBetModal(discord.ui.Modal):
    """Asks the stake for a single outside bet, then places it."""
    amount = discord.ui.TextInput(label="Amount", placeholder="e.g. 250 or 'all'", max_length=20)

    def __init__(self, cog, key, choice):
        super().__init__(title=f"Bet on {_bet_label(choice)}"[:45])
        self.cog = cog
        self.key = key
        self.choice = choice

    async def on_submit(self, interaction: discord.Interaction):
        await self.cog.place_roulette_bet(interaction, self.key, self.choice, str(self.amount))


class RouletteNumberModal(discord.ui.Modal, title="Straight-up number bet"):
    """Asks for a single number (0–36) and the stake, then places the bet."""
    number = discord.ui.TextInput(label="Number (0–36)", placeholder="e.g. 17", max_length=2)
    amount = discord.ui.TextInput(label="Amount", placeholder="e.g. 250 or 'all'", max_length=20)

    def __init__(self, cog, key):
        super().__init__()
        self.cog = cog
        self.key = key

    async def on_submit(self, interaction: discord.Interaction):
        raw = str(self.number).strip()
        if not raw.isdigit() or not (0 <= int(raw) <= 36):
            await interaction.response.send_message("Please enter a whole number from 0 to 36.", ephemeral=True)
            return
        await self.cog.place_roulette_bet(interaction, self.key, str(int(raw)), str(self.amount))


class _OutsideBetButton(discord.ui.Button):
    def __init__(self, cog, key, choice, label):
        super().__init__(label=label, row=_BET_ROWS[choice],
                         style=_BET_STYLES.get(choice, discord.ButtonStyle.primary))
        self.cog = cog
        self.key = key
        self.choice = choice

    async def callback(self, interaction: discord.Interaction):
        if self.cog.roulette_closed(self.key):
            await interaction.response.send_message("This roulette table has already closed.", ephemeral=True)
            return
        await interaction.response.send_modal(RouletteBetModal(self.cog, self.key, self.choice))


class _NumberBetButton(discord.ui.Button):
    def __init__(self, cog, key):
        super().__init__(label="Number…", emoji="🔢", style=discord.ButtonStyle.primary, row=4)
        self.cog = cog
        self.key = key

    async def callback(self, interaction: discord.Interaction):
        if self.cog.roulette_closed(self.key):
            await interaction.response.send_message("This roulette table has already closed.", ephemeral=True)
            return
        await interaction.response.send_modal(RouletteNumberModal(self.cog, self.key))


class RoulettePanelView(discord.ui.View):
    """A player's private (ephemeral) betting panel: one button per outside bet plus a
    number button. Each press opens a modal that asks for the stake."""

    def __init__(self, cog, key):
        super().__init__(timeout=None)
        for choice, label in OUTSIDE_BETS:
            self.add_item(_OutsideBetButton(cog, key, choice, label))
        self.add_item(_NumberBetButton(cog, key))


class RouletteBoardView(discord.ui.View):
    """The public table: the board image plus a single button each player presses to open
    their own private betting panel. Anyone in the channel may bet."""

    def __init__(self, cog, key):
        super().__init__(timeout=None)
        self.cog = cog
        self.key = key
        self.message = None

    @discord.ui.button(label="Place bets", emoji="🎰", style=discord.ButtonStyle.success)
    async def place_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.cog.roulette_closed(self.key):
            await interaction.response.send_message("This roulette table has already closed.", ephemeral=True)
            return
        await interaction.response.send_message(
            "Pick a bet below — you'll be asked for the amount each time.",
            view=RoulettePanelView(self.cog, self.key), ephemeral=True)


class Gambling(commands.Cog):

    ROULETTE_RED = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

    def __init__(self, bot):
        self.bot = bot
        self.games = {}
        # One shared blackjack shoe per guild, keyed by guild_id. It carries over between
        # games and across players and is only reshuffled once it runs out (see _draw_card).
        self.shoes = {}

    @property
    def pool(self):
        return self.bot.pool

    async def cog_check(self, ctx):
        if ctx.guild and await user_is_locked(self.pool, ctx.guild.id, ctx.author.id):
            raise UserLocked()
        return True

    async def cog_command_error(self, ctx, error):
        if isinstance(error, UserLocked):
            return
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), delete_after=10)
        else:
            raise error

    async def check_bet(self, ctx, amount, min_amount=2):
        if not isinstance(amount, int) or amount <= 0:
            return False, "Please enter a valid amount (positive integer)."
        if amount < min_amount:
            return False, f"Please place a bet of at least {min_amount}{MAIN_CURRENCY_EMOJI}."
        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        if wallet["wallet"] < amount:
            return False, f"You don't have enough {CURRENCY_NAME} to place this bet."
        return True, None

    @commands.command(aliases=["cf"])
    @require_channel("gambling_channel")
    async def coinflip(self, ctx, tries: int = 1):
        """Do a coinflip. Specify how many times to flip a coin or leave blank to flip once."""
        if tries <= 0:
            await ctx.send("Please enter a valid number of tries (positive amount).")
            return
        tries = min(tries, 10)
        results = []
        for _ in range(tries):
            results.append("H" if secrets.randbelow(2) == 0 else "T")

        embed = discord.Embed(title="Coin Flip Results", description="\n".join(results), color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["bf"])
    @require_channel("gambling_channel")
    async def betflip(self, ctx, choice, bet_per_try: str, tries: int = 1):
        """Bet on heads or tails. Specify your choice, how much to bet per try, and how many times to flip. (example: .betflip h 10 5 - bet 10 on heads per flip for 5 flips)"""
        if tries <= 0:
            await ctx.send("Please enter a valid number of tries (positive amount).")
            return
        try:
            bet_per_try = parse_amount(bet_per_try)
        except AmountError as e:
            await ctx.send(str(e))
            return
        is_valid, error = await self.check_bet(ctx, bet_per_try * tries)
        if not is_valid:
            await ctx.send(error)
            return

        if choice.lower() not in ["h", "t"]:
            await ctx.send("Please choose 'h' for heads or 't' for tails.")
            return

        choice_u = choice.upper()
        results = []
        total = 0
        for _ in range(tries):
            result = "H" if secrets.randbelow(2) == 0 else "T"

            if result == choice_u:
                total += bet_per_try
            else:
                total -= bet_per_try
            results.append(result)

        if total > 0:
            total = int(0.85 * total)
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, total)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, total, "betflip", f"{tries} tries at {bet_per_try}{MAIN_CURRENCY_EMOJI} each")

        wins = results.count(choice_u)
        losses = tries - wins
        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(None, coins.render_coins, choice_u, results, wins, losses)
        file = discord.File(buf, filename="coins.png")
        color = discord.Color.green() if total > 0 else (discord.Color.red() if total < 0 else discord.Color.blurple())
        embed = discord.Embed(title="Bet Flip Results", color=color)
        embed.set_image(url="attachment://coins.png")
        sign = "+" if total >= 0 else ""
        embed.set_footer(text=f"Net: {sign}{total}{MAIN_CURRENCY_EMOJI} · {wins}W/{losses}L")
        await ctx.send(embed=embed, file=file)

    @staticmethod
    def create_deck(deckcount=0):
        suits = ["♠️", "♥️", "♦️", "♣️"]
        values = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
        deck = [(value, suit) for suit in suits for value in values]
        if deckcount > 0:
            deck *= deckcount
        for i in range(len(deck) - 1, 0, -1):
            j = secrets.randbelow(i + 1)
            deck[i], deck[j] = deck[j], deck[i]
        return deck

    @staticmethod
    def calculate_hand_value(cards):
        value = 0
        aces = 0
        for card in cards:
            rank = card[0]
            if rank in ["J", "Q", "K"]:
                value += 10
            elif rank == "A":
                aces += 1
                value += 11
            else:
                value += int(rank)

        while value > 21 and aces > 0:
            value -= 10
            aces -= 1

        return value

    def _render_buf(self, game, hide_dealer):
        return cards.render_table(
            game["dealer_cards"],
            game["player_hands"],
            current_hand=game["current_hand"],
            hide_dealer=hide_dealer,
            deck_left=len(game["deck"]),
        )

    async def build_state(self, game, *, title, color, hide_dealer, result=None):
        """Render the table to an image and wrap it in a (File, Embed) pair.

        The Pillow rendering is offloaded to an executor so it never blocks the loop.
        """
        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(None, self._render_buf, game, hide_dealer)
        file = discord.File(buf, filename="table.png")
        embed = discord.Embed(title=f"Blackjack — {title}", color=color)
        embed.set_image(url="attachment://table.png")
        if result is not None:
            sign = "+" if result >= 0 else ""
            embed.set_footer(text=f"{sign}{result}{MAIN_CURRENCY_EMOJI}")
        else:
            hand_bet = game["hand_bets"][game["current_hand"]]
            embed.set_footer(text=f"Bet: {hand_bet}{MAIN_CURRENCY_EMOJI}")
        return file, embed

    @commands.command(aliases=["bj"])
    @require_channel("gambling_channel")
    async def blackjack(self, ctx, bet: str):
        """Start a game of blackjack by placing a bet, then play with the Hit / Stand / Double / Split buttons. You can specify an amount or use 'all' to bet everything."""
        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        try:
            bet = parse_amount(bet, wallet_balance=wallet["wallet"])
        except AmountError as e:
            await ctx.send(str(e))
            return
        is_valid, error = await self.check_bet(ctx, bet)
        if not is_valid:
            await ctx.send(error)
            return

        key = (ctx.guild.id, ctx.author.id)
        if key in self.games:
            await ctx.send("You already have an active game! Please finish it before starting a new one.")
            return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)

        shoe = self.shoes.get(ctx.guild.id)
        if not shoe:
            shoe = self.create_deck(BLACKJACK_SHOE_DECKS)
            self.shoes[ctx.guild.id] = shoe

        self.games[key] = {
            "game": "blackjack",
            "bet": bet,
            "current_hand": 0,
            "player_hands": [],
            "hand_bets": [],
            "dealer_cards": [],
            "deck": shoe,
            "state": "player_turn"
        }
        game = self.games[key]

        player_cards = [self._draw_card(game)]
        game["dealer_cards"].append(self._draw_card(game))
        player_cards.append(self._draw_card(game))
        game["dealer_cards"].append(self._draw_card(game))
        game["player_hands"].append(player_cards)
        game["hand_bets"].append(bet)

        if self.calculate_hand_value(player_cards) == 21:
            game["state"] = "dealer_turn"
            net = await self.settle(key, game)
            title, color = _result_meta(net)
            file, embed = await self.build_state(game, title=title, color=color, hide_dealer=False, result=net)
            await ctx.send(embed=embed, file=file)
            return

        view = BlackjackView(self, key)
        await view._sync_buttons(game)
        file, embed = await self.build_state(game, title="Game Started", color=discord.Color.blue(), hide_dealer=True)
        view.message = await ctx.send(embed=embed, file=file, view=view)

    # ── Blackjack actions (driven by BlackjackView buttons) ──

    def _draw_card(self, game):
        """Draw the top card of the shoe, shuffling in a fresh deck when it runs out."""
        deck = game["deck"]
        if not deck:
            deck.extend(self.create_deck(BLACKJACK_SHOE_DECKS))
        return deck.pop()

    def deal_to_current(self, game):
        """Draw one card to the current hand. Returns True if it busts."""
        hand = game["player_hands"][game["current_hand"]]
        hand.append(self._draw_card(game))
        return self.calculate_hand_value(hand) > 21

    async def double_current(self, key, game):
        """Double the current hand's bet and draw one card. Returns True if bust,
        False otherwise, or None if the player can't afford it."""
        guild_id, user_id = key
        wallet = await ensure_wallet(self.pool, guild_id, user_id)
        if wallet["wallet"] < game["bet"]:
            return None
        await update_wallet(self.pool, guild_id, user_id, -game["bet"])
        game["hand_bets"][game["current_hand"]] *= 2
        hand = game["player_hands"][game["current_hand"]]
        hand.append(self._draw_card(game))
        return self.calculate_hand_value(hand) > 21

    async def split_current(self, key, game):
        """Split the current pair into two hands. Returns False if not allowed."""
        guild_id, user_id = key
        idx = game["current_hand"]
        hand = game["player_hands"][idx]
        if not _is_pair(hand):
            return False
        wallet = await ensure_wallet(self.pool, guild_id, user_id)
        if wallet["wallet"] < game["bet"]:
            return False
        await update_wallet(self.pool, guild_id, user_id, -game["bet"])
        card1, card2 = hand[0], hand[1]
        original_bet = game["hand_bets"][idx]
        game["player_hands"][idx:idx + 1] = [
            [card1, self._draw_card(game)],
            [card2, self._draw_card(game)],
        ]
        game["hand_bets"][idx:idx + 1] = [original_bet, original_bet]
        return True

    async def advance_hand(self, key, game):
        """Move past a finished hand. Returns ("continue", None) if another hand is
        still to be played, else ("done", net) after resolving the game."""
        game["current_hand"] += 1
        if game["current_hand"] < len(game["player_hands"]):
            return "continue", None
        if all(self.calculate_hand_value(h) > 21 for h in game["player_hands"]):
            net = -sum(game["hand_bets"])
            await add_transaction(self.pool, key[0], key[1], net, "blackjack_loss")
            self.games.pop(key, None)
            return "done", net
        game["state"] = "dealer_turn"
        net = await self.settle(key, game)
        return "done", net

    async def settle(self, key, game):
        """Play out the dealer and settle every hand. Returns the net result and
        removes the game from the active set."""
        guild_id, user_id = key
        while self.calculate_hand_value(game["dealer_cards"]) < 17:
            game["dealer_cards"].append(self._draw_card(game))

        dealer_value = self.calculate_hand_value(game["dealer_cards"])
        dealer_blackjack = dealer_value == 21 and len(game["dealer_cards"]) == 2
        total_result = 0

        for i, hand in enumerate(game["player_hands"]):
            hand_bet = game["hand_bets"][i]
            player_value = self.calculate_hand_value(hand)
            player_blackjack = player_value == 21 and len(hand) == 2

            if player_value > 21:
                total_result -= hand_bet
                continue
            if dealer_blackjack and not player_blackjack:
                total_result -= hand_bet
            elif player_blackjack and not dealer_blackjack:
                winnings = int(hand_bet * 2.5)
                await update_wallet(self.pool, guild_id, user_id, winnings)
                total_result += winnings - hand_bet
            elif dealer_value > 21 or player_value > dealer_value:
                await update_wallet(self.pool, guild_id, user_id, 2 * hand_bet)
                total_result += hand_bet
            elif player_value < dealer_value:
                total_result -= hand_bet
            else:
                await update_wallet(self.pool, guild_id, user_id, hand_bet)

        if total_result != 0:
            tx_type = "blackjack_win" if total_result > 0 else "blackjack_loss"
            await add_transaction(self.pool, guild_id, user_id, total_result, tx_type)

        self.games.pop(key, None)
        return total_result

    @commands.command()
    @require_channel("gambling_channel")
    async def roulette(self, ctx):
        """Open an interactive roulette table. A public board appears with a 🎰 Place bets button — press it for your own private panel, pick a bet, and enter a stake. The wheel spins on a countdown that each new bet pushes back."""
        key = ("roulette", ctx.channel.id)
        existing = self.games.get(key)
        if existing and not existing.get("spun"):
            msg = existing.get("message")
            where = msg.jump_url if msg else "above"
            await ctx.send(f"A roulette table is already open in this channel — press 🎰 **Place bets** there: {where}")
            return

        game = {
            "game": "roulette",
            "guild_id": ctx.guild.id,
            "channel_id": ctx.channel.id,
            "opener_id": ctx.author.id,
            "bets": {},
            "message": None,
            "view": None,
            "spun": False,
            "deadline": time.time() + ROULETTE_WINDOW,
        }
        self.games[key] = game

        view = RouletteBoardView(self, key)
        game["view"] = view
        embed = self.build_roulette_embed(game, ctx.guild)
        file = discord.File(board.render_board(), filename="board.png")
        message = await ctx.send(embed=embed, file=file, view=view)
        game["message"] = message
        view.message = message
        game["timer"] = asyncio.create_task(self._roulette_timer(key))

    def build_roulette_embed(self, game, guild):
        embed = discord.Embed(title="🎡 Roulette — place your bets!", color=discord.Color.dark_green())
        deadline = int(game["deadline"])
        embed.description = (
            f"Spinning <t:{deadline}:R> — each new bet pushes the timer back.\n"
            f"Press 🎰 **Place bets** to open your private betting panel."
        )
        if any(game["bets"].values()):
            lines = []
            for uid, bets in game["bets"].items():
                member = guild.get_member(uid) if guild else None
                name = member.display_name if member else str(uid)
                staked = sum(b for _, b in bets)
                summary = ", ".join(f"{bet}{MAIN_CURRENCY_EMOJI} {_bet_label(c)}" for c, bet in bets)
                lines.append(f"**{name}** — {summary}  *(staked {staked}{MAIN_CURRENCY_EMOJI})*")
            embed.add_field(name="Current bets", value="\n".join(lines)[:1024], inline=False)
        embed.set_image(url="attachment://board.png")
        return embed

    def roulette_closed(self, key):
        game = self.games.get(key)
        return not game or game.get("spun")

    async def place_roulette_bet(self, interaction, key, choice, amount_text):
        game = self.games.get(key)
        if not game or game.get("spun"):
            await interaction.response.send_message("This roulette table has already closed.", ephemeral=True)
            return
        uid = interaction.user.id
        guild_id = game["guild_id"]
        wallet = await ensure_wallet(self.pool, guild_id, uid)
        try:
            amount = parse_amount(amount_text, wallet_balance=wallet["wallet"])
        except AmountError as e:
            await interaction.response.send_message(str(e), ephemeral=True)
            return
        if amount <= 0:
            await interaction.response.send_message("Please enter a positive amount.", ephemeral=True)
            return
        if wallet["wallet"] < amount:
            await interaction.response.send_message(
                f"You don't have enough {CURRENCY_NAME} for a {amount}{MAIN_CURRENCY_EMOJI} bet.", ephemeral=True)
            return
        await update_wallet(self.pool, guild_id, uid, -amount)
        game["bets"].setdefault(uid, []).append((choice, amount))
        game["deadline"] = time.time() + ROULETTE_WINDOW
        await self._refresh_roulette_board(game)
        await interaction.response.send_message(
            f"Placed **{amount}{MAIN_CURRENCY_EMOJI}** on **{_bet_label(choice)}**. "
            f"Spinning <t:{int(game['deadline'])}:R>.", ephemeral=True)

    async def _refresh_roulette_board(self, game):
        message = game.get("message")
        if message is None:
            return
        guild = self.bot.get_guild(game["guild_id"])
        embed = self.build_roulette_embed(game, guild)
        try:
            await message.edit(embed=embed, view=game.get("view"))
        except discord.HTTPException:
            pass

    async def _roulette_timer(self, key):
        """Spin once the deadline passes; each new bet extends game['deadline']."""
        while True:
            game = self.games.get(key)
            if not game or game.get("spun"):
                return
            remaining = game["deadline"] - time.time()
            if remaining <= 0:
                break
            await asyncio.sleep(remaining)
        await self.do_spin(key)

    async def do_spin(self, key):
        game = self.games.get(key)
        if not game or game.get("spun"):
            return
        game["spun"] = True
        view = game.get("view")
        message = game.get("message")
        guild = self.bot.get_guild(game["guild_id"])

        result = secrets.randbelow(37)
        color = "green" if result == 0 else ("red" if result in self.ROULETTE_RED else "black")

        embed = discord.Embed(
            title=f"Roulette Result: {result} ({color})",
            color=discord.Color.green() if result == 0 else (discord.Color.red() if color == "red" else discord.Color.dark_gray())
        )

        for user_id, bets in game["bets"].items():
            total_result = 0
            for choice, bet in bets:
                total_result += self.resolve_roulette_bet(choice, bet, result, color)

            if total_result > 0:
                await update_wallet(self.pool, game["guild_id"], user_id, total_result)

            net = total_result - sum(b for _, b in bets)
            if net != 0:
                tx_type = "roulette_win" if net > 0 else "roulette_loss"
                await add_transaction(self.pool, game["guild_id"], user_id, net, tx_type)

            member = guild.get_member(user_id) if guild else None
            name = member.display_name if member else str(user_id)
            sign = "+" if net >= 0 else ""
            embed.add_field(name=name, value=f"{sign}{net}{MAIN_CURRENCY_EMOJI}", inline=True)

        if not any(game["bets"].values()):
            embed.description = "No bets were placed."

        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(None, wheel.render_wheel, result)
        embed.set_image(url="attachment://wheel.png")

        if view:
            view.stop()
        self.games.pop(key, None)

        if message is None:
            return
        try:
            await message.edit(embed=embed, attachments=[discord.File(buf, filename="wheel.png")], view=None)
        except discord.HTTPException:
            buf.seek(0)
            await message.channel.send(embed=embed, file=discord.File(buf, filename="wheel.png"))

    @staticmethod
    def resolve_roulette_bet(choice, bet, result, color):
        """Returns the payout (0 if lost, includes original bet if won)."""
        if choice.isdigit():
            return bet * 36 if int(choice) == result else 0

        if choice == "red":
            won = color == "red"
        elif choice == "black":
            won = color == "black"
        elif choice == "odd":
            won = result != 0 and result % 2 == 1
        elif choice == "even":
            won = result != 0 and result % 2 == 0
        elif choice == "low":
            won = 1 <= result <= 18
        elif choice == "high":
            won = 19 <= result <= 36
        elif choice == "dozen1":
            won = 1 <= result <= 12
        elif choice == "dozen2":
            won = 13 <= result <= 24
        elif choice == "dozen3":
            won = 25 <= result <= 36
        elif choice == "col1":
            won = result != 0 and result % 3 == 1
        elif choice == "col2":
            won = result != 0 and result % 3 == 2
        elif choice == "col3":
            won = result != 0 and result % 3 == 0
        else:
            return 0

        if not won:
            return 0
        return bet * 3 if choice in ("dozen1", "dozen2", "dozen3", "col1", "col2", "col3") else bet * 2

    @commands.command(aliases=["rr"])
    async def russian_roulette(self, ctx, bet: str):
        """Play Russian Roulette with other players. You can specify an amount or use 'all' to bet everything. Everyone must match the same bet to join."""
        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        try:
            bet = parse_amount(bet, wallet_balance=wallet["wallet"])
        except AmountError as e:
            await ctx.send(str(e))
            return
        is_valid, error = await self.check_bet(ctx, bet)
        if not is_valid:
            await ctx.send(error)
            return

        key = ("russian_roulette", ctx.guild.id, ctx.channel.id)
        game = self.games.get(key)

        if not game:
            self.games[key] = {
                "game": "russian_roulette",
                "guild_id": ctx.guild.id,
                "players": [],
                "bet": bet,
                "version": 0
            }
        else:
            if ctx.author.id in self.games[key]["players"]:
                await ctx.send("You have already joined this round of Russian Roulette!")
                return

            if game["bet"] != bet:
                await ctx.send(f"The current bet for this round is {game['bet']}{MAIN_CURRENCY_EMOJI}. Please match that to join.")
                return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)

        self.games[key]["version"] += 1
        self.games[key]["players"].append(ctx.author.id)

        embed=discord.Embed(
            title="Russian Roulette",
            description=f"{ctx.author.display_name} has joined the game with a bet of {bet}{MAIN_CURRENCY_EMOJI}!\n\nType `{PREFIX}rr {bet}` to join. Spinning in **30 seconds**...",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="Players Joined", value="\n".join(
            f"- {ctx.guild.get_member(pid).display_name if ctx.guild.get_member(pid) else str(pid)}" for pid in self.games[key]["players"]
        ), inline=False)
        await ctx.send(embed=embed)

        asyncio.create_task(self.spin_russian_roulette(ctx.channel, key, self.games[key]["version"]))

    async def spin_russian_roulette(self, channel, key, version):
        await asyncio.sleep(30)

        game = self.games.get(key)
        if not game or game["version"] != version:
            return

        self.games.pop(key)
        players = game["players"]
        playercount = len(players)

        if playercount < 2:
            await channel.send("Not enough players joined for Russian Roulette! Refunding bets...")
            for pid in players:
                await update_wallet(self.pool, game["guild_id"], pid, game["bet"])
            return

        chance_hit = 1 / 6

        alive = players.copy()
        round_num = 0
        while len(alive) > 1:
            round_num += 1
            for pid in alive.copy():
                await asyncio.sleep(1)
                if secrets.randbelow(1_000_000) < int(chance_hit * 1_000_000):
                    alive.remove(pid)
                    member = channel.guild.get_member(pid)
                    name = member.display_name if member else str(pid)
                    await channel.send(f"**{name}** has been hit and died! 💀")
                    await add_transaction(self.pool, game["guild_id"], pid, -game["bet"], "russian_roulette_loss")
                    if len(alive) == 1:
                        break
                else:
                    member = channel.guild.get_member(pid)
                    name = member.display_name if member else str(pid)
                    await channel.send(f"**{name}** survived round {round_num}! 😅")

        winner = alive[0]

        await update_wallet(self.pool, game["guild_id"], winner, game["bet"] * playercount)
        await add_transaction(self.pool, game["guild_id"], winner, game["bet"] * (playercount - 1), "russian_roulette_win")
        member = channel.guild.get_member(winner)
        name = member.display_name if member else str(winner)
        embed = discord.Embed(
            title="Russian Roulette - Game Over",
            description=f"🎉 **{name}** is the last survivor and wins {game['bet'] * playercount}{MAIN_CURRENCY_EMOJI} after {round_num} devastating rounds! 🎉",
            color=discord.Color.green()
        )
        embed.add_field(name="Players", value="\n".join(
            f"- {channel.guild.get_member(pid).display_name if channel.guild.get_member(pid) else str(pid)} {'🏆' if pid == winner else '💀'}" for pid in players
        ), inline=False)
        await channel.send(embed=embed)

    # ── Admin ──

    @commands.command()
    @commands.is_owner()
    async def setgamblingchannel(self, ctx, channel: discord.TextChannel = None):
        """Admin: Set (or clear) the channel where gambling commands are allowed."""
        if channel is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'gambling_channel'",
                ctx.guild.id,
            )
            invalidate(ctx.guild.id, "gambling_channel")
            await ctx.send("Gambling channel restriction removed — commands allowed everywhere.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'gambling_channel', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(channel.id),
            )
            invalidate(ctx.guild.id, "gambling_channel")
            await ctx.send(f"Gambling commands restricted to {channel.mention}.")
