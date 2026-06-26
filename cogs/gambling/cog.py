import asyncio
import secrets
import time

import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, update_bank, add_transaction
from core.checks import require_channel, invalidate, UserLocked, user_is_locked
from core.money import parse_amount, AmountError
from core.names import format_name
from config import PREFIX
from . import cards, coins, wheel, board


BLACKJACK_TIMEOUT = 120
BLACKJACK_SHOE_DECKS = 6  # number of 52-card decks in each guild's shared shoe

HIGHERLOWER_TIMEOUT = 120
HL_HOUSE_EDGE = 0.92  # fair-odds payout is scaled by this to give the house an advantage

COINFLIP_HOUSE_EDGE = 0.95  # fair (1:1) winnings are scaled by this — a 5% house edge
RPS_HOUSE_EDGE = 0.95  # fair (1:1) winnings are scaled by this — a 5% house edge
RPS_TIMEOUT = 60
# High-low rank ordering — Aces are low.
_HL_ORDER = {r: i for i, r in enumerate(
    ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
)}


def _hl_rank(card):
    return _HL_ORDER[card[0]]


def _is_pair(hand):
    return len(hand) == 2 and hand[0][0] == hand[1][0]


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
        title, color = _result_meta(net)
        file, embed = await self.cog.build_state(game, title=title, color=color, hide_dealer=False, result=net)
        play_again = PlayAgainView(self.cog, self.key, game["bet"])
        await interaction.response.edit_message(attachments=[file], embed=embed, view=play_again)
        play_again.message = self.message
        self.stop()

    @discord.ui.button(label="Hit", style=discord.ButtonStyle.success)
    async def hit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        game = self.cog.games.get(self.key)
        if game is None or game["state"] != "player_turn":
            await interaction.response.defer()
            return
        busted = self.cog.deal_to_current(game)
        hand = game["player_hands"][game["current_hand"]]
        if not busted and self.cog.calculate_hand_value(hand) < 21:
            await self._show_turn(interaction, "Hit", discord.Color.green())
            return
        phase, net = await self.cog.advance_hand(self.key, game)
        if phase == "continue":
            title = "Busted — next hand" if busted else "21! — next hand"
            await self._show_turn(interaction, title, discord.Color.orange())
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
            cur = self.cog.bot.get_currency(self.key[0])
            await interaction.response.send_message(f"You don't have enough {cur.name} to double.", ephemeral=True)
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
            cur = self.cog.bot.get_currency(self.key[0])
            await interaction.response.send_message(
                f"You can't split that hand (need a matching pair and enough {cur.name}).", ephemeral=True
            )
            return
        await self._show_turn(interaction, "Split — Hand 1", discord.Color.blue())

    async def on_timeout(self):
        game = self.cog.games.get(self.key)
        if game is None or self.message is None:
            return
        game["state"] = "dealer_turn"
        net = await self.cog.settle(self.key, game)
        title, color = _result_meta(net)
        file, embed = await self.cog.build_state(
            game, title=f"Timed out — {title}", color=color, hide_dealer=False, result=net
        )
        play_again = PlayAgainView(self.cog, self.key, game["bet"])
        play_again.message = self.message
        try:
            await self.message.edit(attachments=[file], embed=embed, view=play_again)
        except discord.HTTPException:
            pass


class PlayAgainView(discord.ui.View):
    """Shown after a blackjack game ends; lets the player start a new one with the same bet."""

    def __init__(self, cog, key, bet, *, timeout=BLACKJACK_TIMEOUT):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.key = key
        self.bet = bet
        self.player_id = key[1]
        self.message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Play Again", style=discord.ButtonStyle.primary, emoji="🔁")
    async def play_again_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.key[0]
        if self.key in self.cog.games:
            await interaction.response.defer()
            return
        ok, error = await self.cog.check_rebet(guild_id, self.player_id, self.bet)
        if not ok:
            for child in self.children:
                child.disabled = True
            await interaction.response.edit_message(view=self)
            await interaction.followup.send(error, ephemeral=True)
            return

        await update_wallet(self.cog.pool, guild_id, self.player_id, -self.bet)
        game = self.cog.new_blackjack_game(self.key, guild_id, self.bet)

        # Retire the old message's button and post the new game as a fresh embed.
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

        if self.cog.calculate_hand_value(game["player_hands"][0]) == 21:
            game["state"] = "dealer_turn"
            net = await self.cog.settle(self.key, game)
            title, color = _result_meta(net)
            file, embed = await self.cog.build_state(game, title=title, color=color, hide_dealer=False, result=net)
            new_view = PlayAgainView(self.cog, self.key, self.bet)
            new_view.message = await interaction.followup.send(embed=embed, file=file, view=new_view)
            return

        new_view = BlackjackView(self.cog, self.key)
        await new_view._sync_buttons(game)
        file, embed = await self.cog.build_state(game, title="Game Started", color=discord.Color.blue(), hide_dealer=True)
        new_view.message = await interaction.followup.send(embed=embed, file=file, view=new_view)

    async def on_timeout(self):
        if self.message is None:
            return
        for child in self.children:
            child.disabled = True
        try:
            await self.message.edit(view=self)
        except discord.HTTPException:
            pass


class HighLowView(discord.ui.View):
    """Buttons for a single high-low game. State lives in ``cog.games[key]``."""

    def __init__(self, cog, key, *, timeout=HIGHERLOWER_TIMEOUT):
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

    def configure(self, odds):
        """Label/enable each button from the current odds (choice -> (count, mult))."""
        for choice, btn in (
            ("higher", self.higher_btn), ("equal", self.equal_btn), ("lower", self.lower_btn)
        ):
            count, mult = odds[choice]
            btn.label = f"{choice.title()} ×{mult:.2f}"
            btn.disabled = count == 0

    async def _resolve(self, interaction, choice):
        game = self.cog.games.get(self.key)
        if game is None:
            await interaction.response.defer()
            return
        net, actual = await self.cog.hl_resolve(self.key, game, choice)
        won = choice == actual
        title = f"{actual.title()}! — You {'win' if won else 'lose'}"
        color = discord.Color.green() if net > 0 else (
            discord.Color.red() if net < 0 else discord.Color.dark_gray()
        )
        file, embed = await self.cog.build_hl_state(game, title=title, color=color, reveal=True, net=net)
        play_again = HLPlayAgainView(self.cog, self.key, game["bet"])
        await interaction.response.edit_message(attachments=[file], embed=embed, view=play_again)
        play_again.message = self.message
        self.stop()

    @discord.ui.button(label="Higher", style=discord.ButtonStyle.success, emoji="⬆️")
    async def higher_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "higher")

    @discord.ui.button(label="Equal", style=discord.ButtonStyle.primary, emoji="🟰")
    async def equal_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "equal")

    @discord.ui.button(label="Lower", style=discord.ButtonStyle.danger, emoji="⬇️")
    async def lower_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "lower")

    async def on_timeout(self):
        game = self.cog.games.get(self.key)
        if game is None or self.message is None:
            return
        # Timing out forfeits the (already-deducted) bet.
        await add_transaction(self.cog.pool, self.key[0], self.key[1], -game["bet"], "higherlower_loss")
        self.cog.games.pop(self.key, None)
        file, embed = await self.cog.build_hl_state(
            game, title="Timed out — You lose", color=discord.Color.red(), reveal=False, net=-game["bet"]
        )
        play_again = HLPlayAgainView(self.cog, self.key, game["bet"])
        play_again.message = self.message
        try:
            await self.message.edit(attachments=[file], embed=embed, view=play_again)
        except discord.HTTPException:
            pass


class HLPlayAgainView(discord.ui.View):
    """Shown after a higher-lower round ends; lets the player replay the same bet."""

    def __init__(self, cog, key, bet, *, timeout=HIGHERLOWER_TIMEOUT):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.key = key
        self.bet = bet
        self.player_id = key[1]
        self.message = None

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Play Again", style=discord.ButtonStyle.primary, emoji="🔁")
    async def play_again_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild_id = self.key[0]
        if self.key in self.cog.games:
            await interaction.response.defer()
            return
        ok, error = await self.cog.check_rebet(guild_id, self.player_id, self.bet)
        if not ok:
            for child in self.children:
                child.disabled = True
            await interaction.response.edit_message(view=self)
            await interaction.followup.send(error, ephemeral=True)
            return

        await update_wallet(self.cog.pool, guild_id, self.player_id, -self.bet)
        game = self.cog.new_higherlower_game(self.key, guild_id, self.bet)

        # Retire the old message's button and post the new game as a fresh embed.
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

        new_view = HighLowView(self.cog, self.key)
        new_view.configure(game["odds"])
        file, embed = await self.cog.build_hl_state(
            game, title="Higher, Lower or Equal?", color=discord.Color.blue(), reveal=False
        )
        new_view.message = await interaction.followup.send(embed=embed, file=file, view=new_view)

    async def on_timeout(self):
        if self.message is None:
            return
        for child in self.children:
            child.disabled = True
        try:
            await self.message.edit(view=self)
        except discord.HTTPException:
            pass


# ── Roulette ──

ROULETTE_WINDOW = 10  # seconds until the wheel spins; each new bet pushes the deadline back
OUTSIDE_BETS = [
    ("red", "Red"), ("black", "Black"), ("odd", "Odd"), ("even", "Even"),
    ("low", "Low (1–18)"), ("high", "High (19–36)"),
    ("dozen1", "1st Dozen (1–12)"), ("dozen2", "2nd Dozen (13–24)"), ("dozen3", "3rd Dozen (25–36)"),
    ("col1", "Column 1"), ("col2", "Column 2"), ("col3", "Column 3"),
]
_OUTSIDE_LABELS = dict(OUTSIDE_BETS)


def _bet_label(choice):
    if choice.isdigit():
        return f"Number {choice}"
    return _OUTSIDE_LABELS.get(choice, choice.title())


class RouletteAgainView(discord.ui.View):
    """Shown after a spin; lets any participant replay their exact bets from that round."""

    def __init__(self, cog, guild_id, channel_id, bets_by_user, *, timeout=ROULETTE_WINDOW + 60):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.bets_by_user = bets_by_user
        self.message = None

    @discord.ui.button(label="Play Again", style=discord.ButtonStyle.primary, emoji="🔁")
    async def play_again_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        bets = self.bets_by_user.get(interaction.user.id)
        if not bets:
            await interaction.response.send_message("You didn't have a bet in that round.", ephemeral=True)
            return

        cur = self.cog.bot.get_currency(self.guild_id)
        total = sum(amount for _, amount in bets)
        wallet = await ensure_wallet(self.cog.pool, self.guild_id, interaction.user.id)
        if wallet["wallet"] < total:
            await interaction.response.send_message(
                f"You don't have enough {cur.name} to repeat that bet.", ephemeral=True
            )
            return
        max_bet = await self.cog.get_max_bet(self.guild_id)
        if max_bet is not None and any(amount > max_bet for _, amount in bets):
            await interaction.response.send_message(
                f"The maximum bet allowed in this server is **{max_bet:,}**{cur.emoji}.", ephemeral=True
            )
            return

        await interaction.response.defer()
        game = await self.cog._get_or_create_roulette_game(interaction.channel, interaction.guild, interaction.user.id)
        await update_wallet(self.cog.pool, self.guild_id, interaction.user.id, -total)
        game["bets"].setdefault(interaction.user.id, []).extend(bets)
        game["deadline"] = time.time() + ROULETTE_WINDOW
        await self.cog._refresh_roulette_board(game)
        summary = ", ".join(f"{amount}{cur.emoji} on {_bet_label(choice)}" for choice, amount in bets)
        await interaction.followup.send(
            f"Placed **{summary}** again. Spinning <t:{int(game['deadline'])}:R>.", ephemeral=True
        )

    async def on_timeout(self):
        if self.message is None:
            return
        for child in self.children:
            child.disabled = True
        try:
            await self.message.edit(view=self)
        except discord.HTTPException:
            pass


# ── Rock Paper Scissors ──

RPS_EMOJI = {"rock": "🪨", "paper": "📄", "scissors": "✂️"}
RPS_BEATS = {"rock": "scissors", "paper": "rock", "scissors": "paper"}


class RPSView(discord.ui.View):
    """Single-shot rock/paper/scissors round. The bet is already deducted; the
    bot's choice is only made once the player picks theirs."""

    def __init__(self, cog, ctx, bet, *, timeout=RPS_TIMEOUT):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.ctx = ctx
        self.bet = bet
        self.player_id = ctx.author.id
        self.message = None
        self.resolved = False

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.player_id:
            await interaction.response.send_message("This isn't your game!", ephemeral=True)
            return False
        return True

    async def _resolve(self, interaction, choice):
        if self.resolved:
            await interaction.response.defer()
            return
        self.resolved = True
        for child in self.children:
            child.disabled = True

        guild_id, user_id = self.ctx.guild.id, self.player_id
        cur = self.cog.bot.get_currency(guild_id)
        bot_choice = ["rock", "paper", "scissors"][secrets.randbelow(3)]

        if choice == bot_choice:
            net = 0
            await update_wallet(self.cog.pool, guild_id, user_id, self.bet)
            title, color = "Push", discord.Color.dark_gray()
        elif RPS_BEATS[choice] == bot_choice:
            net = int(self.bet * RPS_HOUSE_EDGE)
            await update_wallet(self.cog.pool, guild_id, user_id, self.bet + net)
            title, color = "Win", discord.Color.green()
        else:
            net = -self.bet
            title, color = "Loss", discord.Color.red()

        if net != 0:
            tx_type = "rps_win" if net > 0 else "rps_loss"
            await add_transaction(self.cog.pool, guild_id, user_id, net, tx_type)

        sign = "+" if net >= 0 else ""
        embed = discord.Embed(title=f"Rock Paper Scissors — {title}", color=color)
        embed.description = (
            f"You chose {RPS_EMOJI[choice]} **{choice.title()}** — "
            f"I chose {RPS_EMOJI[bot_choice]} **{bot_choice.title()}**.\n"
            f"Net: **{sign}{net}**{cur.emoji}"
        )
        await interaction.response.edit_message(embed=embed, view=self)
        self.stop()

    @discord.ui.button(label="Rock", emoji="🪨", style=discord.ButtonStyle.secondary)
    async def rock_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "rock")

    @discord.ui.button(label="Paper", emoji="📄", style=discord.ButtonStyle.secondary)
    async def paper_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "paper")

    @discord.ui.button(label="Scissors", emoji="✂️", style=discord.ButtonStyle.secondary)
    async def scissors_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._resolve(interaction, "scissors")

    async def on_timeout(self):
        if self.resolved or self.message is None:
            return
        await update_wallet(self.cog.pool, self.ctx.guild.id, self.player_id, self.bet)
        for child in self.children:
            child.disabled = True
        try:
            await self.message.edit(content="Timed out — bet refunded.", view=self)
        except discord.HTTPException:
            pass


class Gambling(commands.Cog):

    ROULETTE_RED = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

    def __init__(self, bot):
        self.bot = bot
        self.games = {}
        # One shared blackjack shoe per guild, keyed by guild_id. It carries over between
        # games and across players and is only reshuffled once it runs out (see _draw_card).
        self.shoes = {}
        # guild_id -> max bet, or None if unset/no limit
        self._max_bet_cache: dict[int, int | None] = {}

    @property
    def pool(self):
        return self.bot.pool

    async def cog_check(self, ctx):
        if ctx.guild and await user_is_locked(self.pool, ctx.guild.id, ctx.author.id):
            raise UserLocked()
        return True

    async def get_max_bet(self, guild_id: int) -> int | None:
        """Return the configured max bet for this guild, or None if unset."""
        if guild_id not in self._max_bet_cache:
            row = await self.pool.fetchrow(
                "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'gambling_max_bet'",
                guild_id,
            )
            self._max_bet_cache[guild_id] = int(row["value"]) if row else None
        return self._max_bet_cache[guild_id]

    async def check_bet(self, ctx, amount, min_amount=2):
        cur = self.bot.get_currency(ctx.guild.id)
        if not isinstance(amount, int) or amount <= 0:
            return False, "Please enter a valid amount (positive integer)."
        if amount < min_amount:
            return False, f"Please place a bet of at least {min_amount}{cur.emoji}."
        max_bet = await self.get_max_bet(ctx.guild.id)
        if max_bet is not None and amount > max_bet:
            return False, f"The maximum bet allowed in this server is **{max_bet:,}**{cur.emoji}."
        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        if wallet["wallet"] < amount:
            return False, f"You don't have enough {cur.name} to place this bet."
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
            total = int(COINFLIP_HOUSE_EDGE * total)
        cur = self.bot.get_currency(ctx.guild.id)
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, total)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, total, "betflip", f"{tries} tries at {bet_per_try}{cur.emoji} each")

        wins = results.count(choice_u)
        losses = tries - wins
        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(None, coins.render_coins, choice_u, results, wins, losses)
        file = discord.File(buf, filename="coins.png")
        color = discord.Color.green() if total > 0 else (discord.Color.red() if total < 0 else discord.Color.blurple())
        embed = discord.Embed(title="Bet Flip Results", color=color)
        embed.set_image(url="attachment://coins.png")
        sign = "+" if total >= 0 else ""
        # Currency is shown in the description, not the footer: Discord doesn't render
        # custom emoji (e.g. a guild's <:Dor:id> currency) in embed footers or titles.
        embed.description = f"Net: **{sign}{total}**{cur.emoji} · {wins}W/{losses}L"
        await ctx.send(embed=embed, file=file)

    @commands.command(aliases=["rps"])
    @require_channel("gambling_channel")
    async def rockpaperscissors(self, ctx, bet: str):
        """Play rock-paper-scissors against the bot. You can specify an amount or use 'all' to bet everything."""
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

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)
        cur = self.bot.get_currency(ctx.guild.id)
        view = RPSView(self, ctx, bet)
        embed = discord.Embed(
            title="Rock Paper Scissors",
            description=f"Bet: **{bet}**{cur.emoji}\nPick your move!",
            color=discord.Color.blue(),
        )
        view.message = await ctx.send(embed=embed, view=view)

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
        cur = self.bot.get_currency(game["guild_id"])
        embed = discord.Embed(title=f"Blackjack — {title}", color=color)
        embed.set_image(url="attachment://table.png")
        # Currency goes in the description, not the footer: Discord doesn't render custom
        # emoji (e.g. a guild's <:Dor:id> currency) in embed footers or titles.
        if result is not None:
            sign = "+" if result >= 0 else ""
            embed.description = f"**{sign}{result}**{cur.emoji}"
        else:
            hand_bet = game["hand_bets"][game["current_hand"]]
            embed.description = f"Bet: **{hand_bet}**{cur.emoji}"
        return file, embed

    @commands.command(aliases=["bj"])
    @require_channel("gambling_channel")
    async def blackjack(self, ctx, bet: str):
        """Start a game of blackjack by placing a bet, then play with the Hit / Stand / Double / Split buttons. You can specify an amount or use 'all' to bet everything. Card values: 2-10 are face value, J/Q/K count as 10, Ace counts as 1 or 11."""
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
        game = self.new_blackjack_game(key, ctx.guild.id, bet)

        if self.calculate_hand_value(game["player_hands"][0]) == 21:
            game["state"] = "dealer_turn"
            net = await self.settle(key, game)
            title, color = _result_meta(net)
            file, embed = await self.build_state(game, title=title, color=color, hide_dealer=False, result=net)
            play_again = PlayAgainView(self, key, bet)
            play_again.message = await ctx.send(embed=embed, file=file, view=play_again)
            return

        view = BlackjackView(self, key)
        await view._sync_buttons(game)
        file, embed = await self.build_state(game, title="Game Started", color=discord.Color.blue(), hide_dealer=True)
        view.message = await ctx.send(embed=embed, file=file, view=view)

    # ── Blackjack actions (driven by BlackjackView buttons) ──

    def new_blackjack_game(self, key, guild_id, bet):
        """Create a fresh game dict and deal the opening hand. The caller must
        already have deducted the bet from the player's wallet."""
        shoe = self.shoes.get(guild_id)
        if not shoe:
            shoe = self.create_deck(BLACKJACK_SHOE_DECKS)
            self.shoes[guild_id] = shoe

        self.games[key] = {
            "game": "blackjack",
            "guild_id": guild_id,
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
        return game

    async def check_rebet(self, guild_id, user_id, bet):
        """Validate that a player can afford to replay their previous bet."""
        cur = self.bot.get_currency(guild_id)
        wallet = await ensure_wallet(self.pool, guild_id, user_id)
        if wallet["wallet"] < bet:
            return False, f"You don't have enough {cur.name} to bet {bet}{cur.emoji} again."
        return True, None

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

    # ── Higher-Lower ──

    def _hl_odds(self, game):
        """Compute, from the remaining shoe, the payout odds for each choice.

        Returns {choice: (count, multiplier)}. The multiplier is the fair payout
        (stake / probability) scaled by the house edge, floored at 1.05 so a win
        always pays out at least a bit more than the bet. A count of 0 means the
        choice is impossible (e.g. "lower" on an Ace) and gets disabled by the view.
        """
        deck = game["deck"]
        if not deck:
            deck.extend(self.create_deck(BLACKJACK_SHOE_DECKS))
        current = _hl_rank(game["current_card"])
        total = len(deck)
        counts = {"higher": 0, "equal": 0, "lower": 0}
        for card in deck:
            r = _hl_rank(card)
            if r > current:
                counts["higher"] += 1
            elif r < current:
                counts["lower"] += 1
            else:
                counts["equal"] += 1
        odds = {}
        for choice, count in counts.items():
            mult = max(1.05, (total / count) * HL_HOUSE_EDGE) if count else 0.0
            odds[choice] = (count, round(mult, 2))
        return odds

    async def hl_resolve(self, key, game, choice):
        """Draw the next card, settle the bet, and tear down the game.

        Returns (net, actual) where actual is "higher"/"equal"/"lower"."""
        guild_id, user_id = key
        next_card = self._draw_card(game)
        game["next_card"] = next_card
        cur_rank, nxt_rank = _hl_rank(game["current_card"]), _hl_rank(next_card)
        if nxt_rank > cur_rank:
            actual = "higher"
        elif nxt_rank < cur_rank:
            actual = "lower"
        else:
            actual = "equal"

        bet = game["bet"]
        if choice == actual:
            winnings = int(bet * game["odds"][choice][1])
            await update_wallet(self.pool, guild_id, user_id, winnings)
            net = winnings - bet
        else:
            net = -bet
        if net != 0:
            tx_type = "higherlower_win" if net > 0 else "higherlower_loss"
            await add_transaction(self.pool, guild_id, user_id, net, tx_type)
        self.games.pop(key, None)
        return net, actual

    def new_higherlower_game(self, key, guild_id, bet):
        """Create a fresh higher-lower game dict and draw the opening card."""
        shoe = self.shoes.get(guild_id)
        if not shoe:
            shoe = self.create_deck(BLACKJACK_SHOE_DECKS)
            self.shoes[guild_id] = shoe

        game = {
            "game": "higherlower",
            "guild_id": guild_id,
            "bet": bet,
            "deck": shoe,
            "next_card": None,
        }
        self.games[key] = game
        game["current_card"] = self._draw_card(game)
        game["odds"] = self._hl_odds(game)
        return game

    async def build_hl_state(self, game, *, title, color, reveal, net=None):
        """Render the high-low table to an image and wrap it in a (File, Embed)."""
        next_card = game.get("next_card") if reveal else None
        outcome = None if net is None else ("win" if net > 0 else ("loss" if net < 0 else None))
        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(
            None,
            lambda: cards.render_highlow(
                game["current_card"], next_card, deck_left=len(game["deck"]), outcome=outcome
            ),
        )
        file = discord.File(buf, filename="highlow.png")
        cur = self.bot.get_currency(game["guild_id"])
        embed = discord.Embed(title=f"Higher-Lower — {title}", color=color)
        embed.set_image(url="attachment://highlow.png")
        # Currency goes in the description, not the footer: Discord doesn't render custom
        # emoji (e.g. a guild's <:Dor:id> currency) in embed footers or titles.
        if net is not None:
            sign = "+" if net >= 0 else ""
            embed.description = f"**{sign}{net}**{cur.emoji}"
        else:
            embed.description = f"Bet: **{game['bet']}**{cur.emoji}"
            embed.set_footer(text="Order: A < 2 < 3 < 4 < 5 < 6 < 7 < 8 < 9 < 10 < J < Q < K")
        return file, embed

    @commands.command(aliases=["hl", "highlow"])
    @require_channel("gambling_channel")
    async def higherlower(self, ctx, bet: str):
        """Play higher-lower: a card is drawn, then bet whether the next card will be Higher, Lower or Equal. Rarer outcomes pay bigger multipliers (shown on the buttons). You can specify an amount or use 'all' to bet everything."""
        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        try:
            bet = parse_amount(bet, wallet_balance=wallet["wallet"])
        except AmountError as e:
            await ctx.send(str(e))
            return
        is_valid, error = await self.check_bet(ctx, bet, 20)
        if not is_valid:
            await ctx.send(error)
            return

        key = (ctx.guild.id, ctx.author.id)
        if key in self.games:
            await ctx.send("You already have an active game! Please finish it before starting a new one.")
            return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)
        game = self.new_higherlower_game(key, ctx.guild.id, bet)

        view = HighLowView(self, key)
        view.configure(game["odds"])
        file, embed = await self.build_hl_state(
            game, title="Higher, Lower or Equal?", color=discord.Color.blue(), reveal=False
        )
        view.message = await ctx.send(embed=embed, file=file, view=view)

    @commands.command(extras={"example": ".roulette red 100"})
    @require_channel("gambling_channel")
    async def roulette(self, ctx, option: str, bet: str):
        """Place a bet on the roulette table. Usage: .roulette <option> <amount>. Options: red, black, odd, even, low, high, dozen1-3, col1-3, or a number 0–36. The wheel spins 10 seconds after the last bet.

        Payouts:
        - Straight number (0-36): ×36
        - Red / black / odd / even / low (1-18) / high (19-36): ×2
        - Dozen (dozen1-3) / column (col1-3): ×3"""
        _valid_outside = {k for k, _ in OUTSIDE_BETS}
        if option.isdigit():
            n = int(option)
            if not (0 <= n <= 36):
                await ctx.send("Number must be between 0 and 36.")
                return
            choice = str(n)
        elif option in _valid_outside:
            choice = option
        else:
            outside_list = ", ".join(k for k, _ in OUTSIDE_BETS)
            await ctx.send(f"Invalid option. Choose from: {outside_list}, or a number 0–36.")
            return

        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        try:
            amount = parse_amount(bet, wallet_balance=wallet["wallet"])
        except AmountError as e:
            await ctx.send(str(e))
            return
        if amount <= 0:
            await ctx.send("Please enter a positive amount.")
            return
        cur = self.bot.get_currency(ctx.guild.id)
        max_bet = await self.get_max_bet(ctx.guild.id)
        if max_bet is not None and amount > max_bet:
            await ctx.send(f"The maximum bet allowed in this server is **{max_bet:,}**{cur.emoji}.")
            return
        if wallet["wallet"] < amount:
            await ctx.send(f"You don't have enough {cur.name} for a {amount}{cur.emoji} bet.")
            return

        game = await self._get_or_create_roulette_game(ctx.channel, ctx.guild, ctx.author.id)

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -amount)
        game["bets"].setdefault(ctx.author.id, []).append((choice, amount))
        game["deadline"] = time.time() + ROULETTE_WINDOW
        await self._refresh_roulette_board(game)
        await ctx.send(
            f"Placed **{amount}{cur.emoji}** on **{_bet_label(choice)}**. "
            f"Spinning <t:{int(game['deadline'])}:R>.",
            delete_after=8,
        )

    async def _get_or_create_roulette_game(self, channel, guild, opener_id):
        """Return the open roulette game for this channel, creating and announcing one if needed."""
        key = ("roulette", channel.id)
        game = self.games.get(key)
        if game and not game.get("spun"):
            return game

        game = {
            "game": "roulette",
            "guild_id": guild.id,
            "channel_id": channel.id,
            "opener_id": opener_id,
            "bets": {},
            "message": None,
            "spun": False,
            "deadline": time.time() + ROULETTE_WINDOW,
        }
        self.games[key] = game
        embed = self.build_roulette_embed(game, guild)
        file = discord.File(board.render_board(), filename="board.png")
        message = await channel.send(embed=embed, file=file)
        game["message"] = message
        game["timer"] = asyncio.create_task(self._roulette_timer(key))
        return game

    def build_roulette_embed(self, game, guild):
        cur = self.bot.get_currency(game["guild_id"])
        embed = discord.Embed(title="🎡 Roulette — place your bets!", color=discord.Color.dark_green())
        deadline = int(game["deadline"])
        embed.description = (
            f"Spinning <t:{deadline}:R> — each new bet pushes the timer back.\n"
            f"Use `.roulette <option> <amount>` to place a bet."
        )
        if any(game["bets"].values()):
            lines = []
            for uid, bets in game["bets"].items():
                member = guild.get_member(uid) if guild else None
                name = format_name(member, guild, fallback=str(uid))
                staked = sum(b for _, b in bets)
                summary = ", ".join(f"{bet}{cur.emoji} {_bet_label(c)}" for c, bet in bets)
                lines.append(f"**{name}** — {summary}  *(staked {staked}{cur.emoji})*")
            embed.add_field(name="Current bets", value="\n".join(lines)[:1024], inline=False)
        embed.set_image(url="attachment://board.png")
        return embed

    async def _refresh_roulette_board(self, game):
        message = game.get("message")
        if message is None:
            return
        guild = self.bot.get_guild(game["guild_id"])
        embed = self.build_roulette_embed(game, guild)
        try:
            await message.edit(embed=embed)
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
        message = game.get("message")
        guild = self.bot.get_guild(game["guild_id"])
        cur = self.bot.get_currency(game["guild_id"])

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
            name = format_name(member, guild, fallback=str(user_id))
            sign = "+" if net >= 0 else ""
            embed.add_field(name=name, value=f"{sign}{net}{cur.emoji}", inline=True)

        if not any(game["bets"].values()):
            embed.description = "No bets were placed."

        loop = asyncio.get_running_loop()
        buf = await loop.run_in_executor(None, wheel.render_wheel, result)
        embed.set_image(url="attachment://wheel.png")

        play_again = RouletteAgainView(self, game["guild_id"], game["channel_id"], dict(game["bets"]))
        self.games.pop(key, None)

        if message is None:
            return
        try:
            await message.edit(embed=embed, attachments=[discord.File(buf, filename="wheel.png")], view=play_again)
            play_again.message = message
        except discord.HTTPException:
            buf.seek(0)
            play_again.message = await message.channel.send(
                embed=embed, file=discord.File(buf, filename="wheel.png"), view=play_again
            )

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
        cur = self.bot.get_currency(ctx.guild.id)
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
                await ctx.send(f"The current bet for this round is {game['bet']}{cur.emoji}. Please match that to join.")
                return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)

        self.games[key]["version"] += 1
        self.games[key]["players"].append(ctx.author.id)

        embed=discord.Embed(
            title="Russian Roulette",
            description=f"{format_name(ctx.author)} has joined the game with a bet of {bet}{cur.emoji}!\n\nType `{PREFIX}rr {bet}` to join. Spinning in **30 seconds**...",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="Players Joined", value="\n".join(
            f"- {format_name(ctx.guild.get_member(pid), ctx.guild, fallback=str(pid))}" for pid in self.games[key]["players"]
        ), inline=False)
        await ctx.send(embed=embed)

        asyncio.create_task(self.spin_russian_roulette(ctx.channel, key, self.games[key]["version"]))

    async def spin_russian_roulette(self, channel, key, version):
        await asyncio.sleep(30)

        game = self.games.get(key)
        if not game or game["version"] != version:
            return

        self.games.pop(key)
        cur = self.bot.get_currency(game["guild_id"])
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
                    name = format_name(member, channel.guild, fallback=str(pid))
                    await channel.send(f"**{name}** has been hit and died! 💀")
                    await add_transaction(self.pool, game["guild_id"], pid, -game["bet"], "russian_roulette_loss")
                    if len(alive) == 1:
                        break
                else:
                    member = channel.guild.get_member(pid)
                    name = format_name(member, channel.guild, fallback=str(pid))
                    await channel.send(f"**{name}** survived round {round_num}! 😅")

        winner = alive[0]

        await update_wallet(self.pool, game["guild_id"], winner, game["bet"] * playercount)
        await add_transaction(self.pool, game["guild_id"], winner, game["bet"] * (playercount - 1), "russian_roulette_win")
        member = channel.guild.get_member(winner)
        name = format_name(member, channel.guild, fallback=str(winner))
        embed = discord.Embed(
            title="Russian Roulette - Game Over",
            description=f"🎉 **{name}** is the last survivor and wins {game['bet'] * playercount}{cur.emoji} after {round_num} devastating rounds! 🎉",
            color=discord.Color.green()
        )
        embed.add_field(name="Players", value="\n".join(
            f"- {format_name(channel.guild.get_member(pid), channel.guild, fallback=str(pid))} {'🏆' if pid == winner else '💀'}" for pid in players
        ), inline=False)
        await channel.send(embed=embed)

    # ── Admin ──

    @commands.command()
    @commands.is_owner()
    async def setgamblingchannel(self, ctx, channel: discord.TextChannel = None):
        """Set (or clear) the channel where gambling commands are allowed."""
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

    @commands.command()
    @commands.is_owner()
    async def setmaxbet(self, ctx, amount: int = None):
        """Set (or clear) the highest amount that can be wagered in a single gambling bet. No argument removes the limit."""
        if amount is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'gambling_max_bet'",
                ctx.guild.id,
            )
            self._max_bet_cache[ctx.guild.id] = None
            await ctx.send("Max bet limit removed — bets are no longer capped.")
        else:
            if amount <= 0:
                await ctx.send("Please enter a positive amount.")
                return
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'gambling_max_bet', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(amount),
            )
            self._max_bet_cache[ctx.guild.id] = amount
            cur = self.bot.get_currency(ctx.guild.id)
            await ctx.send(f"Max bet set to **{amount:,}**{cur.emoji}.")
