import random
import asyncio
import discord
import aiosqlite
from discord.ext import commands
from utils import is_guild_owner, check_channel_allowed, PREFIX

DB_PATH = "data/economy.db"

DEFAULT_MIN_BET = 10
DEFAULT_MAX_BET = 50000
DEFAULT_COINFLIP_MULTIPLIER = 1.9


DEFAULT_RR_JOIN_TIME = 30

SUITS = ["\u2660", "\u2665", "\u2666", "\u2663"]  # spades, hearts, diamonds, clubs
RANKS = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]


def new_deck(count: int = 1) -> list[tuple[str, str]]:
    """Create a shuffled deck (or multiple decks)."""
    deck = [(r, s) for s in SUITS for r in RANKS] * count
    random.shuffle(deck)
    return deck


def card_value(hand: list[tuple[str, str]]) -> int:
    """Calculate the best blackjack value for a hand."""
    total = 0
    aces = 0
    for rank, _ in hand:
        if rank == "A":
            total += 11
            aces += 1
        elif rank in ("J", "Q", "K"):
            total += 10
        else:
            total += int(rank)
    while total > 21 and aces:
        total -= 10
        aces -= 1
    return total


def card_rank_value(rank: str) -> int:
    """Numeric value of a rank for split comparison (10/J/Q/K all equal 10)."""
    if rank in ("J", "Q", "K"):
        return 10
    if rank == "A":
        return 11
    return int(rank)


def format_hand(hand: list[tuple[str, str]], hide_first: bool = False) -> str:
    """Format a hand for display. If hide_first, the first card is hidden."""
    if hide_first:
        return f"[??] {hand[1][0]}{hand[1][1]}"
    return " ".join(f"{r}{s}" for r, s in hand)


# --- Roulette data (American wheel with 0 and 00) ---
ROULETTE_RED = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}
ROULETTE_BLACK = {2, 4, 6, 8, 10, 11, 13, 15, 17, 20, 22, 24, 26, 28, 29, 31, 33, 35}
ROULETTE_SLOTS = list(range(0, 37)) + ["00"]  # 0-36 plus 00


def roulette_color(num) -> str:
    """Return the color of a roulette number."""
    if num == 0 or num == "00":
        return "green"
    return "red" if num in ROULETTE_RED else "black"


def roulette_color_emoji(num) -> str:
    """Return a colored circle for the number."""
    c = roulette_color(num)
    if c == "red":
        return "\U0001f534"
    if c == "black":
        return "\u26ab"
    return "\U0001f7e2"


def parse_roulette_bet(bet_type: str) -> tuple[str, str | None]:
    """Parse a bet type string. Returns (category, detail) or (None, error_msg).
    Categories: number, color, parity, highlow, dozen, column, green
    """
    b = bet_type.lower()

    # Single number
    if b == "00":
        return ("number", "00")
    try:
        n = int(b)
        if 0 <= n <= 36:
            return ("number", n)
        return (None, "Number must be 0-36 or 00.")
    except ValueError:
        pass

    # Color
    if b in ("red", "r"):
        return ("color", "red")
    if b in ("black", "b"):
        return ("color", "black")

    # Parity
    if b in ("odd", "o"):
        return ("parity", "odd")
    if b in ("even", "e"):
        return ("parity", "even")

    # High / Low
    if b in ("high", "hi", "19-36"):
        return ("highlow", "high")
    if b in ("low", "lo", "1-18"):
        return ("highlow", "low")

    # Dozens
    if b in ("1st", "first", "1-12"):
        return ("dozen", "1st")
    if b in ("2nd", "second", "13-24"):
        return ("dozen", "2nd")
    if b in ("3rd", "third", "25-36"):
        return ("dozen", "3rd")

    # Columns
    if b in ("col1", "c1"):
        return ("column", "col1")
    if b in ("col2", "c2"):
        return ("column", "col2")
    if b in ("col3", "c3"):
        return ("column", "col3")

    # Green (0 and 00)
    if b in ("green", "g"):
        return ("green", "green")

    return (None, (
        "Invalid bet type. Options: a number (0-36, 00), "
        "red/black, odd/even, high/low, 1st/2nd/3rd, col1/col2/col3, green."
    ))


def check_roulette_win(category: str, detail, result) -> bool:
    """Check if a roulette bet wins given the result number."""
    if category == "number":
        return result == detail
    if category == "green":
        return result == 0 or result == "00"
    # 0 and 00 lose all outside bets
    if result == 0 or result == "00":
        return False
    if category == "color":
        return roulette_color(result) == detail
    if category == "parity":
        return (result % 2 == 1) == (detail == "odd")
    if category == "highlow":
        return (result >= 19) == (detail == "high")
    if category == "dozen":
        if detail == "1st":
            return 1 <= result <= 12
        if detail == "2nd":
            return 13 <= result <= 24
        return 25 <= result <= 36
    if category == "column":
        if detail == "col1":
            return result % 3 == 1
        if detail == "col2":
            return result % 3 == 2
        return result % 3 == 0
    return False


def roulette_payout_multiplier(category: str) -> int:
    """Return the payout multiplier (including the original bet) for a bet type."""
    if category == "number":
        return 36
    if category == "green":
        return 18
    if category in ("color", "parity", "highlow"):
        return 2
    if category in ("dozen", "column"):
        return 3
    return 0


class Gambling(commands.Cog):
    _owner_commands = {"setminbet", "setmaxbet"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None
        self.rr_games: dict[int, dict] = {}  # channel_id -> game info
        self.bj_games: dict[int, dict] = {}  # user_id -> game info
        self.rl_tables: dict[int, dict] = {}  # channel_id -> roulette table state

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "gambling", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS gambling_settings (
                guild_id INTEGER PRIMARY KEY,
                min_bet INTEGER DEFAULT 10,
                max_bet INTEGER DEFAULT 50000,
                coinflip_multiplier REAL DEFAULT 1.9
            )"""
        )
        await self.db.commit()

    async def cog_unload(self):
        if self.db:
            await self.db.close()

    async def get_settings(self, guild_id: int) -> dict:
        """Get gambling settings for a guild."""
        async with self.db.execute(
            "SELECT min_bet, max_bet, coinflip_multiplier FROM gambling_settings WHERE guild_id = ?",
            (guild_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row:
            return {"min_bet": row[0], "max_bet": row[1], "coinflip_multiplier": row[2]}
        return {
            "min_bet": DEFAULT_MIN_BET,
            "max_bet": DEFAULT_MAX_BET,
            "coinflip_multiplier": DEFAULT_COINFLIP_MULTIPLIER,
        }

    async def get_cash(self, user_id: int) -> int:
        """Get a user's cash balance."""
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else 0

    async def update_cash(self, user_id: int, amount: int):
        """Add (positive) or subtract (negative) cash from a user."""
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, user_id),
        )
        await self.db.commit()

    async def validate_bet(self, ctx: commands.Context, bet: int) -> bool:
        """Validate a bet amount. Returns False and sends a message if invalid."""
        settings = await self.get_settings(ctx.guild.id)

        if bet < settings["min_bet"]:
            await ctx.send(f"Minimum bet is **${settings['min_bet']:,}**.")
            return False
        if bet > settings["max_bet"]:
            await ctx.send(f"Maximum bet is **${settings['max_bet']:,}**.")
            return False

        cash = await self.get_cash(ctx.author.id)
        if bet > cash:
            await ctx.send(f"You only have **${cash:,}** in cash.")
            return False

        return True

    # --- Coin Flip ---

    @commands.command(aliases=["cf"])
    async def coinflip(self, ctx: commands.Context, *args):
        """Flip a coin.
        !cf 10       — flip 10 coins (no bet)
        !cf h 10     — bet $10 on heads
        !cf t 10 5   — 5 flips, $10 each on tails
        """
        side, bet, times = self._parse_coinflip_args(args)

        # No side chosen — just flip for fun
        if side is None:
            times = max(times, 1)
            if bet is not None:
                times = bet  # !cf 10 means 10 fun flips
            times = min(times, 50)
            if times == 1:
                await ctx.send(f"**{random.choice(['Heads', 'Tails'])}!**")
                return
            results = [random.choice(["H", "T"]) for _ in range(times)]
            await ctx.send(
                f"**{' '.join(results)}**\nHeads: {results.count('H')} | Tails: {results.count('T')}"
            )
            return

        # Side chosen — need a bet
        if bet is None or bet <= 0:
            await ctx.send(f"You need to specify a bet amount. Usage: `{ctx.prefix}cf h 100`")
            return

        times = min(max(times, 1), 50)
        win_side = "H" if side == "h" else "T"
        side_name = "Heads" if side == "h" else "Tails"

        # Single flip
        if times == 1:
            if not await self.validate_bet(ctx, bet):
                return

            settings = await self.get_settings(ctx.guild.id)
            result = random.choice(["H", "T"])
            won = result == win_side
            result_name = "Heads" if result == "H" else "Tails"

            if won:
                winnings = int(bet * settings["coinflip_multiplier"])
                profit = winnings - bet
                await self.update_cash(ctx.author.id, profit)
                embed = discord.Embed(
                    title=f"Coin Flip — {result_name}!",
                    description=f"You bet on **{side_name}** and won **${winnings:,}**! (Profit: ${profit:,})",
                    color=discord.Color.green(),
                )
            else:
                await self.update_cash(ctx.author.id, -bet)
                embed = discord.Embed(
                    title=f"Coin Flip — {result_name}!",
                    description=f"You bet on **{side_name}** and lost **${bet:,}**.",
                    color=discord.Color.red(),
                )

            await ctx.send(embed=embed)
            return

        # Multiple flips
        total_bet = bet * times

        cash = await self.get_cash(ctx.author.id)
        if total_bet > cash:
            await ctx.send(
                f"You need **${total_bet:,}** for {times} flips at ${bet:,} each, "
                f"but only have **${cash:,}**."
            )
            return

        settings = await self.get_settings(ctx.guild.id)
        if bet < settings["min_bet"]:
            await ctx.send(f"Minimum bet is **${settings['min_bet']:,}**.")
            return
        if bet > settings["max_bet"]:
            await ctx.send(f"Maximum bet is **${settings['max_bet']:,}**.")
            return

        results = [random.choice(["H", "T"]) for _ in range(times)]
        wins = results.count(win_side)
        losses = times - wins

        total_won = int(wins * bet * settings["coinflip_multiplier"])
        net = total_won - total_bet

        await self.update_cash(ctx.author.id, net)

        color = discord.Color.green() if net >= 0 else discord.Color.red()
        sign = "+" if net >= 0 else "-"

        embed = discord.Embed(
            title=f"Coin Flip — {times} Flips on {side_name}",
            description=f"**{' '.join(results)}**",
            color=color,
        )
        embed.add_field(name="Wins", value=str(wins))
        embed.add_field(name="Losses", value=str(losses))
        embed.add_field(name="Net", value=f"{sign}${abs(net):,}")
        await ctx.send(embed=embed)

    @staticmethod
    def _parse_coinflip_args(args) -> tuple:
        """Parse coinflip args. Returns (side, bet, times).
        {prefix}cf           -> (None, None, 1)
        {prefix}cf 10        -> (None, 10, 1)
        {prefix}cf h 10      -> ("h", 10, 1)
        {prefix}cf h 10 5    -> ("h", 10, 5)
        """
        side = None
        bet = None
        times = 1

        if not args:
            return side, bet, times

        # Check if first arg is a side
        if args[0].lower() in ("h", "t", "heads", "tails"):
            side = args[0].lower()[0]  # normalize to "h" or "t"
            args = args[1:]

        if len(args) >= 1:
            try:
                bet = int(args[0])
            except ValueError:
                pass

        if len(args) >= 2:
            try:
                times = int(args[1])
            except ValueError:
                pass

        return side, bet, times

    # --- Russian Roulette ---

    @commands.command(aliases=["rr"])
    async def russianroulette(self, ctx: commands.Context, action: str = None):
        """Multiplayer Russian Roulette.
        {prefix}rr 500    — start a game with $500 buy-in
        {prefix}rr join   — join an active game
        """
        if action is None:
            await ctx.send(f"Usage: `{ctx.prefix}rr <bet>` to start or `{ctx.prefix}rr join` to join.")
            return

        if action.lower() == "join":
            await self._rr_join(ctx)
        else:
            try:
                bet = int(action)
            except ValueError:
                await ctx.send(f"Usage: `{ctx.prefix}rr <bet>` to start or `{ctx.prefix}rr join` to join.")
                return
            await self._rr_start(ctx, bet)

    async def _rr_start(self, ctx: commands.Context, bet: int):
        """Start a new Russian Roulette game."""
        channel_id = ctx.channel.id

        if channel_id in self.rr_games:
            await ctx.send(f"A game is already running in this channel! Use `{ctx.prefix}rr join` to join.")
            return

        if not await self.validate_bet(ctx, bet):
            return

        # Deduct buy-in from starter
        await self.update_cash(ctx.author.id, -bet)

        self.rr_games[channel_id] = {
            "bet": bet,
            "players": [ctx.author],
            "starter": ctx.author,
        }

        embed = discord.Embed(
            title="Russian Roulette",
            description=(
                f"{ctx.author.mention} started a game!\n"
                f"**Buy-in:** ${bet:,}\n\n"
                f"Type `{ctx.prefix}rr join` to enter. Game starts in **{DEFAULT_RR_JOIN_TIME}s**."
            ),
            color=discord.Color.dark_red(),
        )
        embed.add_field(name="Players", value=f"1. {ctx.author.display_name}")
        await ctx.send(embed=embed)

        # Wait for join phase
        await asyncio.sleep(DEFAULT_RR_JOIN_TIME)

        # Check game still exists (could have been cancelled)
        game = self.rr_games.get(channel_id)
        if game is None:
            return

        players = game["players"]
        if len(players) < 2:
            # Refund the only player
            await self.update_cash(players[0].id, bet)
            del self.rr_games[channel_id]
            await ctx.send("Not enough players joined. Game cancelled, buy-in refunded.")
            return

        # Play the game
        await self._rr_play(ctx, game)

    async def _rr_join(self, ctx: commands.Context):
        """Join an existing Russian Roulette game."""
        channel_id = ctx.channel.id
        game = self.rr_games.get(channel_id)

        if game is None:
            await ctx.send(f"No active game in this channel. Start one with `{ctx.prefix}rr <bet>`.")
            return

        if ctx.author in game["players"]:
            await ctx.send("You're already in this game!")
            return

        bet = game["bet"]

        # Check cash
        cash = await self.get_cash(ctx.author.id)
        if cash < bet:
            await ctx.send(f"You need **${bet:,}** to join but only have **${cash:,}**.")
            return

        # Deduct buy-in
        await self.update_cash(ctx.author.id, -bet)
        game["players"].append(ctx.author)

        player_list = "\n".join(
            f"{i+1}. {p.display_name}" for i, p in enumerate(game["players"])
        )
        embed = discord.Embed(
            title="Russian Roulette",
            description=f"{ctx.author.mention} joined the game!",
            color=discord.Color.dark_red(),
        )
        embed.add_field(name=f"Players ({len(game['players'])})", value=player_list)
        await ctx.send(embed=embed)

    async def _rr_play(self, ctx: commands.Context, game: dict):
        """Simulate the Russian Roulette rounds."""
        channel_id = ctx.channel.id
        players = list(game["players"])
        bet = game["bet"]
        pot = bet * len(players)

        random.shuffle(players)
        eliminated = []

        await ctx.send("**The cylinder spins...** Game is starting!")
        await asyncio.sleep(2)

        round_num = 0
        turn = 0
        while len(players) > 1:
            round_num += 1
            current = players[turn % len(players)]
            shot = random.randint(1, 6) == 1

            if shot:
                players.remove(current)
                eliminated.append(current)
                await ctx.send(
                    f"**Round {round_num}** — {current.mention} pulls the trigger... "
                    f"**BANG!** {current.display_name} is eliminated!"
                )
                # Don't advance turn — next player slides into this index
                if turn >= len(players):
                    turn = 0
            else:
                await ctx.send(
                    f"**Round {round_num}** — {current.mention} pulls the trigger... "
                    f"*click*. Safe!"
                )
                turn = (turn + 1) % len(players)

            await asyncio.sleep(2)

        # Winner
        winner = players[0]
        await self.update_cash(winner.id, pot)

        elim_list = "\n".join(
            f"{i+1}. ~~{p.display_name}~~" for i, p in enumerate(eliminated)
        )

        embed = discord.Embed(
            title="Russian Roulette — Game Over!",
            description=f"{winner.mention} survived and wins **${pot:,}**!",
            color=discord.Color.green(),
        )
        embed.add_field(name="Eliminated", value=elim_list or "None", inline=False)
        embed.add_field(name="Pot", value=f"${pot:,}")
        embed.add_field(name="Profit", value=f"${pot - bet:,}")
        await ctx.send(embed=embed)

        del self.rr_games[channel_id]

    # --- Blackjack ---

    def _bj_embed(self, game: dict, reveal: bool = False) -> discord.Embed:
        """Build the blackjack game embed."""
        hands = game["hands"]
        bets = game["bets"]
        active = game["active_hand"]
        dealer_hand = game["dealer_hand"]
        multi = len(hands) > 1

        embed = discord.Embed(title="Blackjack", color=discord.Color.dark_teal())

        for i, hand in enumerate(hands):
            val = card_value(hand)
            marker = " \u25c0" if multi and i == active and not reveal else ""
            label = f"Hand {i+1}" if multi else "Your Hand"
            embed.add_field(
                name=f"{label} ({val}){marker}",
                value=format_hand(hand),
                inline=False,
            )

        dealer_val = card_value(dealer_hand) if reveal else card_value(dealer_hand[1:])
        embed.add_field(
            name=f"Dealer {'(' + str(card_value(dealer_hand)) + ')' if reveal else ''}",
            value=format_hand(dealer_hand, hide_first=not reveal),
            inline=False,
        )

        total_bet = sum(bets)
        actions = f"{PREFIX}hit \u00b7 {PREFIX}stand \u00b7 {PREFIX}double"
        if not reveal and len(hands) == 1 and len(hands[0]) == 2:
            r1, r2 = hands[0][0][0], hands[0][1][0]
            if card_rank_value(r1) == card_rank_value(r2):
                actions += f" \u00b7 {PREFIX}split"
        embed.set_footer(text=f"Bet: ${total_bet:,} | {actions}")
        return embed

    @commands.command(aliases=["bj"])
    async def blackjack(self, ctx: commands.Context, bet: int):
        """Start a blackjack game. Usage: {prefix}bj 100"""
        if ctx.author.id in self.bj_games:
            await ctx.send(f"You already have a game running! Use `{ctx.prefix}hit`, `{ctx.prefix}stand`, or `{ctx.prefix}double`.")
            return

        if not await self.validate_bet(ctx, bet):
            return

        await self.update_cash(ctx.author.id, -bet)

        deck = new_deck()
        player_hand = [deck.pop(), deck.pop()]
        dealer_hand = [deck.pop(), deck.pop()]

        game = {
            "hands": [player_hand],
            "bets": [bet],
            "active_hand": 0,
            "deck": deck,
            "dealer_hand": dealer_hand,
            "channel_id": ctx.channel.id,
            "guild_id": ctx.guild.id,
        }
        self.bj_games[ctx.author.id] = game

        # Check for natural blackjack
        if card_value(player_hand) == 21:
            await self._bj_finish(ctx, game)
            return

        await ctx.send(embed=self._bj_embed(game))

    def _active_hand(self, game: dict):
        """Return the current active hand list, or None if all hands are done."""
        idx = game["active_hand"]
        if idx >= len(game["hands"]):
            return None
        return game["hands"][idx]

    def _advance_hand(self, game: dict) -> bool:
        """Move to the next hand. Returns True if there's another hand to play."""
        game["active_hand"] += 1
        return game["active_hand"] < len(game["hands"])

    @commands.command()
    async def hit(self, ctx: commands.Context):
        """Draw another card in blackjack."""
        game = self.bj_games.get(ctx.author.id)
        if not game:
            await ctx.send(f"You don't have a blackjack game running. Start one with `{ctx.prefix}bj <bet>`.")
            return

        hand = self._active_hand(game)
        hand.append(game["deck"].pop())

        if card_value(hand) >= 21:
            if not self._advance_hand(game):
                await self._bj_finish(ctx, game)
                return
            multi = len(game["hands"]) > 1
            if multi:
                await ctx.send(
                    f"Hand {game['active_hand']} done. Now playing **Hand {game['active_hand']+1}**.",
                    embed=self._bj_embed(game),
                )
                return

        await ctx.send(embed=self._bj_embed(game))

    @commands.command()
    async def stand(self, ctx: commands.Context):
        """End your turn and let the dealer play."""
        game = self.bj_games.get(ctx.author.id)
        if not game:
            await ctx.send(f"You don't have a blackjack game running. Start one with `{ctx.prefix}bj <bet>`.")
            return

        if not self._advance_hand(game):
            await self._bj_finish(ctx, game)
            return

        await ctx.send(
            f"Now playing **Hand {game['active_hand']+1}**.",
            embed=self._bj_embed(game),
        )

    @commands.command()
    async def double(self, ctx: commands.Context):
        """Double your bet, draw one card, and stand."""
        game = self.bj_games.get(ctx.author.id)
        if not game:
            await ctx.send(f"You don't have a blackjack game running. Start one with `{ctx.prefix}bj <bet>`.")
            return

        idx = game["active_hand"]
        bet = game["bets"][idx]
        cash = await self.get_cash(ctx.author.id)
        if cash < bet:
            await ctx.send(f"You need **${bet:,}** more to double down but only have **${cash:,}**.")
            return

        await self.update_cash(ctx.author.id, -bet)
        game["bets"][idx] = bet * 2
        game["hands"][idx].append(game["deck"].pop())

        if not self._advance_hand(game):
            await self._bj_finish(ctx, game)
            return

        await ctx.send(
            f"Now playing **Hand {game['active_hand']+1}**.",
            embed=self._bj_embed(game),
        )

    @commands.command()
    async def split(self, ctx: commands.Context):
        """Split your hand into two. Only on first two cards of same rank."""
        game = self.bj_games.get(ctx.author.id)
        if not game:
            await ctx.send(f"You don't have a blackjack game running. Start one with `{ctx.prefix}bj <bet>`.")
            return

        idx = game["active_hand"]
        hand = game["hands"][idx]

        if len(hand) != 2:
            await ctx.send("You can only split on your first two cards.")
            return

        r1, r2 = hand[0][0], hand[1][0]
        if card_rank_value(r1) != card_rank_value(r2):
            await ctx.send("You can only split cards of the same rank.")
            return

        bet = game["bets"][idx]
        cash = await self.get_cash(ctx.author.id)
        if cash < bet:
            await ctx.send(f"You need **${bet:,}** more to split but only have **${cash:,}**.")
            return

        await self.update_cash(ctx.author.id, -bet)

        # Split into two hands, each gets one new card
        card1 = hand[0]
        card2 = hand[1]
        deck = game["deck"]
        hand_a = [card1, deck.pop()]
        hand_b = [card2, deck.pop()]

        game["hands"][idx] = hand_a
        game["hands"].insert(idx + 1, hand_b)
        game["bets"].insert(idx + 1, bet)

        await ctx.send(embed=self._bj_embed(game))

    async def _bj_finish(self, ctx: commands.Context, game: dict):
        """Dealer plays and resolve all hands."""
        hands = game["hands"]
        bets = game["bets"]
        dealer_hand = game["dealer_hand"]
        deck = game["deck"]

        # Check if any hand is still in play (not busted)
        any_alive = any(card_value(h) <= 21 for h in hands)
        if any_alive:
            while card_value(dealer_hand) < 17:
                dealer_hand.append(deck.pop())

        dealer_val = card_value(dealer_hand)
        dealer_bj = len(dealer_hand) == 2 and dealer_val == 21
        multi = len(hands) > 1

        total_payout = 0
        total_bet = sum(bets)
        results = []

        for i, (hand, bet) in enumerate(zip(hands, bets)):
            player_val = card_value(hand)
            player_bj = len(hand) == 2 and player_val == 21 and not multi

            if player_val > 21:
                result = "bust"
            elif dealer_val > 21:
                result = "win"
            elif player_bj and not dealer_bj:
                result = "blackjack"
            elif dealer_bj and not player_bj:
                result = "lose"
            elif player_bj and dealer_bj:
                result = "push"
            elif player_val > dealer_val:
                result = "win"
            elif player_val < dealer_val:
                result = "lose"
            else:
                result = "push"

            if result == "blackjack":
                payout = int(bet * 2.5)
            elif result == "win":
                payout = bet * 2
            elif result == "push":
                payout = bet
            else:
                payout = 0

            total_payout += payout
            results.append((hand, bet, payout, result, player_val))

        if total_payout > 0:
            await self.update_cash(ctx.author.id, total_payout)

        net = total_payout - total_bet
        sign = "+" if net >= 0 else ""
        color = discord.Color.green() if net > 0 else (discord.Color.light_grey() if net == 0 else discord.Color.red())

        if multi:
            title_parts = [r[3] for r in results]
            title = "Blackjack — Results"
        else:
            r = results[0][3]
            if r == "blackjack":
                title = "Blackjack \u2014 Blackjack! You win!"
            elif r == "win":
                title = "Blackjack \u2014 You win!"
            elif r == "push":
                title = "Blackjack \u2014 Push \u2014 it's a tie."
            elif r == "bust":
                title = f"Blackjack \u2014 Bust! ({results[0][4]})"
            else:
                title = "Blackjack \u2014 Dealer wins."

        embed = discord.Embed(title=title, color=color)

        for i, (hand, bet, payout, result, val) in enumerate(results):
            label = f"Hand {i+1}" if multi else "Your Hand"
            result_tag = {"blackjack": "BJ!", "win": "Win", "push": "Push", "bust": "Bust", "lose": "Lose"}[result]
            suffix = f" \u2014 **{result_tag}** (${payout:,})" if multi else ""
            embed.add_field(
                name=f"{label} ({val}){suffix}",
                value=format_hand(hand),
                inline=False,
            )

        embed.add_field(
            name=f"Dealer ({dealer_val})",
            value=format_hand(dealer_hand),
            inline=False,
        )
        embed.add_field(name="Total Bet", value=f"${total_bet:,}")
        embed.add_field(name="Payout", value=f"${total_payout:,}")
        embed.add_field(name="Net", value=f"{sign}${net:,}")
        await ctx.send(embed=embed)

        del self.bj_games[ctx.author.id]

    # --- Roulette (multiplayer, per-channel, auto-spin timer) ---

    DEFAULT_RL_TIMER = 15  # seconds after last bet before auto-spin

    @commands.command()
    async def rbet(self, ctx: commands.Context, bet_type: str, amount: int):
        """Place a roulette bet. The wheel auto-spins 15s after the last bet.

        Bet types:
          Number:  !rbet 17 100   — bet on a single number 0-36 (pays 36x)
          00:      !rbet 00 100   — bet on double zero (pays 36x)
          Green:   !rbet green 100 — bet on 0 or 00 (pays 18x)
          Red:     !rbet red 100  — bet on any red number (pays 2x)
          Black:   !rbet black 100 — bet on any black number (pays 2x)
          Odd:     !rbet odd 100  — bet on any odd number (pays 2x)
          Even:    !rbet even 100 — bet on any even number (pays 2x)
          Low:     !rbet low 100  — bet on 1-18 (pays 2x)
          High:    !rbet high 100 — bet on 19-36 (pays 2x)
          1st:     !rbet 1st 100  — first dozen, numbers 1-12 (pays 3x)
          2nd:     !rbet 2nd 100  — second dozen, numbers 13-24 (pays 3x)
          3rd:     !rbet 3rd 100  — third dozen, numbers 25-36 (pays 3x)
          Col1:    !rbet col1 100 — column 1: 1,4,7,10,13,16,19,22,25,28,31,34 (pays 3x)
          Col2:    !rbet col2 100 — column 2: 2,5,8,11,14,17,20,23,26,29,32,35 (pays 3x)
          Col3:    !rbet col3 100 — column 3: 3,6,9,12,15,18,21,24,27,30,33,36 (pays 3x)

        Multiple players can bet on the same spin. Place as many bets as you want!
        """
        category, detail = parse_roulette_bet(bet_type)
        if category is None:
            await ctx.send(detail)
            return

        if amount <= 0:
            await ctx.send("Bet must be positive.")
            return

        settings = await self.get_settings(ctx.guild.id)
        if amount < settings["min_bet"]:
            await ctx.send(f"Minimum bet is **${settings['min_bet']:,}**.")
            return
        if amount > settings["max_bet"]:
            await ctx.send(f"Maximum bet is **${settings['max_bet']:,}**.")
            return

        # Check player can afford this bet on top of existing bets at this table
        channel_id = ctx.channel.id
        table = self.rl_tables.get(channel_id)
        existing_user_total = 0
        if table:
            existing_user_total = sum(
                b[2] for b in table["bets"] if b[0] == ctx.author.id
            )

        cash = await self.get_cash(ctx.author.id)
        if existing_user_total + amount > cash:
            await ctx.send(
                f"Your total bets would be **${existing_user_total + amount:,}** "
                f"but you only have **${cash:,}**."
            )
            return

        # Deduct cash immediately
        await self.update_cash(ctx.author.id, -amount)

        # Create table if needed, or reset timer
        first_bet = table is None
        if first_bet:
            table = {
                "bets": [],  # list of (user_id, category, detail, amount, display_name)
                "spin_version": 0,
            }
            self.rl_tables[channel_id] = table

        table["bets"].append((ctx.author.id, category, detail, amount, ctx.author.display_name))
        table["spin_version"] += 1
        current_version = table["spin_version"]

        bet_desc = self._format_bet(category, detail)
        total_table = sum(b[3] for b in table["bets"])
        player_count = len(set(b[0] for b in table["bets"]))

        embed = discord.Embed(
            title="Roulette — Bet Placed",
            description=f"{ctx.author.mention}: **${amount:,}** on **{bet_desc}**",
            color=discord.Color.dark_gold(),
        )
        embed.set_footer(
            text=f"Table: ${total_table:,} from {player_count} player(s) | "
                 f"Spinning in {self.DEFAULT_RL_TIMER}s — !rbet to add more · !rclear to cancel"
        )
        await ctx.send(embed=embed)

        # Schedule auto-spin (cancels previous timer via version check)
        asyncio.create_task(self._rl_auto_spin(ctx, channel_id, current_version))

    async def _rl_auto_spin(self, ctx: commands.Context, channel_id: int, version: int):
        """Wait and spin if no new bets have been placed."""
        await asyncio.sleep(self.DEFAULT_RL_TIMER)

        table = self.rl_tables.get(channel_id)
        if table is None or table["spin_version"] != version:
            return  # Timer was reset by a new bet, or table was cleared

        await self._rl_resolve(ctx, channel_id)

    async def _rl_resolve(self, ctx: commands.Context, channel_id: int):
        """Spin the wheel and resolve all bets at the table."""
        table = self.rl_tables.pop(channel_id, None)
        if not table or not table["bets"]:
            return

        result = random.choice(ROULETTE_SLOTS)
        result_display = str(result)
        emoji = roulette_color_emoji(result)
        color_name = roulette_color(result)

        # Group results by player
        player_results: dict[int, list] = {}
        player_names: dict[int, str] = {}
        total_wagered = 0
        total_payout = 0

        for user_id, category, detail, amount, display_name in table["bets"]:
            player_names[user_id] = display_name
            if user_id not in player_results:
                player_results[user_id] = []

            bet_desc = self._format_bet(category, detail)
            won = check_roulette_win(category, detail, result)
            total_wagered += amount

            if won:
                multiplier = roulette_payout_multiplier(category)
                payout = amount * multiplier
                total_payout += payout
                player_results[user_id].append((bet_desc, amount, payout, True))
            else:
                player_results[user_id].append((bet_desc, amount, 0, False))

        # Pay out winners
        for user_id, results in player_results.items():
            user_payout = sum(r[2] for r in results)
            if user_payout > 0:
                await self.update_cash(user_id, user_payout)

        # Build embed
        embed = discord.Embed(
            title=f"Roulette — {emoji} {result_display} ({color_name})",
            color=discord.Color.dark_gold(),
        )

        for user_id, results in player_results.items():
            name = player_names[user_id]
            user_wagered = sum(r[1] for r in results)
            user_payout = sum(r[2] for r in results)
            user_net = user_payout - user_wagered
            sign = "+" if user_net >= 0 else ""

            lines = []
            for bet_desc, amount, payout, won in results:
                if won:
                    lines.append(f"\u2705 **{bet_desc}** ${amount:,} \u2192 **${payout:,}**")
                else:
                    lines.append(f"\u274c **{bet_desc}** ${amount:,} \u2192 $0")
            lines.append(f"**Net: {sign}${user_net:,}**")

            embed.add_field(
                name=name,
                value="\n".join(lines),
                inline=False,
            )

        await ctx.send(embed=embed)

    @commands.command()
    async def rclear(self, ctx: commands.Context):
        """Remove all your bets from the roulette table and get refunded."""
        channel_id = ctx.channel.id
        table = self.rl_tables.get(channel_id)

        if not table:
            await ctx.send("No roulette table active in this channel.")
            return

        user_bets = [b for b in table["bets"] if b[0] == ctx.author.id]
        if not user_bets:
            await ctx.send("You have no bets on this table.")
            return

        refund = sum(b[3] for b in user_bets)
        table["bets"] = [b for b in table["bets"] if b[0] != ctx.author.id]
        await self.update_cash(ctx.author.id, refund)

        # If table is now empty, clean it up
        if not table["bets"]:
            del self.rl_tables[channel_id]
            await ctx.send(f"Refunded **${refund:,}**. Table is now empty.")
        else:
            await ctx.send(f"Refunded **${refund:,}**. Your bets have been removed.")

    @staticmethod
    def _format_bet(category: str, detail) -> str:
        """Format a bet for display."""
        if category == "number":
            return f"#{detail}"
        if category == "color":
            return detail.capitalize()
        if category == "parity":
            return detail.capitalize()
        if category == "highlow":
            return "High (19-36)" if detail == "high" else "Low (1-18)"
        if category == "dozen":
            ranges = {"1st": "1-12", "2nd": "13-24", "3rd": "25-36"}
            return f"{detail} dozen ({ranges[detail]})"
        if category == "column":
            num = detail[-1]
            return f"Column {num}"
        if category == "green":
            return "Green (0/00)"
        return detail

    # --- Settings Commands (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def setminbet(self, ctx: commands.Context, amount: int):
        """Set the minimum bet. Server owner only."""
        if amount < 1:
            await ctx.send("Minimum bet must be at least $1.")
            return

        await self.db.execute(
            """INSERT INTO gambling_settings (guild_id, min_bet) VALUES (?, ?)
               ON CONFLICT(guild_id) DO UPDATE SET min_bet = ?""",
            (ctx.guild.id, amount, amount),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Setting Updated",
            description=f"Minimum bet set to **${amount:,}**.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    @commands.command()
    @is_guild_owner()
    async def setmaxbet(self, ctx: commands.Context, amount: int):
        """Set the maximum bet. Server owner only."""
        if amount < 1:
            await ctx.send("Maximum bet must be at least $1.")
            return

        await self.db.execute(
            """INSERT INTO gambling_settings (guild_id, max_bet) VALUES (?, ?)
               ON CONFLICT(guild_id) DO UPDATE SET max_bet = ?""",
            (ctx.guild.id, amount, amount),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Setting Updated",
            description=f"Maximum bet set to **${amount:,}**.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- Error Handler ---

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return

        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
            await ctx.send("Only the server owner can use this command.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Gambling(bot))
