import asyncio

from datetime import timedelta

import discord
from discord.ext import commands
from discord import app_commands

from cogs.utils.db import ensure_wallet, update_wallet, update_bank, add_transaction
from cogs.utils.checks import require_channel, WrongChannel, invalidate
from cogs.utils.money import parse_amount, AmountError
from config import MAIN_CURRENCY_EMOJI, CURRENCY_NAME, PREFIX

import random

class Gambling(commands.Cog):
    
    ROULETTE_RED = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

    def __init__(self, bot):
        self.bot = bot
        self.games = {} # (guild_id, user_id) -> game_state, ("roulette", channel_id) -> roulette state

    @property
    def pool(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        if isinstance(error, WrongChannel):
            await ctx.send(str(error), delete_after=10)
        else:
            raise error

    async def check_bet(self, ctx, amount, min_amount=2):
        # helper function for validating wallet has enough to fund a bet
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
        results = []
        for _ in range(tries):
            results.append("H" if random.random() < 0.5 else "T")
        
        embed = discord.Embed(title="Coin Flip Results", description="\n".join(results), color=discord.Color.blue())
        await ctx.send(embed=embed)

    @commands.command(aliases=["bf"])
    @require_channel("gambling_channel")
    async def betflip(self, ctx, choice, bet_per_try: str, tries: int = 1):
        """Bet on heads or tails. Specify your choice, how much to bet per try, and how many times to flip. (example: .betflip h 10 5 - bet 10 on heads per flip for 5 flips)"""
        # validate inputs
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

        # perform the bets
        results = []
        total = 0
        for _ in range(tries):
            result = "H" if random.random() < 0.5 else "T"

            if result == choice.upper():
                total += bet_per_try
            else:
                total -= bet_per_try
            results.append(result)
        
        if total > 0:
            total = int(0.8 * total)
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, total)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, total, "betflip", f"{tries} tries at {bet_per_try}{MAIN_CURRENCY_EMOJI} each")

        # show results
        embed = discord.Embed(title="Bet Flip Results", description="\n".join(results) + f"\nTotal outcome: {total}{MAIN_CURRENCY_EMOJI}", color=discord.Color.blue())
        await ctx.send(embed=embed)

    @staticmethod
    def create_deck(deckcount=0):
        suits = ["♠️", "♥️", "♦️", "♣️"]
        values = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"]
        deck = [(value, suit) for suit in suits for value in values]
        if deckcount > 0:
            deck *= deckcount
        random.shuffle(deck)
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
   
    def format_hand(self, cards, hide_second=False):
        if hide_second:
            first = f"{cards[0][0]}{cards[0][1]}"
            return f"{first} ??"
        display = " ".join(f"{rank}{suit}" for rank, suit in cards)
        return f"{display} ({self.calculate_hand_value(cards)})"

    def build_game_embed(self, game, title, color, hide_dealer=True, result=None):
        embed = discord.Embed(title=f"Blackjack - {title}", color=color)
        embed.add_field(name="Dealer Hand", value=self.format_hand(game["dealer_cards"], hide_second=hide_dealer), inline=False)
        embed.add_field(name="Your Hand", value=self.format_hand(game["player_hands"][game["current_hand"]]), inline=False)
        if result is not None:
            sign = "+" if result >= 0 else ""
            embed.set_footer(text=f"{sign}{result}{MAIN_CURRENCY_EMOJI}")
        else:
            hand_bet = game["hand_bets"][game["current_hand"]]
            embed.set_footer(text=f"Bet: {hand_bet}{MAIN_CURRENCY_EMOJI}")
        return embed

    @commands.command(aliases=["bj"])
    @require_channel("gambling_channel")
    async def blackjack(self, ctx, bet: str):
        """Start a game of blackjack by placing a bet. You can specify an amount or use 'all' to bet everything. During the game, use .hit, .stand, .double, or .split."""
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

        if (ctx.guild.id, ctx.author.id) in self.games:
            await ctx.send("You already have an active game! Please finish it before starting a new one.")
            return

        # take money
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)

        # initialize game state
        self.games[(ctx.guild.id, ctx.author.id)] = {
            "game": "blackjack",
            "bet": bet,
            "current_hand": 0,
            "player_hands": [],
            "hand_bets": [],
            "dealer_cards": [],
            "deck": self.create_deck(),
            "state": "player_turn"
        }

        game = self.games[(ctx.guild.id, ctx.author.id)]

        # represent the initial deal
        player_cards = []
        player_cards.append(game["deck"].pop())
        game["dealer_cards"].append(game["deck"].pop())
        player_cards.append(game["deck"].pop())
        game["dealer_cards"].append(game["deck"].pop())

        game["player_hands"].append(player_cards)
        game["hand_bets"].append(bet)
        player_value = self.calculate_hand_value(game["player_hands"][0])

        if player_value == 21:
            # Player blackjack — go straight to dealer turn to reveal outcome
            game["state"] = "dealer_turn"
            await self.dealer_turn(ctx, game)
            return
        embed = self.build_game_embed(game, "Game Started", discord.Color.blue())
        await ctx.send(embed=embed)

    async def dealer_turn(self, ctx, game):
        while self.calculate_hand_value(game["dealer_cards"]) < 17:
            game["dealer_cards"].append(game["deck"].pop())

        dealer_value = self.calculate_hand_value(game["dealer_cards"])
        dealer_blackjack = dealer_value == 21 and len(game["dealer_cards"]) == 2
        total_result = 0
        game["current_hand"] = 0

        # Resolve each remaining hand
        for i, hand in enumerate(game["player_hands"]):
            hand_bet = game["hand_bets"][i]
            player_value = self.calculate_hand_value(hand)
            player_blackjack = player_value == 21 and len(hand) == 2

            if player_value > 21:
                # Already busted, already deducted
                total_result -= hand_bet
                continue
            if dealer_blackjack and not player_blackjack:
                # Dealer natural beats non-natural 21
                total_result -= hand_bet
            elif player_blackjack and not dealer_blackjack:
                # Player natural blackjack pays 3:2
                winnings = int(hand_bet * 2.5)
                await update_wallet(self.pool, ctx.guild.id, ctx.author.id, winnings)
                total_result += winnings - hand_bet
            elif dealer_value > 21 or player_value > dealer_value:
                await update_wallet(self.pool, ctx.guild.id, ctx.author.id, 2 * hand_bet)
                total_result += hand_bet
            elif player_value < dealer_value:
                total_result -= hand_bet
            else:
                await update_wallet(self.pool, ctx.guild.id, ctx.author.id, hand_bet)

        if total_result > 0:
            embed = self.build_game_embed(game, "Win", discord.Color.green(), hide_dealer=False, result=total_result)
        elif total_result < 0:
            embed = self.build_game_embed(game, "Loss", discord.Color.red(), hide_dealer=False, result=total_result)
        else:
            embed = self.build_game_embed(game, "Push", discord.Color.dark_gray(), hide_dealer=False, result=0)

        # Log a single transaction for the entire game
        if total_result != 0:
            tx_type = "blackjack_win" if total_result > 0 else "blackjack_loss"
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, total_result, tx_type)

        await ctx.send(embed=embed)
        del self.games[(ctx.guild.id, ctx.author.id)]

    @commands.command()
    @require_channel("gambling_channel")
    async def hit(self, ctx):
        """Take a hit and get another card. Only works during your turn in an active blackjack game."""
        if (ctx.guild.id, ctx.author.id) not in self.games or self.games[(ctx.guild.id, ctx.author.id)]["game"] != "blackjack":
            return
        
        game = self.games[(ctx.guild.id, ctx.author.id)]

        if game["state"] == "player_turn":
            game["player_hands"][game["current_hand"]].append(game["deck"].pop())
            player_value = self.calculate_hand_value(game["player_hands"][game["current_hand"]])
            if player_value > 21:
                hand_bet = game["hand_bets"][game["current_hand"]]
                embed = self.build_game_embed(game, "Busted", discord.Color.red(), hide_dealer=False, result=-hand_bet)
                await ctx.send(embed=embed)
                game["current_hand"] += 1

                if game["current_hand"] >= len(game["player_hands"]):
                    # Check if all hands busted
                    all_busted = all(
                        self.calculate_hand_value(h) > 21 for h in game["player_hands"]
                    )
                    if all_busted:
                        total_loss = -sum(game["hand_bets"])
                        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, total_loss, "blackjack_loss")
                        del self.games[(ctx.guild.id, ctx.author.id)]
                    else:
                        game["state"] = "dealer_turn"
                        await self.dealer_turn(ctx, game)
            else:
                embed = self.build_game_embed(game, "Hit", discord.Color.green())
                await ctx.send(embed=embed)

    @commands.command()
    @require_channel("gambling_channel")
    async def stand(self, ctx):
        """Stand and end your turn. Only works during your turn in an active blackjack game."""
        if (ctx.guild.id, ctx.author.id) not in self.games or self.games[(ctx.guild.id, ctx.author.id)]["game"] != "blackjack":
            return
        
        game = self.games[(ctx.guild.id, ctx.author.id)]

        if game["state"] == "player_turn":
            # If more hands to play, advance to next hand
            if game["current_hand"] < len(game["player_hands"]) - 1:
                game["current_hand"] += 1
                embed = self.build_game_embed(game, f"Stand - Hand {game['current_hand'] + 1}", discord.Color.blue())
                await ctx.send(embed=embed)
                return

            # All hands played, dealer's turn
            game["state"] = "dealer_turn"
            await self.dealer_turn(ctx, game)
            

    @commands.command()
    @require_channel("gambling_channel")
    async def split(self, ctx):
        """Split your hand into two separate hands if you have a pair. Only works during your turn in an active blackjack game."""
        if (ctx.guild.id, ctx.author.id) not in self.games or self.games[(ctx.guild.id, ctx.author.id)]["game"] != "blackjack":
            return

        game = self.games[(ctx.guild.id, ctx.author.id)]

        current_hand = game["player_hands"][game["current_hand"]]

        if len(current_hand) != 2:
            return

        if current_hand[0][0] != current_hand[1][0]:
            return

        # Check if they can afford the second bet
        is_valid, error = await self.check_bet(ctx, game["bet"])
        if not is_valid:
            await ctx.send(error)
            return

        # Deduct second bet
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -game["bet"])

        # Split into two hands
        card1 = current_hand[0]
        card2 = current_hand[1]
        idx = game["current_hand"]
        original_bet = game["hand_bets"][idx]
        game["player_hands"][idx:idx+1] = [
            [card1, game["deck"].pop()],
            [card2, game["deck"].pop()],
        ]
        game["hand_bets"][idx:idx+1] = [original_bet, original_bet]
        game["current_hand"] = idx

        embed = self.build_game_embed(game, "Split - Hand 1", discord.Color.blue())
        await ctx.send(embed=embed)


    @commands.command()
    @require_channel("gambling_channel")
    async def double(self, ctx):
        """Double your bet and take exactly one more card. Only works during your turn in an active blackjack game and only if you have exactly 2 cards in your hand."""
        if (ctx.guild.id, ctx.author.id) not in self.games or self.games[(ctx.guild.id, ctx.author.id)]["game"] != "blackjack":
            return

        if self.games[(ctx.guild.id, ctx.author.id)]["state"] != "player_turn":
            return
        

        game = self.games[(ctx.guild.id, ctx.author.id)]

        if len(game["player_hands"][game["current_hand"]]) != 2:
            return

        # Check if they can afford to double
        is_valid, error = await self.check_bet(ctx, game["bet"])
        if not is_valid:
            await ctx.send(error)
            return

        # Deduct additional bet
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -game["bet"])
        game["hand_bets"][game["current_hand"]] *= 2

        # Player gets one card and then stands
        game["player_hands"][game["current_hand"]].append(game["deck"].pop())
        player_value = self.calculate_hand_value(game["player_hands"][game["current_hand"]])
        hand_bet = game["hand_bets"][game["current_hand"]]

        if player_value > 21:
            embed = self.build_game_embed(game, "Double Down - Busted", discord.Color.red(), hide_dealer=False, result=-hand_bet)
            await ctx.send(embed=embed)

            # Advance to next hand or dealer turn
            game["current_hand"] += 1
            if game["current_hand"] >= len(game["player_hands"]):
                all_busted = all(
                    self.calculate_hand_value(h) > 21 for h in game["player_hands"]
                )
                if all_busted:
                    total_loss = -sum(game["hand_bets"])
                    await add_transaction(self.pool, ctx.guild.id, ctx.author.id, total_loss, "blackjack_loss")
                    del self.games[(ctx.guild.id, ctx.author.id)]
                else:
                    game["state"] = "dealer_turn"
                    await self.dealer_turn(ctx, game)
            return

        embed = self.build_game_embed(game, "Double Down", discord.Color.orange())
        await ctx.send(embed=embed)

        # Advance to next hand or dealer turn
        game["current_hand"] += 1
        if game["current_hand"] >= len(game["player_hands"]):
            game["state"] = "dealer_turn"
            await self.dealer_turn(ctx, game)

    @commands.command()
    @require_channel("gambling_channel")
    async def roulette(self, ctx, choice, bet: str):
        """Play a game of roulette by placing a bet on a color, odd/even, or specific number. You can specify an amount or use 'all' to bet everything. (example: .roulette red 10 - bet 10 on red)"""
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

        choice = choice.lower()
        valid_choices = ["red", "black", "odd", "even"] + [str(i) for i in range(37)]
        if choice not in valid_choices:
            await ctx.send("Invalid choice! Please choose 'red', 'black', 'odd', 'even', or a number between 0 and 36.")
            return

        # take money
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)

        key = ("roulette", ctx.channel.id)
        new_round = key not in self.games

        if new_round:
            self.games[key] = {
                "game": "roulette",
                "guild_id": ctx.guild.id,
                "bets": {},  # user_id -> [(choice, bet), ...]
                "version": 0
            }

        game = self.games[key]
        user_bets = game["bets"].setdefault(ctx.author.id, [])
        user_bets.append((choice, bet))
        game["version"] += 1

        await ctx.send(f"{ctx.author.display_name} bet {bet}{MAIN_CURRENCY_EMOJI} on **{choice}**! Spinning in **15 seconds**...")
        asyncio.create_task(self.spin_roulette(ctx.channel, key, game["version"]))

    async def spin_roulette(self, channel, key, version):
        await asyncio.sleep(15)

        game = self.games.get(key)
        if not game or game["version"] != version:
            # A newer bet reset the timer, this coroutine is stale
            return
        self.games.pop(key)

        result = random.randint(0, 36)
        color = "green" if result == 0 else ("red" if result in self.ROULETTE_RED else "black")

        embed = discord.Embed(
            title=f"Roulette Result: {result} ({color})",
            color=discord.Color.green() if result == 0 else (discord.Color.red() if color == "red" else discord.Color.dark_gray())
        )

        # Resolve each player's bets
        for user_id, bets in game["bets"].items():
            total_result = 0
            for choice, bet in bets:
                winnings = self.resolve_roulette_bet(choice, bet, result, color)
                total_result += winnings

            if total_result > 0:
                await update_wallet(self.pool, game["guild_id"], user_id, total_result)

            net = total_result - sum(b for _, b in bets)
            if net != 0:
                tx_type = "roulette_win" if net > 0 else "roulette_loss"
                await add_transaction(self.pool, game["guild_id"], user_id, net, tx_type)

            member = channel.guild.get_member(user_id)
            name = member.display_name if member else str(user_id)
            sign = "+" if net >= 0 else ""
            embed.add_field(name=name, value=f"{sign}{net}{MAIN_CURRENCY_EMOJI}", inline=True)

        await channel.send(embed=embed)

    @staticmethod
    def resolve_roulette_bet(choice, bet, result, color):
        """Returns the payout (0 if lost, includes original bet if won)."""
        if choice == "red" and color == "red":
            return bet * 2
        elif choice == "black" and color == "black":
            return bet * 2
        elif choice == "odd" and result != 0 and result % 2 == 1:
            return bet * 2
        elif choice == "even" and result != 0 and result % 2 == 0:
            return bet * 2
        elif choice.isdigit() and int(choice) == result:
            return bet * 36
        return 0
    
    @commands.command(aliases=["rr"])
    @require_channel("gambling_channel")
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
        
        # check if game is already active for channel
        game = self.games.get(("russian_roulette", ctx.channel.id))

        if not game:
            # initialize game state
            self.games[("russian_roulette", ctx.channel.id)] = {
                "game": "russian_roulette",
                "guild_id": ctx.guild.id,
                "players": [],
                "bet": bet,
                "version": 0
            }
        else:
            if ctx.author.id in self.games[("russian_roulette", ctx.channel.id)]["players"]:
                await ctx.send("You have already joined this round of Russian Roulette!")
                return
            
            if game["bet"] != bet:
                await ctx.send(f"The current bet for this round is {game['bet']}{MAIN_CURRENCY_EMOJI}. Please match that to join.")
                return
        

        # take money
        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -bet)

        self.games[("russian_roulette", ctx.channel.id)]["version"] += 1
        self.games[("russian_roulette", ctx.channel.id)]["players"].append(ctx.author.id)

        embed=discord.Embed(
            title="Russian Roulette",
            description=f"{ctx.author.display_name} has joined the game with a bet of {bet}{MAIN_CURRENCY_EMOJI}!\n\nType `{PREFIX}rr {bet}` to join. Spinning in **30 seconds**...",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="Players Joined", value="\n".join(
            f"- {ctx.guild.get_member(pid).display_name if ctx.guild.get_member(pid) else str(pid)}" for pid in self.games[("russian_roulette", ctx.channel.id)]["players"]
        ), inline=False)
        await ctx.send(embed=embed)

        asyncio.create_task(self.spin_russian_roulette(ctx.channel, ("russian_roulette", ctx.channel.id), self.games[("russian_roulette", ctx.channel.id)]["version"]))

        

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
                if random.random() < chance_hit:
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


async def setup(bot):
    await bot.add_cog(Gambling(bot))
