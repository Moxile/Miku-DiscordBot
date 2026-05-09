import asyncio
import secrets
import string

import discord
from discord.ext import commands

from config import MAIN_CURRENCY_EMOJI
from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from core.money import parse_amount, AmountError


class Acro(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.games: dict[int, dict] = {}

    @property
    def pool(self):
        return self.bot.pool

    @commands.command()
    async def acro(self, ctx: commands.Context, bet: str = None):
        """Start an Acro game. A random acronym is generated; everyone has 60s to submit a matching phrase, then 30s of voting — highest votes wins the pot. (example: .acro 100 — start a game with a 100 coin entry fee)"""
        if ctx.channel.id in self.games:
            await ctx.send("An Acro game is already running in this channel!")
            return

        if bet is not None:
            try:
                bet = parse_amount(bet)
            except AmountError as e:
                await ctx.send(str(e))
                return

        letter_count = secrets.randbelow(3) + 3
        letters = [secrets.choice(string.ascii_uppercase) for _ in range(letter_count)]

        self.games[ctx.channel.id] = {
            "guild_id": ctx.guild.id,
            "letters": letters,
            "bet": bet,
            "phase": "guessing",
            "guesses": {},
            "votes": {},
            "participants": set(),
            "submission_order": [],
        }

        bet_text = f"\nEntry fee: {MAIN_CURRENCY_EMOJI} **{bet:,}** (deducted when you submit)" if bet else ""
        embed = discord.Embed(
            title="Acro Game Started!",
            description=(
                f"**Letters: {' '.join(letters)}**\n\n"
                f"Submit a phrase where each word starts with the corresponding letter.\n"
                f"You have **60 seconds** to submit your guess.{bet_text}"
            ),
            color=discord.Color.orange(),
        )
        embed.set_footer(text="Your message will be deleted to keep guesses anonymous.")
        await ctx.send(embed=embed)

        asyncio.create_task(self._end_guessing(ctx.channel.id))

    # ── Phases ──

    async def _end_guessing(self, channel_id: int):
        await asyncio.sleep(60)
        game = self.games.get(channel_id)
        if not game or game["phase"] != "guessing":
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            self.games.pop(channel_id, None)
            return

        if not game["participants"]:
            await self._cancel_game(channel, game, "No one submitted a phrase.")
            return

        if len(game["participants"]) == 1:
            self.games.pop(channel_id)
            await self._resolve(channel, game)
            return

        game["phase"] = "voting"

        lines = []
        for i, uid in enumerate(game["submission_order"], 1):
            lines.append(f"**{i}.** {game['guesses'][uid]}")

        embed = discord.Embed(
            title="Voting Time!",
            description=(
                f"**Letters: {' '.join(game['letters'])}**\n\n"
                + "\n".join(lines)
                + "\n\nType a number to vote. You have **30 seconds**."
            ),
            color=discord.Color.blue(),
        )
        embed.set_footer(text="Players who don't vote get -1 on their submission.")
        await channel.send(embed=embed)

        asyncio.create_task(self._end_voting(channel_id))

    async def _end_voting(self, channel_id: int):
        await asyncio.sleep(30)
        game = self.games.get(channel_id)
        if not game or game["phase"] != "voting":
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            self.games.pop(channel_id, None)
            return

        self.games.pop(channel_id)
        await self._resolve(channel, game)

    async def _resolve(self, channel: discord.TextChannel, game: dict):
        order = game["submission_order"]
        num_submissions = len(order)
        voters_who_are_players = {uid for uid in game["participants"]}

        vote_counts = [0] * num_submissions
        for voter_id, idx in game["votes"].items():
            vote_counts[idx] += 1

        if num_submissions > 1:
            players_who_voted = set(game["votes"].keys())
            for i, uid in enumerate(order):
                if uid in voters_who_are_players and uid not in players_who_voted:
                    vote_counts[i] -= 1

        max_votes = max(vote_counts)
        winners = [order[i] for i in range(num_submissions) if vote_counts[i] == max_votes]

        pot = (game["bet"] or 0) * len(game["participants"])
        if pot > 0 and winners:
            share = pot // len(winners)
            for uid in winners:
                if share > 0:
                    await update_wallet(self.pool, game["guild_id"], uid, share)
                    await add_transaction(self.pool, game["guild_id"], uid, share, "acro_win",
                                          f"Acro game winner (share of {pot})")

        embed = discord.Embed(
            title="Acro Results",
            description=f"**Letters: {' '.join(game['letters'])}**",
            color=discord.Color.green(),
        )

        results_lines = []
        for i, uid in enumerate(order):
            member = channel.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            marker = " 🏆" if uid in winners else ""
            if num_submissions == 1:
                results_lines.append(f"**1.** {game['guesses'][uid]} — *{name}*{marker}")
            else:
                results_lines.append(
                    f"**{i + 1}.** {game['guesses'][uid]} — *{name}* "
                    f"({vote_counts[i]} vote{'s' if vote_counts[i] != 1 else ''}){marker}"
                )
        embed.add_field(name="Submissions", value="\n".join(results_lines), inline=False)

        if pot > 0 and winners:
            share = pot // len(winners)
            winner_names = []
            for uid in winners:
                m = channel.guild.get_member(uid)
                winner_names.append(m.display_name if m else f"User {uid}")
            if len(winners) == 1:
                embed.add_field(
                    name="Winner",
                    value=f"**{winner_names[0]}** wins {MAIN_CURRENCY_EMOJI} **{pot:,}**!",
                    inline=False,
                )
            else:
                embed.add_field(
                    name="Tie!",
                    value=(
                        f"**{', '.join(winner_names)}** split the pot — "
                        f"{MAIN_CURRENCY_EMOJI} **{share:,}** each."
                    ),
                    inline=False,
                )
        elif not pot:
            winner_names = []
            for uid in winners:
                m = channel.guild.get_member(uid)
                winner_names.append(m.display_name if m else f"User {uid}")
            embed.add_field(
                name="Winner" if len(winners) == 1 else "Tie!",
                value=f"**{', '.join(winner_names)}** wins!",
                inline=False,
            )

        await channel.send(embed=embed)

    async def _cancel_game(self, channel: discord.TextChannel, game: dict, reason: str):
        if game["bet"] and game["participants"]:
            for uid in game["participants"]:
                await update_wallet(self.pool, game["guild_id"], uid, game["bet"])
                await add_transaction(self.pool, game["guild_id"], uid, game["bet"],
                                      "acro_refund", "Acro game cancelled — refund")

        self.games.pop(channel.id, None)
        await channel.send(f"Acro game cancelled: {reason} All bets have been refunded.")

    # ── Message listener ──

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        game = self.games.get(message.channel.id)
        if not game:
            return

        if game["phase"] == "guessing":
            await self._handle_guess(message, game)
        elif game["phase"] == "voting":
            await self._handle_vote(message, game)

    async def _handle_guess(self, message: discord.Message, game: dict):
        letters = game["letters"]
        words = message.content.strip().split()

        if len(words) != len(letters):
            return
        for word, letter in zip(words, letters):
            if not word[0].upper() == letter:
                return

        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

        uid = message.author.id
        guild_id = game["guild_id"]

        if uid in game["participants"]:
            game["guesses"][uid] = message.content.strip()
            return

        if game["bet"]:
            async with self.pool.acquire() as conn:
                await ensure_wallet(conn, guild_id, uid)
                bal = await conn.fetchrow(
                    "SELECT * FROM balances WHERE guild_id = $1 AND user_id = $2 FOR UPDATE",
                    guild_id, uid,
                )
                if bal["wallet"] < game["bet"]:
                    await message.channel.send(
                        f"{message.author.mention} you need {MAIN_CURRENCY_EMOJI} **{game['bet']:,}** "
                        f"in your wallet to play. (Have: {MAIN_CURRENCY_EMOJI} **{bal['wallet']:,}**)",
                        delete_after=10,
                    )
                    return
                await update_wallet(conn, guild_id, uid, -game["bet"])
                await add_transaction(conn, guild_id, uid, -game["bet"], "acro_bet", "Acro game entry")

        game["participants"].add(uid)
        game["guesses"][uid] = message.content.strip()
        game["submission_order"].append(uid)

    async def _handle_vote(self, message: discord.Message, game: dict):
        content = message.content.strip()
        if not content.isdigit():
            return

        choice = int(content)
        num_submissions = len(game["submission_order"])

        if choice < 1 or choice > num_submissions:
            return

        voter_id = message.author.id
        voted_for_uid = game["submission_order"][choice - 1]

        if voter_id == voted_for_uid:
            try:
                await message.channel.send(
                    f"{message.author.mention} you can't vote for your own submission!",
                    delete_after=5,
                )
            except discord.Forbidden:
                pass
            return

        if voter_id in game["votes"]:
            return

        game["votes"][voter_id] = choice - 1
