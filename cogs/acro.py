import asyncio
import random
import string
import discord
import aiosqlite
from discord.ext import commands
from utils import check_channel_allowed, log_tx

DB_PATH = "data/economy.db"

ACRO_SUBMIT_TIME = 90   # seconds to submit a phrase
ACRO_VOTE_TIME   = 30   # seconds to vote
ACRO_MIN_LETTERS = 3
ACRO_MAX_LETTERS = 5

CONSONANTS = "BCDFGHJKLMNPRSTVWZ"
VOWELS     = "AEIOU"

def _gen_letters(n: int) -> list[str]:
    """Generate n letters that are pronounceable enough (not all consonants)."""
    letters = []
    for i in range(n):
        # Sprinkle a vowel roughly every 2-3 letters
        if i > 0 and i % 2 == 1:
            letters.append(random.choice(VOWELS))
        else:
            letters.append(random.choice(CONSONANTS + VOWELS))
    random.shuffle(letters)
    return letters


class Acro(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None
        # channel_id -> game state dict
        self.games: dict[int, dict] = {}

    async def cog_check(self, ctx: commands.Context) -> bool:
        return await check_channel_allowed(
            self.db, ctx.guild.id, "gambling", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")

    async def cog_unload(self):
        if self.db:
            await self.db.close()

    async def get_cash(self, user_id: int) -> int:
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (user_id,)
        ) as cur:
            row = await cur.fetchone()
        return row[0] if row else 0

    async def update_cash(self, user_id: int, amount: int, source: str = "acro",
                          counterpart_id: int = None):
        await self.db.execute(
            "INSERT INTO economy (user_id, cash, bank) VALUES (?, 0, 0) "
            "ON CONFLICT(user_id) DO NOTHING",
            (user_id,),
        )
        await self.db.execute(
            "UPDATE economy SET cash = cash + ? WHERE user_id = ?",
            (amount, user_id),
        )
        await log_tx(self.db, user_id, amount, source, counterpart_id)
        await self.db.commit()

    # ── Command ──────────────────────────────────────────────────────

    @commands.command()
    async def acro(self, ctx: commands.Context, bet: int = 0):
        """Start an Acrophobia game. Players invent a phrase for random letters.
        Usage: .acro        — no bet
               .acro 200   — winner takes the pot"""
        channel_id = ctx.channel.id

        if channel_id in self.games:
            await ctx.send("A game is already running in this channel!")
            return

        if bet < 0:
            await ctx.send("Bet cannot be negative.")
            return

        if bet > 0:
            cash = await self.get_cash(ctx.author.id)
            if cash < bet:
                await ctx.send(f"You need **{bet:,}** \U0001f338 but only have **{cash:,}** \U0001f338.")
                return

        n_letters = random.randint(ACRO_MIN_LETTERS, ACRO_MAX_LETTERS)
        letters = _gen_letters(n_letters)
        letters_display = " - ".join(letters)

        game = {
            "letters": letters,
            "bet": bet,
            "starter": ctx.author,
            # user_id -> {"phrase": str, "msg_id": int}
            "submissions": {},
            # user_id -> voted_for_user_id
            "votes": {},
            "phase": "submit",
        }
        self.games[channel_id] = game

        bet_line = f"**Bet:** {bet:,} \U0001f338 per player — winner takes the pot!\n" if bet > 0 else ""
        embed = discord.Embed(
            title="\U0001f524 Acrophobia!",
            description=(
                f"Your letters are:\n"
                f"# {letters_display}\n\n"
                f"{bet_line}"
                f"DM **or** type your phrase here — your message will be deleted to keep it secret.\n"
                f"You have **{ACRO_SUBMIT_TIME}s** to submit!"
            ),
            color=discord.Color.blurple(),
        )
        embed.set_footer(text=f"Each word must start with the given letter, in order.")
        await ctx.send(embed=embed)

        # Collect submissions for ACRO_SUBMIT_TIME seconds via on_message listener
        await asyncio.sleep(ACRO_SUBMIT_TIME)

        # Check game wasn't cancelled
        game = self.games.get(channel_id)
        if game is None:
            return

        game["phase"] = "vote"

        submissions = game["submissions"]
        if len(submissions) < 2:
            # Not enough players
            await self._cancel_game(channel_id, ctx.channel,
                                    "Not enough submissions (need at least 2). Game cancelled.")
            return

        # Deduct bets now that we have enough players
        if bet > 0:
            bailed = []
            for uid in list(submissions.keys()):
                cash = await self.get_cash(uid)
                if cash < bet:
                    bailed.append(uid)
                else:
                    await self.update_cash(uid, -bet, "acro:buyin")
            for uid in bailed:
                del submissions[uid]
            if len(submissions) < 2:
                # Refund already-deducted bets — can't have a game with <2 payers
                for uid in submissions:
                    await self.update_cash(uid, bet, "acro:refund")
                await self._cancel_game(channel_id, ctx.channel,
                                        "Not enough players could afford the bet. Game cancelled.")
                return

        # Build numbered reveal list
        player_list = list(submissions.items())  # [(user_id, {"phrase": ...}), ...]
        random.shuffle(player_list)
        game["player_list"] = player_list  # ordered for voting

        pot = bet * len(player_list)

        lines = []
        for i, (uid, data) in enumerate(player_list, 1):
            lines.append(f"**{i}.** {data['phrase']}")

        pot_line = f"\nPot: **{pot:,}** \U0001f338" if bet > 0 else ""
        embed = discord.Embed(
            title="\U0001f5f3\ufe0f Vote for your favourite!",
            description=(
                "\n".join(lines) + "\n\n"
                f"Reply with the **number** of the phrase you like most.\n"
                f"You cannot vote for yourself. You have **{ACRO_VOTE_TIME}s**!" + pot_line
            ),
            color=discord.Color.gold(),
        )
        await ctx.send(embed=embed)

        await asyncio.sleep(ACRO_VOTE_TIME)

        game = self.games.get(channel_id)
        if game is None:
            return

        await self._resolve(ctx, game, channel_id)

    # ── Message listener ─────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot:
            return

        channel_id = message.channel.id
        game = self.games.get(channel_id)
        if game is None:
            return

        user_id = message.author.id
        letters = game["letters"]

        # --- Submission phase ---
        if game["phase"] == "submit":
            # Ignore bot commands
            if message.content.startswith(self.bot.command_prefix):
                return

            words = message.content.strip().split()
            if len(words) != len(letters):
                # Wrong word count — delete silently and ignore
                try:
                    await message.delete()
                except discord.HTTPException:
                    pass
                return

            # Check each word starts with the right letter
            if not all(w[0].upper() == l for w, l in zip(words, letters)):
                try:
                    await message.delete()
                except discord.HTTPException:
                    pass
                return

            # Valid submission
            game["submissions"][user_id] = {"phrase": message.content.strip()}
            try:
                await message.delete()
            except discord.HTTPException:
                pass

            # Acknowledge via DM so the user knows it was accepted
            try:
                await message.author.send(
                    f"\u2705 Got your submission: **{message.content.strip()}**"
                )
            except discord.Forbidden:
                pass
            return

        # --- Vote phase ---
        if game["phase"] == "vote":
            if message.content.startswith(self.bot.command_prefix):
                return

            player_list = game.get("player_list", [])
            try:
                choice = int(message.content.strip())
            except ValueError:
                return

            if choice < 1 or choice > len(player_list):
                return

            voted_for_uid = player_list[choice - 1][0]

            # Can't vote for yourself
            if voted_for_uid == user_id:
                try:
                    await message.delete()
                except discord.HTTPException:
                    pass
                try:
                    await message.author.send("\u274c You can't vote for yourself!")
                except discord.Forbidden:
                    pass
                return

            # Only one vote per person (overwrite allowed so they can change)
            game["votes"][user_id] = voted_for_uid

            try:
                await message.delete()
            except discord.HTTPException:
                pass

            # Confirm via DM
            try:
                await message.author.send(
                    f"\U0001f5f3\ufe0f Vote recorded for phrase **{choice}**!"
                )
            except discord.Forbidden:
                pass

    # ── Resolution ───────────────────────────────────────────────────

    async def _resolve(self, ctx: commands.Context, game: dict, channel_id: int):
        del self.games[channel_id]

        player_list = game["player_list"]   # [(uid, data), ...]
        votes       = game["votes"]         # voter_uid -> voted_for_uid
        bet         = game["bet"]
        submissions = game["submissions"]

        # Tally votes
        vote_counts: dict[int, int] = {uid: 0 for uid, _ in player_list}
        for voted_for in votes.values():
            if voted_for in vote_counts:
                vote_counts[voted_for] += 1

        # Penalty: players who didn't vote lose 1 point
        vote_penalties: dict[int, int] = {}
        for uid, _ in player_list:
            if uid not in votes:
                vote_penalties[uid] = -1

        # Final scores = votes received + penalty
        scores = {uid: vote_counts[uid] + vote_penalties.get(uid, 0) for uid, _ in player_list}

        max_score = max(scores.values())
        winners = [uid for uid, sc in scores.items() if sc == max_score]

        # Pay out pot
        if bet > 0 and max_score > 0:
            pot = bet * len(player_list)
            share = pot // len(winners)
            for uid in winners:
                await self.update_cash(uid, share, "acro:win")

        # Build results embed
        lines = []
        for i, (uid, data) in enumerate(player_list, 1):
            member = ctx.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            sc = scores[uid]
            voted_line = ""
            if uid in votes:
                voted_for = votes[uid]
                voted_idx = next((j+1 for j, (u, _) in enumerate(player_list) if u == voted_for), "?")
                voted_line = f" (voted #{voted_idx})"
            penalty = " ⚠️ -1 (didn't vote)" if uid in vote_penalties else ""
            winner_tag = " \U0001f3c6" if uid in winners and max_score > 0 else ""
            lines.append(
                f"**{i}.** {data['phrase']}\n"
                f"\u2003*{name}* — **{sc} vote{'s' if sc != 1 else ''}**{voted_line}{penalty}{winner_tag}"
            )

        if max_score <= 0:
            outcome = "No votes were cast — no winner!"
        elif len(winners) == 1:
            member = ctx.guild.get_member(winners[0])
            wname = member.display_name if member else f"User {winners[0]}"
            outcome = f"\U0001f3c6 **{wname}** wins!"
            if bet > 0:
                outcome += f" Takes **{bet * len(player_list):,}** \U0001f338!"
        else:
            names = [
                (ctx.guild.get_member(u).display_name if ctx.guild.get_member(u) else f"User {u}")
                for u in winners
            ]
            outcome = f"\U0001f91d It's a tie between **{', '.join(names)}**!"
            if bet > 0:
                share = (bet * len(player_list)) // len(winners)
                outcome += f" Each gets **{share:,}** \U0001f338!"

        embed = discord.Embed(
            title="\U0001f324\ufe0f Acrophobia — Results",
            description="\n\n".join(lines) + f"\n\n{outcome}",
            color=discord.Color.green() if max_score > 0 else discord.Color.light_grey(),
        )
        embed.set_footer(text="Letters: " + " - ".join(game["letters"]))
        await ctx.send(embed=embed)

    async def _cancel_game(self, channel_id: int, channel: discord.TextChannel, reason: str):
        self.games.pop(channel_id, None)
        await channel.send(reason)

    # ── Error Handler ────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return
        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Usage: `{ctx.prefix}acro [bet]`")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Acro(bot))
