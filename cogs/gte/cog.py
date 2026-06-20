from __future__ import annotations
import asyncio
import io
import re
import time

import aiohttp
import discord
from discord.ext import commands

from core.time_utils import parse_duration
from cogs.economy.db import ensure_wallet, update_wallet, lock_wallet, add_transaction


# ── Site extractors ──
# Each returns (white_rating, black_rating, gif_bytes, variant, time_control_str)
# or raises ValueError with an error message.

_LICHESS_GAME_RE = re.compile(r"lichess\.org/(\w{8})")


async def _extract_lichess(session: aiohttp.ClientSession, url: str):
    match = _LICHESS_GAME_RE.search(url)
    if not match:
        raise ValueError("Could not find a valid Lichess game ID in the link.")
    game_id = match.group(1)

    api_url = f"https://lichess.org/api/game/{game_id}"
    async with session.get(api_url, headers={"Accept": "application/json"}) as resp:
        if resp.status != 200:
            raise ValueError(f"Lichess API returned status {resp.status}. Is the link valid?")
        data = await resp.json()

    white = data.get("players", {}).get("white", {})
    black = data.get("players", {}).get("black", {})
    white_rating = white.get("rating")
    black_rating = black.get("rating")
    if white_rating is None or black_rating is None:
        raise ValueError("This game has no ratings (unrated or anonymous players).")

    variant = data.get("variant", "standard")
    clock = data.get("clock")
    if clock:
        initial = clock.get("initial", 0)
        inc = clock.get("increment", 0)
        tc_str = f"{initial // 60}+{inc}"
    else:
        tc_str = "correspondence"

    gif_url = (
        f"https://lichess1.org/game/export/gif/white/{game_id}.gif"
        f"?theme=purple&piece=caliente&players=false&ratings=false&glyphs=true&clocks=true"
    )
    async with session.get(gif_url) as resp:
        if resp.status != 200:
            raise ValueError("Failed to generate game GIF from Lichess.")
        gif_bytes = await resp.read()

    return white_rating, black_rating, gif_bytes, variant, tc_str


SITE_EXTRACTORS = {
    "lichess.org": _extract_lichess,
}


def _identify_site(url: str) -> str | None:
    for site in SITE_EXTRACTORS:
        if site in url:
            return site
    return None


_KNOWN_UNSUPPORTED = {"chess.com"}

_GUESS_RE = re.compile(r"^(\d{1,5})\s+vs\s+(\d{1,5})$", re.IGNORECASE)


class GTE(commands.Cog):
    """Guess the Elo — guess player ratings from a chess game GIF."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.games: dict[int, dict] = {}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers={"User-Agent": "MikuDiscordBot/1.0"}
            )
        return self._session

    async def cog_unload(self):
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def pool(self):
        return self.bot.pool

    # ── Command ──

    @commands.command()
    async def gte(self, ctx: commands.Context, lichess_link: str, award: int, duration: str = "60s"):
        """Start a Guess the Elo game. The award is paid from your wallet to the closest guesser. Guess format: <white_rating> vs <black_rating> (e.g. 1500 vs 1800). Usage: .gte <link> <award> [duration]. Example: .gte https://lichess.org/abc123 500 90s"""
        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

        if ctx.channel.id in self.games:
            await ctx.send("A GTE game is already running in this channel!", delete_after=10)
            return

        cur = self.bot.get_currency(ctx.guild.id)

        if award <= 0:
            await ctx.send("Award must be a positive amount.", delete_after=10)
            return

        delta = parse_duration(duration)
        if delta is None:
            await ctx.send("Invalid duration. Use e.g. `60s`, `2m`, `5m`.", delete_after=10)
            return
        seconds = int(delta.total_seconds())
        if seconds < 10 or seconds > 300:
            await ctx.send("Duration must be between 10 seconds and 5 minutes.", delete_after=10)
            return

        for site in _KNOWN_UNSUPPORTED:
            if site in lichess_link:
                await ctx.send(f"**{site}** support coming soon! Only Lichess is supported for now.")
                return

        site = _identify_site(lichess_link)
        if not site:
            await ctx.send("Unrecognised link. Currently supported: Lichess.")
            return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                bal = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                if bal["wallet"] < award:
                    await ctx.send(
                        f"You need {cur.emoji} **{award:,}** in your wallet to fund this game. "
                        f"(Have: {cur.emoji} **{bal['wallet']:,}**)"
                    )
                    return
                await update_wallet(conn, ctx.guild.id, ctx.author.id, -award)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, -award, "gte_fund",
                                      "Funded Guess the Elo game")

        session = await self._get_session()
        extractor = SITE_EXTRACTORS[site]
        try:
            white_rating, black_rating, gif_bytes, variant, tc_str = await extractor(session, lichess_link)
        except ValueError as e:
            async with self.pool.acquire() as conn:
                await update_wallet(conn, ctx.guild.id, ctx.author.id, award)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, award, "gte_refund",
                                      "GTE game failed to start — refund")
            await ctx.send(f"Error: {e}")
            return
        except Exception:
            async with self.pool.acquire() as conn:
                await update_wallet(conn, ctx.guild.id, ctx.author.id, award)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, award, "gte_refund",
                                      "GTE game failed to start — refund")
            await ctx.send("Failed to fetch game data. Please check the link and try again.")
            return

        self.games[ctx.channel.id] = {
            "guild_id": ctx.guild.id,
            "starter_id": ctx.author.id,
            "award": award,
            "white_rating": white_rating,
            "black_rating": black_rating,
            "variant": variant,
            "tc": tc_str,
            "guesses": {},
            "phase": "guessing",
            "duration": seconds,
        }

        gif_file = discord.File(io.BytesIO(gif_bytes), filename="game.gif")
        ends_at = int(time.time()) + seconds
        embed = discord.Embed(
            title="Guess the Elo!",
            description=(
                f"**Variant:** {variant.capitalize()} | **Time:** {tc_str}\n"
                f"**Award:** {cur.emoji} **{award:,}** (funded by {ctx.author.display_name})\n\n"
                f"Guess both players' ratings!\n"
                f"Type `<white> vs <black>` (e.g. `1500 vs 1800`)\n"
                f"Guessing ends <t:{ends_at}:R>"
            ),
            color=discord.Color.dark_green(),
        )
        embed.set_image(url="attachment://game.gif")
        await ctx.send(file=gif_file, embed=embed)

        asyncio.create_task(self._end_guessing(ctx.channel.id))

    # ── Phases ──

    async def _end_guessing(self, channel_id: int):
        game = self.games.get(channel_id)
        if not game:
            return
        await asyncio.sleep(game["duration"])

        game = self.games.get(channel_id)
        if not game or game["phase"] != "guessing":
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            self.games.pop(channel_id, None)
            return

        self.games.pop(channel_id)

        if not game["guesses"]:
            async with self.pool.acquire() as conn:
                await update_wallet(conn, game["guild_id"], game["starter_id"], game["award"])
                await add_transaction(conn, game["guild_id"], game["starter_id"], game["award"],
                                      "gte_refund", "GTE game — no guesses, refund")
            await channel.send("No one guessed! Award refunded to the game starter.")
            return

        await self._resolve(channel, game)

    async def _resolve(self, channel: discord.TextChannel, game: dict):
        cur = self.bot.get_currency(channel.guild.id)
        actual_w = game["white_rating"]
        actual_b = game["black_rating"]

        results = []
        for uid, (gw, gb) in game["guesses"].items():
            dist = abs(gw - actual_w) + abs(gb - actual_b)
            results.append((uid, gw, gb, dist))

        results.sort(key=lambda r: r[3])
        min_dist = results[0][3]
        winners = [r for r in results if r[3] == min_dist]

        award = game["award"]
        if winners:
            share = award // len(winners)
            for uid, _, _, _ in winners:
                if share > 0:
                    async with self.pool.acquire() as conn:
                        await update_wallet(conn, game["guild_id"], uid, share)
                        await add_transaction(conn, game["guild_id"], uid, share, "gte_win",
                                              f"GTE winner (share of {award})")

        embed = discord.Embed(
            title="Guess the Elo — Results!",
            color=discord.Color.dark_green(),
        )
        embed.add_field(
            name="Actual Ratings",
            value=f"**White:** {actual_w} | **Black:** {actual_b}",
            inline=False,
        )

        lines = []
        for uid, gw, gb, dist in results:
            member = channel.guild.get_member(uid)
            name = member.display_name if member else f"User {uid}"
            marker = " 🏆" if dist == min_dist else ""
            lines.append(f"**{name}** — guessed {gw} / {gb} (off by {dist}){marker}")

        embed.add_field(name="Guesses", value="\n".join(lines), inline=False)

        if len(winners) == 1:
            winner_member = channel.guild.get_member(winners[0][0])
            winner_name = winner_member.display_name if winner_member else f"User {winners[0][0]}"
            embed.add_field(
                name="Winner",
                value=f"**{winner_name}** wins {cur.emoji} **{award:,}**!",
                inline=False,
            )
        else:
            share = award // len(winners)
            winner_names = []
            for uid, _, _, _ in winners:
                m = channel.guild.get_member(uid)
                winner_names.append(m.display_name if m else f"User {uid}")
            embed.add_field(
                name="Tie!",
                value=f"**{', '.join(winner_names)}** split the award — {cur.emoji} **{share:,}** each.",
                inline=False,
            )

        await channel.send(embed=embed)

    # ── Message listener ──

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        game = self.games.get(message.channel.id)
        if not game or game["phase"] != "guessing":
            return

        if message.author.id == game["starter_id"]:
            return

        match = _GUESS_RE.match(message.content.strip())
        if not match:
            return

        white_guess = int(match.group(1))
        black_guess = int(match.group(2))

        game["guesses"][message.author.id] = (white_guess, black_guess)

        try:
            await message.add_reaction("✅")
        except (discord.Forbidden, discord.NotFound):
            pass

    # ── Error handler ──

    @gte.error
    async def gte_error(self, ctx, error):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send("Usage: `.gte <lichess_link> <award> [duration]`\nExample: `.gte https://lichess.org/7o8NKKnL 500 90s`")
        elif isinstance(error, commands.BadArgument):
            await ctx.send("Invalid argument. Award must be a number.")
