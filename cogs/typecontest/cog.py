import asyncio
import random
import time

import discord
from discord.ext import commands

from config import PREFIX
from core.names import format_name
from .texts import SNIPPETS
from .utils import edit_distance, salt_text, ZW_PATTERN

TIMEOUT_SECONDS = 180
MEDALS = ["🥇", "🥈", "🥉"]


class TypeContest(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.games: dict[int, dict] = {}

    @commands.command(name="typestart")
    async def typestart(self, ctx: commands.Context, category: str = None):
        """Start a typing contest. Optionally pick a category (example: .typestart code)."""
        if ctx.channel.id in self.games:
            await ctx.send("A typing contest is already running in this channel! Use `.typecontest` to stop it.")
            return

        if category:
            category = category.lower()
            if category not in SNIPPETS:
                available = ", ".join(sorted(SNIPPETS))
                await ctx.send(f"Unknown category `{category}`. Available categories: {available}")
                return
        else:
            category = random.choice(list(SNIPPETS))

        text = random.choice(SNIPPETS[category])
        salted = salt_text(text)

        self.games[ctx.channel.id] = {
            "text": text,
            "category": category,
            "start": time.monotonic(),
            "finishers": [],
            "finished_ids": set(),
            "task": None,
        }

        embed = discord.Embed(
            title="⌨️ Typing Contest!",
            description=(
                f"Category: **{category}**\n\n"
                f"Retype the text below as fast and accurately as you can:\n\n"
                f"```\n{salted}\n```"
            ),
            color=discord.Color.gold(),
        )
        embed.set_footer(text="Copy-pasting won't work — type it out yourself! Use .typecontest to end early.")
        await ctx.send(embed=embed)

        self.games[ctx.channel.id]["task"] = asyncio.create_task(self._auto_end(ctx.channel.id))

    @commands.command(name="typecontest")
    async def typecontest_stop(self, ctx: commands.Context):
        """Stop the typing contest running in this channel and show the results."""
        game = self.games.pop(ctx.channel.id, None)
        if not game:
            await ctx.send("No typing contest is running in this channel.")
            return

        task = game.get("task")
        if task:
            task.cancel()

        await self._post_results(ctx.channel, game, "Contest stopped.")

    async def _auto_end(self, channel_id: int):
        await asyncio.sleep(TIMEOUT_SECONDS)
        game = self.games.pop(channel_id, None)
        if not game:
            return
        channel = self.bot.get_channel(channel_id)
        if channel:
            await self._post_results(channel, game, "Time's up!")

    async def _post_results(self, channel: discord.TextChannel, game: dict, reason: str):
        embed = discord.Embed(title="⌨️ Typing Contest Results", description=reason, color=discord.Color.green())

        if not game["finishers"]:
            embed.add_field(name="Finishers", value="No one finished in time!", inline=False)
        else:
            lines = []
            for i, (uid, wpm, mistakes) in enumerate(game["finishers"]):
                member = channel.guild.get_member(uid)
                name = format_name(member, channel.guild, fallback=f"User {uid}")
                medal = MEDALS[i] if i < len(MEDALS) else f"`{i + 1}.`"
                lines.append(
                    f"{medal} **{name}** — {wpm:.0f} WPM, {mistakes} mistake{'s' if mistakes != 1 else ''}"
                )
            embed.add_field(name="Finishers", value="\n".join(lines), inline=False)

        embed.add_field(name="Original Text", value=f"```\n{game['text']}\n```", inline=False)
        await channel.send(embed=embed)

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        game = self.games.get(message.channel.id)
        if not game:
            return

        content = message.content
        if content.startswith(PREFIX):
            return

        if message.author.id in game["finished_ids"]:
            return

        target = game["text"]
        if len(content) < int(len(target) * 0.8):
            return

        if ZW_PATTERN.search(content):
            try:
                await message.reply(
                    "Nice try, but copy-pasting won't work here — type it out yourself!",
                    mention_author=False,
                    delete_after=8,
                )
            except discord.Forbidden:
                pass
            return

        mistakes = edit_distance(content.strip(), target)
        elapsed_minutes = max(time.monotonic() - game["start"], 0.01) / 60
        word_count = len(target.split())
        wpm = word_count / elapsed_minutes

        game["finished_ids"].add(message.author.id)
        rank = len(game["finishers"]) + 1
        game["finishers"].append((message.author.id, wpm, mistakes))

        ordinal = {1: "first", 2: "second", 3: "third"}.get(rank, f"{rank}th")
        try:
            await message.reply(
                f"You finished **{ordinal}**! {wpm:.0f} WPM, {mistakes} mistake{'s' if mistakes != 1 else ''}.",
                mention_author=False,
            )
        except discord.Forbidden:
            pass
