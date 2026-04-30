from __future__ import annotations
import random
from pathlib import Path

import discord
from discord.ext import commands

MOVES_FILE = Path(__file__).parent / "moves.txt"


class WolfRandom(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self._moves: list[str] = []
        self._load_moves()

    def _load_moves(self):
        if MOVES_FILE.exists():
            self._moves = [line.strip() for line in MOVES_FILE.read_text().splitlines() if line.strip()]

    @commands.command()
    async def wolfrandom(self, ctx):
        """Pick a random set of first moves for the atomic chess variant."""
        if not self._moves:
            await ctx.send("No moves loaded. Please populate `moves.txt`.")
            return
        pick = random.choice(self._moves)
        await ctx.send(f"The following first moves have been decided: {pick}")
