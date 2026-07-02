from __future__ import annotations
import os
import secrets
from pathlib import Path

import discord
from discord.ext import commands

POSITION_SET_FILE = Path(__file__).parent / "basic1k.wr"


class WolfPosition:
    def __init__(self, pgn, eval_bounds):
        self.pgn = pgn
        self._eval_bounds = eval_bounds


class WolfPositionSet:
    def __init__(self, name, positions, filtered=False, eval_bounds=(None, None)):
        self.original_set_name = name
        self.filtered = filtered
        self.positions = positions
        self._eval_bounds = eval_bounds

    def len(self):
        return len(self.positions)

    def name(self):
        return self.original_set_name + (' (filtered)' if self.filtered else '')

    def filter(self, eval_bounds):
        new_positions = []
        filter_low, filter_high = eval_bounds

        for pos in self.positions:
            pos_low, pos_high = pos._eval_bounds
            assert pos_low <= pos_high, "Invalid position eval bounds"
            pos_mid = pos_low + (pos_high - pos_low) / 2
            satisfied = True
            if filter_low is not None and abs(pos_mid) <= filter_low:
                satisfied = False
            if filter_high is not None and abs(pos_mid) > filter_high:
                satisfied = False
            if satisfied:
                new_positions.append(pos)

        new_low, new_high = self._eval_bounds
        if eval_bounds[0] is not None and (new_low is None or new_low < eval_bounds[0]):
            new_low = eval_bounds[0]
        if eval_bounds[1] is not None and (new_high is None or new_high > eval_bounds[1]):
            new_high = eval_bounds[1]
        return WolfPositionSet(
            name=self.original_set_name,
            positions=new_positions,
            filtered=True,
            eval_bounds=(new_low, new_high))


def load_position_set(path):
    positions = []
    with open(path) as wr_set:
        for line in wr_set:
            line = line.strip()
            if not line:
                continue
            try:
                pgn, evaluation = line.split('|')
                pgn, evaluation = pgn.strip(), evaluation.strip()
                if evaluation.startswith('-M'):
                    ev = -1000.0
                elif evaluation.startswith('M'):
                    ev = 1000.0
                else:
                    ev = float(evaluation)
                position = WolfPosition(pgn=pgn, eval_bounds=(ev, ev))
                positions.append(position)
            except ValueError as e:
                print(f'Error while parsing position set:\n{e}')
                return None
    name = os.path.basename(path)
    if path.endswith('.wr'):
        name = name[:len(name) - len('.wr')]
    return WolfPositionSet(name=name, positions=positions)


class WolfRandom(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self._position_set: WolfPositionSet | None = None
        self._load_positions()

    def _load_positions(self):
        if POSITION_SET_FILE.exists():
            self._position_set = load_position_set(str(POSITION_SET_FILE))

    @commands.command(aliases=['wr'])
    async def wolfrandom(self, ctx, eval_bounds: str = None):
        """Pick a random position from the loaded set. Optional eval_bounds: [low]:high (e.g. 0.5:2.0) to filter by engine evaluation."""
        if self._position_set is None or self._position_set.len() == 0:
            await ctx.send("No positions loaded. Please populate `positions.wr`.")
            return

        position_set = self._position_set
        if eval_bounds is not None:
            try:
                low_str, high_str = eval_bounds.split(':')
                bounds = [None, None]
                bounds[1] = float(high_str)
                if bounds[1] < 0:
                    await ctx.send("Error: higher bound must be non-negative.")
                    return
                if low_str:
                    bounds[0] = float(low_str)
                    if not 0 <= bounds[0] < bounds[1]:
                        await ctx.send("Error: lower bound must be non-negative and less than the higher bound.")
                        return
                position_set = self._position_set.filter(eval_bounds=bounds)
            except ValueError:
                await ctx.send("Invalid eval bounds format. Use `[low]:high`, e.g. `0.5:2.0`.")
                return

        if position_set.len() == 0:
            await ctx.send("No positions match the given eval bounds.")
            return

        position = secrets.choice(position_set.positions)
        await ctx.send(
            f"**{position_set.name()}** — {position_set.len()} position(s) available.\n"
            f"The following moves have been decided:\n```\n{position.pgn}\n```"
        )
