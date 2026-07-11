from __future__ import annotations
import asyncio
import io
from pathlib import Path

import discord
from discord.ext import commands

from . import wr

DB_FILE = Path(__file__).parent / "basic44k.wr"
DEFAULT_MAX_EVAL = 8.0

BOUNDS_FORMAT_ERROR = (
    "Invalid eval bounds format. Use `:high` (symmetric), `low:high` (asymmetric), "
    "or `:low:high` (symmetric pair), e.g. `:0.5`, `-1.2:6.7`, `:9.0:15.0`.")


def _parse_bounds(s: str) -> wr.EvalRange:
    """Parse the friendly bounds syntax used by the `.wr` command:
        :high        -> symmetric interval   [-high, high],            high >= 0
        low:high     -> asymmetric interval  [low, high],               low <= high
        :low:high    -> symmetric pair       [-high,-low] & [low,high], 0 <= low <= high
    Raises ValueError with a user-facing message on bad input.
    """
    parts = s.split(':')

    def to_float(token):
        try:
            return float(token)
        except ValueError:
            raise ValueError(BOUNDS_FORMAT_ERROR)

    if len(parts) == 2:
        low_str, high_str = parts
        if low_str == '':
            high = to_float(high_str)
            if high < 0:
                raise ValueError("Error: bound must be non-negative.")
            return wr.EvalRange(-high, high)
        low, high = to_float(low_str), to_float(high_str)
        if low > high:
            raise ValueError("Error: lower bound must be less than or equal to upper bound.")
        return wr.EvalRange(low, high)

    if len(parts) == 3 and parts[0] == '':
        low, high = to_float(parts[1]), to_float(parts[2])
        if not 0 <= low <= high:
            raise ValueError("Error: bounds must satisfy 0 <= low <= high.")
        return wr.EvalRange(low, high, allow_inverse_evals=True)

    raise ValueError(BOUNDS_FORMAT_ERROR)


class WolfRandom(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    @commands.command(aliases=['wr'])
    async def wolfrandom(self, ctx, *args):
        """Pick a random position. Optional: -eval to show eval, -print to list all matches instead of
        picking one, and an eval bounds filter: `:high` for [-high,high], `low:high` for [low,high],
        or `:low:high` for [-high,-low] & [low,high]."""
        if not DB_FILE.exists():
            await ctx.send("No positions loaded.")
            return

        show_eval = '-eval' in args
        show_print = '-print' in args
        remaining = [a for a in args if a not in ('-eval', '-print')]
        eval_bounds_str = remaining[0] if remaining else None

        if eval_bounds_str is None:
            eval_range = wr.EvalRange(-DEFAULT_MAX_EVAL, DEFAULT_MAX_EVAL)
        else:
            try:
                eval_range = _parse_bounds(eval_bounds_str)
            except ValueError as e:
                await ctx.send(str(e))
                return

        if show_print:
            buffer = io.StringIO()
            result = await asyncio.to_thread(
                wr.wolfrandom, str(DB_FILE), eval_range, 'print', buffer, show_eval)

            if result['filtered'] == 0:
                await ctx.send("No positions match the given eval bounds.")
                return

            buffer.seek(0)
            file = discord.File(io.BytesIO(buffer.getvalue().encode()), filename="positions.txt")
            await ctx.send(f"{result['filtered']} matching positions:", file=file)
            return

        result = await asyncio.to_thread(
            wr.wolfrandom, str(DB_FILE), eval_range, 'select', None, show_eval)

        if result['filtered'] == 0:
            await ctx.send("No positions match the given eval bounds.")
            return

        content = f"```\n{result['selected']}\n```"
        if show_eval:
            content += f"Eval: `{result['eval']}`"
        await ctx.send(content)
