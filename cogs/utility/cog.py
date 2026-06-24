from __future__ import annotations
import asyncio
import io
import random
import re
import struct
import zlib

import discord
from discord.ext import commands

from . import calc

_CALC_TIMEOUT = 10  # seconds; symbolic work can be slow, so cap it

_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{6})$")


class Utility(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.group(
        invoke_without_command=True,
        extras={"example": ".calc sin(pi/4) + sqrt(2)"},
    )
    async def calc(self, ctx: commands.Context, *, expression: str):
        """Evaluate a math expression and render the result as an image.

        **Operators**
        `+` `-` `*` `/` · `^` or `**` power · `%` modulo · `//` floor division · `!` factorial · `( )` to group

        **Implicit multiplication** — `2x`, `3(4+1)` and `2pi` all work.

        **Functions** — `sqrt` `cbrt` `exp` `log` (`log(x)` natural, `log(x, b)` base-b) `ln` `abs` `floor` `ceil` `sin` `cos` `tan` `asin` `acos` `atan` `sinh` `cosh` `tanh` `factorial` `gamma` `gcd` `lcm` `min` `max` `sign`

        **Constants** — `pi`, `e`, `oo` (infinity), `I` (imaginary unit)

        **Numbers** — integers, decimals, scientific (`1.5e3`); fractions stay exact (`1/3`) and a decimal is shown alongside.

        **Subcommands** (use a variable like `x`):
        `solve` solve = 0 · `diff` differentiate (add `, point` to evaluate there) · `integrate` integrate (add `, lower, upper` for a definite integral) · `simplify`

        **Examples**
        `.calc sin(pi/4) + sqrt(2)` · `.calc (10+67/7)^2` · `.calc 5!`
        `.calc solve x^2 - 4` · `.calc diff sin(x)*x^2` · `.calc diff x^2, 3`
        `.calc integrate x^2` · `.calc integrate x^2, 0, 1`"""
        await self._run_calc(ctx, "eval", expression)

    @calc.command(name="solve", extras={"example": ".calc solve x^2 - 4"})
    async def calc_solve(self, ctx: commands.Context, *, expression: str):
        """Solve an equation for its variable (expr = 0). Example: .calc solve x^2 - 4"""
        await self._run_calc(ctx, "solve", expression)

    @calc.command(name="diff", aliases=["derivative"], extras={"example": ".calc diff sin(x)*x^2, 3"})
    async def calc_diff(self, ctx: commands.Context, *, expression: str):
        """Differentiate an expression. Add ", point" to evaluate the derivative there.
        Examples: .calc diff sin(x)*x^2 · .calc diff x^2, 3"""
        await self._run_calc(ctx, "diff", expression)

    @calc.command(name="integrate", aliases=["integral"], extras={"example": ".calc integrate x^2, 0, 1"})
    async def calc_integrate(self, ctx: commands.Context, *, expression: str):
        """Integrate an expression. Add ", lower, upper" for a definite integral.
        Examples: .calc integrate x^2 · .calc integrate x^2, 0, 1"""
        await self._run_calc(ctx, "integrate", expression)

    @calc.command(name="simplify", extras={"example": ".calc simplify (x^2-1)/(x-1)"})
    async def calc_simplify(self, ctx: commands.Context, *, expression: str):
        """Simplify an expression. Example: .calc simplify (x^2-1)/(x-1)"""
        await self._run_calc(ctx, "simplify", expression)

    async def _run_calc(self, ctx: commands.Context, mode: str, expression: str):
        async with ctx.typing():
            try:
                # run_job offloads the CPU-bound work to a killable child
                # process, so a runaway expression can't freeze the bot.
                plain, png = await asyncio.to_thread(
                    calc.run_job, mode, expression, _CALC_TIMEOUT
                )
            except calc.CalcError as exc:
                await ctx.send(str(exc))
                return
            except Exception:
                await ctx.send("Invalid expression. See `.help calc` for the supported syntax.")
                return

        # Discord embed descriptions cap at 4096 chars; keep the plaintext sane.
        if len(plain) > 1000:
            plain = plain[:1000] + "…"

        file = discord.File(io.BytesIO(png), filename="calc.png")
        embed = discord.Embed(
            description=f"```{plain}```",
            color=discord.Color.blurple(),
        )
        embed.set_image(url="attachment://calc.png")
        await ctx.send(file=file, embed=embed)

    @commands.command()
    async def color(self, ctx: commands.Context, hex_code: str):
        """Show a color from a hex code. Example: .color #FF5733"""
        match = _HEX_RE.match(hex_code.strip())
        if not match:
            await ctx.send("Invalid hex code. Use format `#RRGGBB` (e.g. `#FF5733`).")
            return

        hex_str = match.group(1)
        r = int(hex_str[0:2], 16)
        g = int(hex_str[2:4], 16)
        b = int(hex_str[4:6], 16)

        width, height = 256, 128
        raw_row = b"\x00" + bytes([r, g, b]) * width
        raw_data = raw_row * height
        compressed = zlib.compress(raw_data)

        def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
            chunk = chunk_type + data
            return struct.pack(">I", len(data)) + chunk + struct.pack(">I", zlib.crc32(chunk) & 0xFFFFFFFF)

        png = b"\x89PNG\r\n\x1a\n"
        png += _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        png += _png_chunk(b"IDAT", compressed)
        png += _png_chunk(b"IEND", b"")

        file = discord.File(io.BytesIO(png), filename="color.png")
        embed = discord.Embed(
            title=f"#{hex_str.upper()}",
            description=f"RGB({r}, {g}, {b})",
            color=discord.Color.from_rgb(r, g, b),
        )
        embed.set_image(url="attachment://color.png")
        await ctx.send(file=file, embed=embed)

    @commands.command()
    async def dice(self, ctx: commands.Context, sides: int = 20):
        """Roll a dice with the given number of sides (default 20). Example: .dice 6"""
        if sides < 2:
            await ctx.send("A dice needs at least 2 sides.")
            return
        if sides > 1_000_000:
            await ctx.send("That dice is too large to roll.")
            return

        result = random.randint(1, sides)
        await ctx.send(f"🎲 You rolled a **{result}** (d{sides})")
