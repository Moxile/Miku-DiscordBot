import ast
import io
import operator
import re
import struct
import zlib

import discord
from discord.ext import commands


# ── Safe math evaluator ──
# Supports: +, -, *, /, //, %, ** (or ^), parentheses, unary minus
# No access to builtins, variables, or function calls.

_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}


def _safe_eval(node):
    if isinstance(node, ast.Expression):
        return _safe_eval(node.body)
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _OPS:
        left = _safe_eval(node.left)
        right = _safe_eval(node.right)
        if isinstance(node.op, ast.Pow) and right > 1000:
            raise ValueError("Exponent too large.")
        return _OPS[type(node.op)](left, right)
    if isinstance(node, ast.UnaryOp) and type(node.op) in _OPS:
        return _OPS[type(node.op)](_safe_eval(node.operand))
    raise ValueError("Unsupported expression.")


def safe_calc(expr: str) -> float | int:
    # Allow ^ as power operator
    expr = expr.replace("^", "**")
    tree = ast.parse(expr, mode="eval")
    result = _safe_eval(tree)
    if isinstance(result, float) and result.is_integer():
        return int(result)
    return result


_HEX_RE = re.compile(r"^#?([0-9a-fA-F]{6})$")


class Utility(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.command()
    async def calc(self, ctx: commands.Context, *, expression: str):
        """Calculate a math expression. Supports +, -, *, /, ^, %, parentheses. Example: .calc (10+67/7)^2"""
        try:
            result = safe_calc(expression)
        except ZeroDivisionError:
            await ctx.send("Division by zero.")
            return
        except Exception:
            await ctx.send("Invalid expression. Supports: `+`, `-`, `*`, `/`, `^`, `%`, parentheses.")
            return

        if isinstance(result, float):
            display = f"{result:,.6g}"
        else:
            display = f"{result:,}"

        embed = discord.Embed(
            description=f"```{expression} = {display}```",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

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

        # Create a solid-color PNG (256x128) — no external libraries needed
        width, height = 256, 128
        raw_row = b"\x00" + bytes([r, g, b]) * width  # filter byte + RGB pixels
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

    @calc.error
    @color.error
    async def utility_error(self, ctx, error):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Usage: `{ctx.prefix}{ctx.command.name} {ctx.command.signature}`")


async def setup(bot: commands.Bot):
    await bot.add_cog(Utility(bot))
