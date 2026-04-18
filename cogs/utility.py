import ast
import io
import operator
import re

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

        # Create a small solid-color image (64x64)
        # BMP format: simplest to generate without external libraries
        width, height = 64, 64
        row_size = (width * 3 + 3) & ~3  # rows padded to 4-byte boundary
        pixel_data_size = row_size * height
        file_size = 54 + pixel_data_size

        bmp = bytearray()
        # File header (14 bytes)
        bmp += b"BM"
        bmp += file_size.to_bytes(4, "little")
        bmp += (0).to_bytes(4, "little")
        bmp += (54).to_bytes(4, "little")
        # DIB header (40 bytes)
        bmp += (40).to_bytes(4, "little")
        bmp += width.to_bytes(4, "little")
        bmp += height.to_bytes(4, "little")
        bmp += (1).to_bytes(2, "little")   # planes
        bmp += (24).to_bytes(2, "little")  # bits per pixel
        bmp += (0).to_bytes(4, "little")   # no compression
        bmp += pixel_data_size.to_bytes(4, "little")
        bmp += (2835).to_bytes(4, "little")  # h resolution
        bmp += (2835).to_bytes(4, "little")  # v resolution
        bmp += (0).to_bytes(4, "little")
        bmp += (0).to_bytes(4, "little")
        # Pixel data (BMP stores BGR, bottom-to-top)
        row = bytes([b, g, r]) * width + b"\x00" * (row_size - width * 3)
        bmp += row * height

        file = discord.File(io.BytesIO(bmp), filename="color.bmp")
        embed = discord.Embed(
            title=f"#{hex_str.upper()}",
            description=f"RGB({r}, {g}, {b})",
            color=discord.Color.from_rgb(r, g, b),
        )
        embed.set_thumbnail(url="attachment://color.bmp")
        await ctx.send(file=file, embed=embed)

    @calc.error
    @color.error
    async def utility_error(self, ctx, error):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Usage: `{ctx.prefix}{ctx.command.name} {ctx.command.signature}`")


async def setup(bot: commands.Bot):
    await bot.add_cog(Utility(bot))
