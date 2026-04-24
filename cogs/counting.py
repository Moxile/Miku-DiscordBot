import ast
import operator

import discord
from discord.ext import commands

from config import MAIN_CURRENCY_EMOJI
from cogs.utils.db import ensure_wallet, update_wallet, add_transaction

REWARD = 5

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


def safe_eval(expr: str) -> int | None:
    """Evaluate a math expression, returning an int or None if invalid/non-integer."""
    try:
        tree = ast.parse(expr.strip(), mode="eval")
    except SyntaxError:
        return None

    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.BinOp) and type(node.op) in _OPS:
            l, r = _eval(node.left), _eval(node.right)
            if l is None or r is None:
                return None
            return _OPS[type(node.op)](l, r)
        if isinstance(node, ast.UnaryOp) and type(node.op) in _OPS:
            v = _eval(node.operand)
            return None if v is None else _OPS[type(node.op)](v)
        return None

    try:
        result = _eval(tree)
    except (ZeroDivisionError, OverflowError, ValueError):
        return None

    if result is None:
        return None
    rounded = round(result)
    return rounded if abs(result - rounded) < 1e-9 else None


class Counting(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # guild_id -> {"channel_id": int, "count": int, "last_user": int | None}
        self._cache: dict[int, dict] = {}

    @property
    def pool(self):
        return self.bot.pool

    async def _get_state(self, guild_id: int) -> dict | None:
        if guild_id in self._cache:
            return self._cache[guild_id]
        row = await self.pool.fetchrow(
            "SELECT channel_id, count, last_user FROM counting WHERE guild_id = $1",
            guild_id,
        )
        if row is None:
            return None
        state = dict(row)
        self._cache[guild_id] = state
        return state

    async def _reset(self, guild_id: int):
        await self.pool.execute(
            "UPDATE counting SET count = 0, last_user = NULL WHERE guild_id = $1",
            guild_id,
        )
        if guild_id in self._cache:
            self._cache[guild_id]["count"] = 0
            self._cache[guild_id]["last_user"] = None

    async def _advance(self, guild_id: int, user_id: int, new_count: int):
        await self.pool.execute(
            "UPDATE counting SET count = $2, last_user = $3 WHERE guild_id = $1",
            guild_id, new_count, user_id,
        )
        if guild_id in self._cache:
            self._cache[guild_id]["count"] = new_count
            self._cache[guild_id]["last_user"] = user_id

    @commands.group(invoke_without_command=True)
    async def counting(self, ctx: commands.Context):
        """Show the current counting state."""
        state = await self._get_state(ctx.guild.id)
        if state is None:
            await ctx.send("No counting channel set. Use `.counting bind` to set one.")
            return
        channel = ctx.guild.get_channel(state["channel_id"])
        await ctx.send(
            f"Counting channel: {channel.mention if channel else 'unknown'} — "
            f"current count: **{state['count']}**"
        )

    @counting.command(name="bind")
    @commands.has_permissions(manage_channels=True)
    async def counting_bind(self, ctx: commands.Context):
        """Bind counting to this channel (resets current count)."""
        await self.pool.execute(
            """
            INSERT INTO counting (guild_id, channel_id, count, last_user)
            VALUES ($1, $2, 0, NULL)
            ON CONFLICT (guild_id) DO UPDATE SET channel_id = $2, count = 0, last_user = NULL
            """,
            ctx.guild.id, ctx.channel.id,
        )
        self._cache[ctx.guild.id] = {
            "channel_id": ctx.channel.id,
            "count": 0,
            "last_user": None,
        }
        await ctx.send(f"✅ Counting bound to {ctx.channel.mention}. Start at **1**!")

    @counting.command(name="unbind")
    @commands.has_permissions(manage_channels=True)
    async def counting_unbind(self, ctx: commands.Context):
        """Remove the counting channel binding."""
        await self.pool.execute("DELETE FROM counting WHERE guild_id = $1", ctx.guild.id)
        self._cache.pop(ctx.guild.id, None)
        await ctx.send("Counting channel unbound.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.guild is None:
            return

        state = await self._get_state(message.guild.id)
        if state is None or message.channel.id != state["channel_id"]:
            return

        value = safe_eval(message.content)
        if value is None:
            return  # not a math expression — ignore silently

        guild_id = message.guild.id
        user_id = message.author.id
        current = state["count"]

        if user_id == state["last_user"]:
            await message.add_reaction("❌")
            await self._reset(guild_id)
            await message.channel.send(
                f"{message.author.mention} can't count twice in a row! Back to **0**."
            )
            return

        if value != current + 1:
            await message.add_reaction("❌")
            await self._reset(guild_id)
            await message.channel.send(
                f"{message.author.mention} broke the count at **{current}**! "
                f"Expected **{current + 1}**. Back to **0**."
            )
            return

        await self._advance(guild_id, user_id, value)
        await message.add_reaction("✅")
        async with self.pool.acquire() as conn:
            await ensure_wallet(conn, guild_id, user_id)
            await update_wallet(conn, guild_id, user_id, REWARD)
            await add_transaction(
                conn, guild_id, user_id, REWARD, "counting", f"Counted {value} correctly"
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(Counting(bot))
