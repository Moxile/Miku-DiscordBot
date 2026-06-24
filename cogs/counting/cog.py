from __future__ import annotations
import ast
import math
import operator
import re

import discord
from discord.ext import commands

from config import PREFIX
from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from core.checks import has_permissions_or_owner, user_is_locked
from core.names import format_name

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
    ast.BitOr: operator.or_,
    ast.BitAnd: operator.and_,
    ast.BitXor: operator.xor,
    ast.LShift: operator.lshift,
    ast.RShift: operator.rshift,
    ast.Invert: operator.invert,
}

def _safe_factorial(n):
    if n != int(n) or n < 0 or n > 1000:
        raise ValueError
    return math.factorial(int(n))

_FUNCS = {
    "sqrt": math.sqrt,
    "abs": abs,
    "floor": math.floor,
    "ceil": math.ceil,
    "log": math.log,
    "log2": math.log2,
    "log10": math.log10,
    "sin": math.sin,
    "cos": math.cos,
    "tan": math.tan,
    "factorial": _safe_factorial,
    "gcd": math.gcd,
    "lcm": math.lcm,
    "max": max,
    "min": min,
    "isqrt": math.isqrt,
}

_CONSTS = {
    "pi": math.pi,
    "e": math.e,
    "tau": math.tau,
}


def _preprocess(expr: str) -> str:
    """Convert postfix ! notation to factorial() calls."""
    prev = None
    while prev != expr:
        prev = expr
        expr = re.sub(r'(\d+)!', r'factorial(\1)', expr)
    # Handle (expr)! by finding the matching open paren
    while ')!' in expr:
        idx = expr.index(')!')
        depth, j = 0, idx
        while j >= 0:
            if expr[j] == ')':
                depth += 1
            elif expr[j] == '(':
                depth -= 1
                if depth == 0:
                    break
            j -= 1
        if j < 0:
            break
        expr = expr[:j] + 'factorial(' + expr[j:idx+1] + ')' + expr[idx+2:]
    return expr


def safe_eval(expr: str) -> int | None:
    """Evaluate a math expression, returning an int or None if invalid/non-integer."""
    try:
        tree = ast.parse(_preprocess(expr.strip()), mode="eval")
    except SyntaxError:
        return None

    def _eval(node):
        if isinstance(node, ast.Expression):
            return _eval(node.body)
        if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
            return node.value
        if isinstance(node, ast.Name) and node.id in _CONSTS:
            return _CONSTS[node.id]
        if isinstance(node, ast.BinOp) and type(node.op) in _OPS:
            l, r = _eval(node.left), _eval(node.right)
            if l is None or r is None:
                return None
            return _OPS[type(node.op)](l, r)
        if isinstance(node, ast.UnaryOp) and type(node.op) in _OPS:
            v = _eval(node.operand)
            return None if v is None else _OPS[type(node.op)](v)
        if isinstance(node, ast.Call):
            if not isinstance(node.func, ast.Name) or node.func.id not in _FUNCS or node.keywords:
                return None
            args = [_eval(a) for a in node.args]
            if any(a is None for a in args):
                return None
            return _FUNCS[node.func.id](*args)
        return None

    try:
        result = _eval(tree)
    except (ZeroDivisionError, OverflowError, ValueError, TypeError):
        return None

    if result is None:
        return None
    try:
        rounded = round(result)
        return rounded if abs(result - rounded) < 1e-9 else None
    except (OverflowError, ValueError):
        return None


def _reward(count: int) -> int:
    if count >= 500:
        return 10
    if count >= 200:
        return 7
    if count >= 100:
        return 5
    if count >= 50:
        return 4
    if count >= 10:
        return 3
    return 2


_MILESTONES = {
    10:  "🎉 **Milestone 10 reached!** Double digits! +3 from now on!",
    50:  "🎉 **Milestone 50 reached!** Halfway to the century! +4 from now on!",
    100: "🎉 **Milestone 100 reached!** Triple digits achieved! +5 from now on!",
    200: "🎉 **Milestone 200 reached!** Two hundred strong! +7 from now on!",
    500: "🎉 **Milestone 500 reached!** Sky isn't the limit! +10 from now on!",
}


class Counting(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
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

    async def _record_fail(self, guild_id: int, user_id: int) -> int:
        row = await self.pool.fetchrow(
            """
            INSERT INTO counting_fails (guild_id, user_id, fails)
            VALUES ($1, $2, 1)
            ON CONFLICT (guild_id, user_id) DO UPDATE SET fails = counting_fails.fails + 1
            RETURNING fails
            """,
            guild_id, user_id,
        )
        return row["fails"]

    async def _maybe_assign_fail_role(self, guild: discord.Guild, member: discord.Member | None, fails: int):
        if member is None:
            return
        row = await self.pool.fetchrow(
            "SELECT role_id, threshold FROM counting_fail_roles WHERE guild_id = $1",
            guild.id,
        )
        if row is None or fails < row["threshold"]:
            return
        role = guild.get_role(row["role_id"])
        if role is None or role in member.roles:
            return
        try:
            await member.add_roles(role, reason=f"Reached {row['threshold']} counting fails")
        except discord.Forbidden:
            pass

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
    @has_permissions_or_owner(manage_channels=True)
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
    @has_permissions_or_owner(manage_channels=True)
    async def counting_unbind(self, ctx: commands.Context):
        """Remove the counting channel binding."""
        await self.pool.execute("DELETE FROM counting WHERE guild_id = $1", ctx.guild.id)
        self._cache.pop(ctx.guild.id, None)
        await ctx.send("Counting channel unbound.")

    @commands.command(name="countfails")
    async def countfails(self, ctx: commands.Context):
        """Show the server's counting fail leaderboard."""
        rows = await self.pool.fetch(
            "SELECT user_id, fails FROM counting_fails WHERE guild_id = $1 ORDER BY fails DESC LIMIT 10",
            ctx.guild.id,
        )
        if not rows:
            await ctx.send("No one has broken the count yet.")
            return

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}
        lines = []
        for rank, row in enumerate(rows, start=1):
            member = ctx.guild.get_member(row["user_id"])
            name = format_name(member, ctx.guild, fallback=f"User {row['user_id']}")
            prefix = medals.get(rank, f"`{rank}.`")
            fails = row["fails"]
            lines.append(f"{prefix} **{name}** — {fails} fail{'s' if fails != 1 else ''}")

        embed = discord.Embed(
            title="📉 Counting Fail Leaderboard",
            description="\n".join(lines),
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    @commands.command(name="countfailrole")
    @has_permissions_or_owner(manage_roles=True)
    async def countfailrole(self, ctx: commands.Context, role: discord.Role = None, threshold: int = None):
        """Admin: bind a role to be auto-granted once a member reaches X counting fails.

        Usage: `.countfailrole @Role 10` to bind, or `.countfailrole` with no
        arguments to remove the current binding.
        """
        if role is None:
            await self.pool.execute("DELETE FROM counting_fail_roles WHERE guild_id = $1", ctx.guild.id)
            await ctx.send("Counting fail role binding removed.")
            return

        if threshold is None or threshold <= 0:
            await ctx.send("Please provide a positive fail threshold, e.g. `.countfailrole @Role 10`.")
            return

        await self.pool.execute(
            """
            INSERT INTO counting_fail_roles (guild_id, role_id, threshold)
            VALUES ($1, $2, $3)
            ON CONFLICT (guild_id) DO UPDATE SET role_id = $2, threshold = $3
            """,
            ctx.guild.id, role.id, threshold,
        )
        await ctx.send(f"✅ Members will now receive {role.mention} after **{threshold}** counting fails.")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.guild is None:
            return

        state = await self._get_state(message.guild.id)
        if state is None or message.channel.id != state["channel_id"]:
            return

        if message.content.startswith(PREFIX):
            return

        value = safe_eval(message.content)

        if value is None:
            # Not a number/expression - treat as casual chat, ignore it.
            return

        guild_id = message.guild.id
        user_id = message.author.id
        current = state["count"]

        if user_id == state["last_user"]:
            await message.add_reaction("❌")
            await self._reset(guild_id)
            fails = await self._record_fail(guild_id, user_id)
            await self._maybe_assign_fail_role(message.guild, message.author, fails)
            await message.channel.send(
                f"{message.author.mention} can't count twice in a row! Back to **0**."
            )
            return

        if value != current + 1:
            await message.add_reaction("❌")
            await self._reset(guild_id)
            fails = await self._record_fail(guild_id, user_id)
            await self._maybe_assign_fail_role(message.guild, message.author, fails)
            await message.channel.send(
                f"{message.author.mention} broke the count at **{current}**! "
                f"Expected **{current + 1}**. Back to **0**."
            )
            return

        await self._advance(guild_id, user_id, value)
        await message.add_reaction("✅")
        if not await user_is_locked(self.pool, guild_id, user_id):
            reward = _reward(value)
            async with self.pool.acquire() as conn:
                await ensure_wallet(conn, guild_id, user_id)
                await update_wallet(conn, guild_id, user_id, reward)
                await add_transaction(
                    conn, guild_id, user_id, reward, "counting", f"Counted {value} correctly"
                )
        if value in _MILESTONES:
            await message.channel.send(_MILESTONES[value])
