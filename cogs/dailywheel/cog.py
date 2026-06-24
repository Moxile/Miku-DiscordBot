import datetime
import secrets

import discord
from discord.ext import commands

from cogs.economy.db import update_wallet, add_transaction
from cogs.dailywheel.db import (
    get_prizes, add_currency_prize, add_message_prize, remove_prize,
    get_last_spin, record_spin, remove_member_data,
)
from core.checks import require_channel, invalidate, has_permissions_or_owner
from core.money import parse_amount, AmountError


def _pick_prize(prizes):
    """Weighted-random pick using secrets, not the random module."""
    total_weight = sum(p["weight"] for p in prizes)
    roll = secrets.randbelow(total_weight)
    cumulative = 0
    for prize in prizes:
        cumulative += prize["weight"]
        if roll < cumulative:
            return prize
    return prizes[-1]


class DailyWheel(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        await remove_member_data(self.pool, member.guild.id, member.id)

    @commands.command(aliases=["wheel", "spin"])
    @require_channel("dailywheel_channel")
    async def dailywheel(self, ctx):
        """Spin the daily wheel for a chance at currency or a funny prize. Resets at 0:00 UTC."""
        prizes = await get_prizes(self.pool, ctx.guild.id)
        if not prizes:
            return

        last_spin = await get_last_spin(self.pool, ctx.guild.id, ctx.author.id)
        today = datetime.datetime.now(datetime.timezone.utc).date()
        if last_spin is not None and last_spin >= today:
            tomorrow = datetime.datetime.combine(
                today + datetime.timedelta(days=1), datetime.time.min, tzinfo=datetime.timezone.utc
            )
            await ctx.send(f"You already spun the wheel today! Try again <t:{int(tomorrow.timestamp())}:R>.")
            return

        prize = _pick_prize(prizes)
        await record_spin(self.pool, ctx.guild.id, ctx.author.id)

        if prize["kind"] == "currency":
            cur = self.bot.get_currency(ctx.guild.id)
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, prize["amount"])
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, prize["amount"], "dailywheel_win")
            await ctx.send(f"🎡 You win **{prize['amount']:,}**{cur.emoji}!\n{prize['text']}")
        else:
            await ctx.send(f"🎡 {prize['text']}")

    # ── Admin ──

    @commands.command()
    @has_permissions_or_owner(administrator=True)
    async def wheeladd(self, ctx, weight: int, kind: str, *, rest: str):
        """Add a wheel prize. Usage: .wheeladd <weight> currency <amount> <message> or .wheeladd <weight> message <text>"""
        if weight <= 0:
            await ctx.send("Weight must be a positive integer.")
            return
        kind = kind.lower()
        if kind == "currency":
            parts = rest.split(maxsplit=1)
            if not parts:
                await ctx.send("Usage: `.wheeladd <weight> currency <amount> <message>`")
                return
            try:
                amount = parse_amount(parts[0])
            except AmountError as e:
                await ctx.send(str(e))
                return
            text = parts[1] if len(parts) > 1 else ""
            if not text:
                await ctx.send("Usage: `.wheeladd <weight> currency <amount> <message>`")
                return
            cur = self.bot.get_currency(ctx.guild.id)
            await add_currency_prize(self.pool, ctx.guild.id, weight, amount, text)
            await ctx.send(f"Added currency prize: **{amount:,}**{cur.emoji} (weight {weight}) — {text}")
        elif kind == "message":
            await add_message_prize(self.pool, ctx.guild.id, weight, rest)
            await ctx.send(f"Added message prize (weight {weight}): {rest}")
        else:
            await ctx.send("Kind must be `currency` or `message`.")

    @commands.command()
    @has_permissions_or_owner(administrator=True)
    async def wheelremove(self, ctx, prize_id: int):
        """Remove a wheel prize by id. Usage: .wheelremove <id>"""
        deleted = await remove_prize(self.pool, ctx.guild.id, prize_id)
        if deleted:
            await ctx.send(f"Removed prize #{prize_id}.")
        else:
            await ctx.send("No prize found with that id.")

    @commands.command()
    @has_permissions_or_owner(administrator=True)
    async def wheellist(self, ctx):
        """List this server's wheel prizes. Usage: .wheellist"""
        prizes = await get_prizes(self.pool, ctx.guild.id)
        if not prizes:
            await ctx.send("No wheel prizes configured yet — add some with `.wheeladd`.")
            return
        cur = self.bot.get_currency(ctx.guild.id)
        embed = discord.Embed(title="Daily Wheel Prizes", color=discord.Color.blurple())
        for prize in prizes:
            if prize["kind"] == "currency":
                value = f"**{prize['amount']:,}**{cur.emoji} — {prize['text']}"
            else:
                value = prize["text"]
            embed.add_field(name=f"#{prize['id']} · weight {prize['weight']}", value=value, inline=False)
        await ctx.send(embed=embed)

    @commands.command()
    @has_permissions_or_owner(administrator=True)
    async def setdailywheelchannel(self, ctx, channel: discord.TextChannel = None):
        """Set (or clear) the channel where the daily wheel can be spun."""
        if channel is None:
            await self.pool.execute(
                "DELETE FROM guild_settings WHERE guild_id = $1 AND key = 'dailywheel_channel'",
                ctx.guild.id,
            )
            invalidate(ctx.guild.id, "dailywheel_channel")
            await ctx.send("Daily wheel channel restriction removed — command allowed everywhere.")
        else:
            await self.pool.execute(
                """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'dailywheel_channel', $2)
                   ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
                ctx.guild.id, str(channel.id),
            )
            invalidate(ctx.guild.id, "dailywheel_channel")
            await ctx.send(f"Daily wheel restricted to {channel.mention}.")
