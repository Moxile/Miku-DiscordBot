import discord
from datetime import datetime, timezone, timedelta
from discord.ext import commands, tasks

from config import (
    WAIFU_BASE_VALUE, WAIFU_VALUE_MULTIPLIER, WAIFU_DECAY_RATE,
    WAIFU_RESALE_RATE, WAIFU_GIFT_RATE, WAIFU_GIFT_MIN,
    MARRIAGE_FEE, ENGAGEMENT_DAYS,
)
from cogs.economy.db import ensure_wallet, lock_wallet, update_wallet, add_transaction
from cogs.waifu.db import (
    ensure_waifu, get_waifu, get_harem,
    set_waifu_owner, engage_if_mutual, set_gifted,
    set_marriage, dissolve_marriage,
    decay_waifu_values, remove_member_waifus,
)
from core.checks import require_not_locked
from core.money import parse_amount, AmountError
from core.names import format_name


class Waifu(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._proposals: dict[tuple, tuple] = {}
        self.decay_task.start()

    def cog_unload(self):
        self.decay_task.cancel()

    @property
    def pool(self):
        return self.bot.pool

    # ── Background decay task ──

    @tasks.loop(hours=24)
    async def decay_task(self):
        async with self.pool.acquire() as conn:
            await decay_waifu_values(conn, WAIFU_BASE_VALUE, WAIFU_DECAY_RATE)

    @decay_task.before_loop
    async def before_decay(self):
        await self.bot.wait_until_ready()
        now = datetime.now(timezone.utc)
        target = now.replace(hour=0, minute=30, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        await discord.utils.sleep_until(target)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Release/clean up waifu data when a member leaves, is kicked, or is banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_waifus(conn, member.guild.id, member.id)

    @staticmethod
    def _gift_required(value: int) -> int:
        """Minimum money-gift that pauses a waifu's decay for the day."""
        return max(WAIFU_GIFT_MIN, int(value * WAIFU_GIFT_RATE))

    @commands.Cog.listener()
    async def on_money_gift(self, guild_id: int, giver_id: int, recipient_id: int, amount: int):
        """Dispatched by the economy `.gift` flow. If an owner gifts their own waifu
        at least the required amount, refresh their decay clock so their value holds."""
        async with self.pool.acquire() as conn:
            row = await get_waifu(conn, guild_id, recipient_id)
            if not row or row["owner_id"] != giver_id:
                return
            if amount < self._gift_required(row["value"]):
                return
            await set_gifted(conn, guild_id, recipient_id)

    # ── Helpers ──

    async def _get_display_name(self, guild: discord.Guild, user_id: int) -> str:
        member = guild.get_member(user_id)
        if member:
            return format_name(member, guild)
        try:
            user = await self.bot.fetch_user(user_id)
            return format_name(user, guild)
        except Exception:
            return f"User {user_id}"

    def _engagement_status(self, row_a, row_b) -> str:
        """Given two waifu rows, return relationship status string."""
        if row_a and row_a["spouse_id"] and row_b and row_b["spouse_id"]:
            return "Married"
        if (row_a and row_a["owner_id"] == row_b["user_id"] and
                row_b and row_b["owner_id"] == row_a["user_id"]):
            return "Engaged"
        return "None"

    # ── Commands ──

    @commands.command(aliases=["wbuy"])
    @require_not_locked()
    async def waifubuy(self, ctx: commands.Context, member: discord.Member, amount: str = None):
        """Buy a user as your waifu. Pay at least their current value.
        Usage: .waifubuy <@user> [amount]"""
        cur = self.bot.get_currency(ctx.guild.id)
        if member == ctx.author:
            await ctx.send("You can't buy yourself.")
            return
        if member.bot:
            await ctx.send("You can't buy a bot.")
            return
        if amount is not None:
            try:
                amount = parse_amount(amount)
            except AmountError as e:
                await ctx.send(str(e))
                return

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                buyer_bal = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                target_row = await ensure_waifu(conn, ctx.guild.id, member.id)

                if target_row["spouse_id"] is not None:
                    await ctx.send(f"**{format_name(member)}** is married and cannot be bought.")
                    return

                current_value = target_row["value"]
                pay = amount if amount is not None else current_value

                if pay < current_value:
                    await ctx.send(
                        f"**{format_name(member)}** is worth {cur.emoji} **{current_value:,}**. "
                        f"You must pay at least that much."
                    )
                    return
                if buyer_bal["wallet"] < pay:
                    await ctx.send(
                        f"You don't have enough in your wallet. "
                        f"Need {cur.emoji} **{pay:,}**, have {cur.emoji} **{buyer_bal['wallet']:,}**."
                    )
                    return

                prev_owner = target_row["owner_id"]
                new_value = int(max(pay, current_value) * WAIFU_VALUE_MULTIPLIER)
                await update_wallet(conn, ctx.guild.id, ctx.author.id, -pay)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, -pay, "waifu_buy",
                                      f"Bought {member.id} as waifu")

                # The previous owner is paid out a share of the sale; the rest is a
                # money sink. With no previous owner the whole payment is sunk.
                payout = 0
                if prev_owner and prev_owner != ctx.author.id:
                    payout = int(pay * WAIFU_RESALE_RATE)
                    await ensure_wallet(conn, ctx.guild.id, prev_owner)
                    await update_wallet(conn, ctx.guild.id, prev_owner, payout)
                    await add_transaction(conn, ctx.guild.id, prev_owner, payout, "waifu_sale",
                                          f"Sold {member.id} to {ctx.author.id}")

                await set_waifu_owner(conn, ctx.guild.id, member.id, ctx.author.id, new_value)
                engaged = await engage_if_mutual(conn, ctx.guild.id, ctx.author.id, member.id)

        embed = discord.Embed(
            title="Waifu Purchased!",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.add_field(name="New Waifu", value=member.mention, inline=True)
        embed.add_field(name="Paid", value=f"{cur.emoji} {pay:,}", inline=True)
        embed.add_field(name="New Value", value=f"{cur.emoji} {new_value:,}", inline=True)
        if payout:
            prev_name = await self._get_display_name(ctx.guild, prev_owner)
            embed.add_field(
                name="Previous owner paid",
                value=f"**{prev_name}** received {cur.emoji} {payout:,}",
                inline=False,
            )
        if engaged:
            embed.add_field(
                name="💍 Engaged!",
                value=f"{ctx.author.mention} and {member.mention} now own each other — you're **engaged**!",
                inline=False,
            )
        await ctx.send(embed=embed)

    @commands.command(aliases=["waifulist", "mywaifu"])
    async def harem(self, ctx: commands.Context, member: discord.Member = None):
        """Show your (or someone's) harem — all waifus owned.
        Usage: .harem [@member]"""
        cur = self.bot.get_currency(ctx.guild.id)
        target = member or ctx.author
        async with self.pool.acquire() as conn:
            rows = await get_harem(conn, ctx.guild.id, target.id)

        if not rows:
            who = "You don't" if target == ctx.author else f"{format_name(target)} doesn't"
            await ctx.send(f"{who} own any waifus.")
            return

        total_value = sum(r["value"] for r in rows)
        embed = discord.Embed(
            title=f"{format_name(target)}'s Harem",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        lines = []
        for i, row in enumerate(rows, 1):
            name = await self._get_display_name(ctx.guild, row["user_id"])
            status = "💍 Married" if row["spouse_id"] else ("💕 Engaged" if row["engaged_since"] and row["owner_id"] == target.id else "")
            lines.append(f"`{i}.` **{name}** — {cur.emoji} {row['value']:,} {status}")
        embed.description = "\n".join(lines)
        embed.add_field(name="Total harem value", value=f"{cur.emoji} {total_value:,}", inline=False)
        await ctx.send(embed=embed)

    @commands.command(aliases=["winfo"])
    async def waifuinfo(self, ctx: commands.Context, member: discord.Member = None):
        """Show a user's waifu stats: value, owner, and relationship status.
        Usage: .waifuinfo [@member]"""
        cur = self.bot.get_currency(ctx.guild.id)
        target = member or ctx.author
        async with self.pool.acquire() as conn:
            row = await ensure_waifu(conn, ctx.guild.id, target.id)

        embed = discord.Embed(
            title=f"{format_name(target)}'s Waifu Info",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.set_thumbnail(url=target.display_avatar.url)
        embed.add_field(name="Value", value=f"{cur.emoji} {row['value']:,}", inline=True)

        if row["owner_id"]:
            owner_name = await self._get_display_name(ctx.guild, row["owner_id"])
            embed.add_field(name="Owned by", value=owner_name, inline=True)
        else:
            embed.add_field(name="Owned by", value="Nobody", inline=True)

        if row["spouse_id"]:
            spouse_name = await self._get_display_name(ctx.guild, row["spouse_id"])
            embed.add_field(name="Status", value=f"💍 Married to **{spouse_name}**", inline=False)
        elif row["engaged_since"]:
            days = (datetime.now(timezone.utc) - row["engaged_since"]).days
            embed.add_field(name="Status", value=f"💕 Engaged ({days}d)", inline=False)
        else:
            embed.add_field(name="Status", value="Single", inline=False)

        if not row["owner_id"] and not row["spouse_id"]:
            embed.set_footer(text=f"Buy them with .waifubuy @{format_name(target)}")
        await ctx.send(embed=embed)

    @commands.command(aliases=["wgift"])
    async def waifugift(self, ctx: commands.Context, waifu: discord.Member, recipient: discord.Member):
        """Gift a waifu you own to another user.
        Usage: .waifugift <@waifu> <@recipient>"""
        cur = self.bot.get_currency(ctx.guild.id)
        if waifu == ctx.author:
            await ctx.send("You can't gift yourself.")
            return
        if recipient == ctx.author:
            await ctx.send("Just keep them yourself then!")
            return
        if waifu == recipient:
            await ctx.send("You can't gift someone to themselves.")
            return

        async with self.pool.acquire() as conn:
            row = await get_waifu(conn, ctx.guild.id, waifu.id)
            if not row or row["owner_id"] != ctx.author.id:
                await ctx.send(f"You don't own **{format_name(waifu)}**.")
                return
            if row["spouse_id"] is not None:
                await ctx.send(f"**{format_name(waifu)}** is married — they cannot be gifted.")
                return
            await set_waifu_owner(conn, ctx.guild.id, waifu.id, recipient.id, row["value"])
            # A gift can create mutual ownership (recipient already owns the giver) —
            # that engages them, same as a buy would.
            engaged = await engage_if_mutual(conn, ctx.guild.id, recipient.id, waifu.id)

        embed = discord.Embed(
            description=f"{ctx.author.mention} gifted **{format_name(waifu)}** to {recipient.mention}! 🎁",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.add_field(name="Value", value=f"{cur.emoji} {row['value']:,}", inline=True)
        if engaged:
            embed.add_field(
                name="💍 Engaged!",
                value=f"{recipient.mention} and **{format_name(waifu)}** now own each other — they're **engaged**!",
                inline=False,
            )
        await ctx.send(embed=embed)

    @commands.command()
    @require_not_locked()
    async def propose(self, ctx: commands.Context, member: discord.Member):
        """Propose marriage to your engaged partner. Requires 7 days of mutual ownership.
        Usage: .propose <@member>"""
        cur = self.bot.get_currency(ctx.guild.id)
        if member == ctx.author:
            await ctx.send("You can't propose to yourself.")
            return

        async with self.pool.acquire() as conn:
            proposer_row = await get_waifu(conn, ctx.guild.id, ctx.author.id)
            target_row = await get_waifu(conn, ctx.guild.id, member.id)

        if not proposer_row or not target_row:
            await ctx.send("You need to be engaged first (own each other).")
            return
        if proposer_row.get("spouse_id") or target_row.get("spouse_id"):
            await ctx.send("One of you is already married.")
            return

        mutually_owned = (
            proposer_row["owner_id"] == member.id and
            target_row["owner_id"] == ctx.author.id
        )
        if not mutually_owned:
            await ctx.send("You need to own each other (be engaged) before proposing.")
            return

        engaged_since = proposer_row["engaged_since"] or target_row["engaged_since"]
        if not engaged_since:
            await ctx.send("You need to be engaged first.")
            return
        days_engaged = (datetime.now(timezone.utc) - engaged_since).days
        if days_engaged < ENGAGEMENT_DAYS:
            remaining = ENGAGEMENT_DAYS - days_engaged
            await ctx.send(
                f"You need to be engaged for **{ENGAGEMENT_DAYS} days** before proposing. "
                f"{remaining} day(s) remaining."
            )
            return

        async with self.pool.acquire() as conn:
            await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
            bal = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
            if bal["wallet"] < MARRIAGE_FEE:
                await ctx.send(
                    f"Proposing costs {cur.emoji} **{MARRIAGE_FEE:,}**. "
                    f"You only have {cur.emoji} **{bal['wallet']:,}**."
                )
                return

        expires_at = datetime.now(timezone.utc) + timedelta(seconds=60)
        self._proposals[(ctx.guild.id, ctx.author.id)] = (member.id, expires_at)

        embed = discord.Embed(
            title="💍 Marriage Proposal!",
            description=(
                f"{ctx.author.mention} is proposing to {member.mention}!\n\n"
                f"**{format_name(member)}**, use `.accept` or `.deny` within 60 seconds."
            ),
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.add_field(name="Fee", value=f"{cur.emoji} {MARRIAGE_FEE:,} (charged on acceptance)", inline=False)
        await ctx.send(embed=embed)

    @commands.command()
    async def accept(self, ctx: commands.Context):
        """Accept a pending marriage proposal directed at you."""
        proposal_key = None
        for (gid, proposer_id), (target_id, expires_at) in list(self._proposals.items()):
            if gid == ctx.guild.id and target_id == ctx.author.id:
                proposal_key = (gid, proposer_id)
                proposer_id_found = proposer_id
                exp = expires_at
                break

        if not proposal_key:
            await ctx.send("You have no pending proposals.")
            return
        if datetime.now(timezone.utc) > exp:
            del self._proposals[proposal_key]
            await ctx.send("That proposal has expired.")
            return

        del self._proposals[proposal_key]

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await ensure_wallet(conn, ctx.guild.id, proposer_id_found)
                bal = await lock_wallet(conn, ctx.guild.id, proposer_id_found)
                if bal["wallet"] < MARRIAGE_FEE:
                    await ctx.send("The proposer no longer has enough funds for the fee. Proposal cancelled.")
                    return
                await update_wallet(conn, ctx.guild.id, proposer_id_found, -MARRIAGE_FEE)
                await add_transaction(conn, ctx.guild.id, proposer_id_found, -MARRIAGE_FEE,
                                      "marriage_fee", f"Marriage proposal to {ctx.author.id}")
                await set_marriage(conn, ctx.guild.id, proposer_id_found, ctx.author.id)

        proposer = ctx.guild.get_member(proposer_id_found) or await self.bot.fetch_user(proposer_id_found)
        embed = discord.Embed(
            title="💒 Married!",
            description=f"Congratulations! {proposer.mention} and {ctx.author.mention} are now **married**! 🎉",
            color=discord.Color.from_rgb(255, 105, 180),
        )
        embed.set_footer(text="They are now safe from being bought by others.")
        await ctx.send(embed=embed)

    @commands.command()
    async def deny(self, ctx: commands.Context):
        """Deny a pending marriage proposal directed at you."""
        for (gid, proposer_id), (target_id, _) in list(self._proposals.items()):
            if gid == ctx.guild.id and target_id == ctx.author.id:
                del self._proposals[(gid, proposer_id)]
                proposer = ctx.guild.get_member(proposer_id) or await self.bot.fetch_user(proposer_id)
                await ctx.send(f"{ctx.author.mention} rejected {proposer.mention}'s proposal. 💔")
                return
        await ctx.send("You have no pending proposals.")

    @commands.command()
    async def divorce(self, ctx: commands.Context):
        """Divorce your spouse. Both become buyable again."""
        async with self.pool.acquire() as conn:
            row = await get_waifu(conn, ctx.guild.id, ctx.author.id)
            if not row or not row["spouse_id"]:
                await ctx.send("You're not married.")
                return
            spouse_id = row["spouse_id"]
            await dissolve_marriage(conn, ctx.guild.id, ctx.author.id, spouse_id)

        spouse = ctx.guild.get_member(spouse_id) or await self.bot.fetch_user(spouse_id)
        embed = discord.Embed(
            description=f"{ctx.author.mention} and {spouse.mention} have **divorced**. 💔",
            color=discord.Color.greyple(),
        )
        await ctx.send(embed=embed)
