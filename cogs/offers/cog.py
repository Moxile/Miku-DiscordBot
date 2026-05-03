import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction, lock_wallet
from cogs.offers.db import (
    create_offer, get_offer, lock_offer, get_active_offers, get_offer_takes,
    add_offer_take, decrement_offer_pool, set_offer_status,
)
from core.checks import require_not_locked, UserLocked
from core.money import parse_amount, AmountError
from config import MAIN_CURRENCY_EMOJI


def _format_odds(odds: float) -> str:
    """Render a numeric odds value like 10 or 2.5 without trailing zeros."""
    return f"{float(odds):g}"


class Offers(commands.Cog):
    """Bookmaker-style fixed-odds bets: a host offers odds on an outcome and
    funds a pool that limits their exposure. Players accept with a stake and
    either win stake*odds (host pays from the pool) or lose their stake
    (host keeps it)."""

    def __init__(self, bot):
        self.bot = bot

    @property
    def pool_conn(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        if isinstance(error, UserLocked):
            return
        raise error

    async def _can_create(self, ctx) -> bool:
        """Admin or the configured predictor role (shared with Predictions cog)."""
        if ctx.author.guild_permissions.administrator:
            return True
        row = await self.pool_conn.fetchrow(
            "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'predictor_role'",
            ctx.guild.id,
        )
        if not row:
            return False
        role_id = int(row["value"])
        return any(r.id == role_id for r in ctx.author.roles)

    # ── Create ──

    @commands.command()
    @require_not_locked()
    async def offer(self, ctx, *, args: str = ""):
        """Create a bookmaker offer. Two forms:
        .offer x<odds> <min> <max> <pool> <message>   — variable stake
        .offer <return> <risk> <pool> <message>       — fixed stake (risk=stake, return=total payout)

        Examples:
        .offer x10 100 200 5000 Pens win tonight
        .offer 100 10 500 Miku hits 3 goals
        """
        if not await self._can_create(ctx):
            await ctx.send("You don't have permission to create offers.")
            return

        tokens = args.split()
        if not tokens:
            await ctx.send(
                "Usage:\n"
                "`.offer x<odds> <min> <max> <pool> <message>`\n"
                "`.offer <return> <risk> <pool> <message>`"
            )
            return

        try:
            if tokens[0].lower().startswith("x"):
                odds = float(tokens[0][1:])
                min_stake = parse_amount(tokens[1])
                max_stake = parse_amount(tokens[2])
                pool = parse_amount(tokens[3])
                description = " ".join(tokens[4:])
            else:
                ret = parse_amount(tokens[0])
                risk = parse_amount(tokens[1])
                pool = parse_amount(tokens[2])
                if ret <= risk:
                    await ctx.send("Return must be greater than risk.")
                    return
                odds = ret / risk
                min_stake = risk
                max_stake = risk
                description = " ".join(tokens[3:])
        except (AmountError, ValueError, IndexError) as e:
            msg = str(e) if isinstance(e, AmountError) else (
                "Invalid arguments.\n"
                "`.offer x<odds> <min> <max> <pool> <message>` — e.g. `.offer x10 100 200 5k Pens win`\n"
                "`.offer <return> <risk> <pool> <message>` — e.g. `.offer 100 10 500 Miku hits 3 goals`"
            )
            await ctx.send(msg)
            return

        if odds <= 1:
            await ctx.send("Odds must be greater than 1.")
            return
        if min_stake <= 0 or max_stake < min_stake:
            await ctx.send("Min/max stake invalid: min must be positive and max >= min.")
            return
        if pool <= 0:
            await ctx.send("Pool must be positive.")
            return

        max_liability_per_take = int(max_stake * odds) - max_stake
        if max_liability_per_take > pool:
            await ctx.send(
                f"Pool too small: a single max-stake take would cost {max_liability_per_take}{MAIN_CURRENCY_EMOJI} "
                f"but pool is only {pool}{MAIN_CURRENCY_EMOJI}. Raise the pool or lower max stake."
            )
            return

        async with self.pool_conn.acquire() as conn:
            async with conn.transaction():
                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                if wallet["wallet"] < pool:
                    await ctx.send(
                        f"You need {pool}{MAIN_CURRENCY_EMOJI} to fund this offer's pool "
                        f"but only have {wallet['wallet']}{MAIN_CURRENCY_EMOJI}."
                    )
                    return
                await update_wallet(conn, ctx.guild.id, ctx.author.id, -pool)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, -pool, "offer_pool",
                                      f"Funded offer pool")
                row = await create_offer(
                    conn, ctx.guild.id, ctx.channel.id, ctx.author.id,
                    description, odds, min_stake, max_stake, pool,
                )

        stake_range = f"{min_stake}{MAIN_CURRENCY_EMOJI}" if min_stake == max_stake \
            else f"{min_stake}-{max_stake}{MAIN_CURRENCY_EMOJI}"
        embed = discord.Embed(
            title=f"New Offer #{row['id']}",
            description=description or "*no description*",
            color=discord.Color.dark_gold(),
        )
        embed.add_field(name="Odds", value=f"x{_format_odds(odds)}", inline=True)
        embed.add_field(name="Stake", value=stake_range, inline=True)
        embed.add_field(name="Pool", value=f"{pool}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.set_footer(text=f"Use .take {row['id']} <stake> to accept.")
        await ctx.send(embed=embed)

    # ── Accept ──

    @commands.command()
    @require_not_locked()
    async def take(self, ctx, offer_id: int, stake: str):
        """Accept a host's offer with a given stake. Usage: .take <offer_id> <stake>"""
        try:
            stake = parse_amount(stake)
        except AmountError as e:
            await ctx.send(str(e))
            return

        async with self.pool_conn.acquire() as conn:
            async with conn.transaction():
                offer = await lock_offer(conn, offer_id)
                if not offer or offer["guild_id"] != ctx.guild.id:
                    await ctx.send("Offer not found.")
                    return
                if offer["status"] != "open":
                    await ctx.send("This offer is no longer accepting takes.")
                    return
                if offer["host_id"] == ctx.author.id:
                    await ctx.send("You can't take your own offer.")
                    return
                if stake < offer["min_stake"] or stake > offer["max_stake"]:
                    await ctx.send(
                        f"Stake must be between {offer['min_stake']} and {offer['max_stake']}{MAIN_CURRENCY_EMOJI}."
                    )
                    return

                odds = float(offer["odds"])
                liability = int(stake * odds) - stake
                if liability > offer["pool_remaining"]:
                    await ctx.send(
                        f"Pool can only cover up to {offer['pool_remaining']}{MAIN_CURRENCY_EMOJI} more in winnings. "
                        f"Your take would need {liability}{MAIN_CURRENCY_EMOJI}."
                    )
                    return

                await ensure_wallet(conn, ctx.guild.id, ctx.author.id)
                wallet = await lock_wallet(conn, ctx.guild.id, ctx.author.id)
                if wallet["wallet"] < stake:
                    await ctx.send(
                        f"You need {stake}{MAIN_CURRENCY_EMOJI} but only have {wallet['wallet']}{MAIN_CURRENCY_EMOJI}."
                    )
                    return

                await update_wallet(conn, ctx.guild.id, ctx.author.id, -stake)
                await add_transaction(conn, ctx.guild.id, ctx.author.id, -stake, "offer_take",
                                      f"Took offer #{offer_id}")
                await decrement_offer_pool(conn, offer_id, liability)
                await add_offer_take(conn, offer_id, ctx.author.id, stake, liability)

        payout = int(stake * odds)
        await ctx.send(
            f"Take placed on offer #{offer_id}: stake {stake}{MAIN_CURRENCY_EMOJI}, "
            f"payout if correct = {payout}{MAIN_CURRENCY_EMOJI}."
        )

    # ── Resolve ──

    @commands.command()
    async def closeoffer(self, ctx, offer_id: int, outcome: str):
        """Resolve an offer. Usage: .closeoffer <offer_id> win|lose
        'win' = players were correct (host pays out).
        'lose' = host was correct (host keeps stakes)."""
        outcome = outcome.lower()
        if outcome not in ("win", "lose"):
            await ctx.send("Outcome must be `win` (players win) or `lose` (players lose).")
            return

        async with self.pool_conn.acquire() as conn:
            async with conn.transaction():
                offer = await lock_offer(conn, offer_id)
                if not offer or offer["guild_id"] != ctx.guild.id:
                    await ctx.send("Offer not found.")
                    return
                if offer["host_id"] != ctx.author.id and not ctx.author.guild_permissions.administrator:
                    await ctx.send("Only the host (or an admin) can close this offer.")
                    return
                if offer["status"] != "open":
                    await ctx.send("This offer is already resolved.")
                    return

                takes = await get_offer_takes(conn, offer_id)
                odds = float(offer["odds"])
                total_stake = sum(t["stake"] for t in takes)
                total_liability_committed = offer["pool"] - offer["pool_remaining"]

                payout_lines = []
                if outcome == "win":
                    for t in takes:
                        payout = int(t["stake"] * odds)
                        await update_wallet(conn, ctx.guild.id, t["user_id"], payout)
                        await add_transaction(conn, ctx.guild.id, t["user_id"], payout, "offer_win",
                                              f"Won offer #{offer_id}")
                        payout_lines.append((t["user_id"], t["stake"], payout))
                    host_refund = offer["pool"] - total_liability_committed
                    if host_refund > 0:
                        await update_wallet(conn, ctx.guild.id, offer["host_id"], host_refund)
                        await add_transaction(conn, ctx.guild.id, offer["host_id"], host_refund,
                                              "offer_pool_refund", f"Unused pool from offer #{offer_id}")
                    await set_offer_status(conn, offer_id, "won")
                else:
                    host_gain = total_stake + offer["pool"]
                    if host_gain > 0:
                        await update_wallet(conn, ctx.guild.id, offer["host_id"], host_gain)
                        await add_transaction(conn, ctx.guild.id, offer["host_id"], host_gain,
                                              "offer_lose", f"Offer #{offer_id} resolved against players")
                    await set_offer_status(conn, offer_id, "lost")

        embed = discord.Embed(
            title=f"Offer #{offer_id} resolved",
            description=offer["description"] or "*no description*",
            color=discord.Color.green() if outcome == "win" else discord.Color.red(),
        )
        embed.add_field(name="Outcome", value="Players won" if outcome == "win" else "Host won", inline=True)
        embed.add_field(name="Takes", value=str(len(takes)), inline=True)
        embed.add_field(name="Total Stake", value=f"{total_stake}{MAIN_CURRENCY_EMOJI}", inline=True)

        if outcome == "win":
            lines = []
            for user_id, stake_amt, payout in payout_lines:
                member = ctx.guild.get_member(user_id)
                name = member.display_name if member else str(user_id)
                profit = payout - stake_amt
                lines.append(f"{name}: +{profit}{MAIN_CURRENCY_EMOJI} (staked {stake_amt}, got {payout})")
            embed.add_field(name="Payouts", value="\n".join(lines) or "No takes.", inline=False)
        else:
            embed.add_field(
                name="Host Gain",
                value=f"{total_stake + offer['pool']}{MAIN_CURRENCY_EMOJI} "
                      f"(pool {offer['pool']} back + {total_stake} in stakes)",
                inline=False,
            )
        await ctx.send(embed=embed)

    # ── Cancel ──

    @commands.command()
    async def canceloffer(self, ctx, offer_id: int):
        """Cancel an open offer. Only allowed before any takes are placed."""
        async with self.pool_conn.acquire() as conn:
            async with conn.transaction():
                offer = await lock_offer(conn, offer_id)
                if not offer or offer["guild_id"] != ctx.guild.id:
                    await ctx.send("Offer not found.")
                    return
                if offer["host_id"] != ctx.author.id and not ctx.author.guild_permissions.administrator:
                    await ctx.send("Only the host (or an admin) can cancel this offer.")
                    return
                if offer["status"] != "open":
                    await ctx.send("This offer is already resolved.")
                    return

                takes = await get_offer_takes(conn, offer_id)
                if takes:
                    await ctx.send(
                        f"Can't cancel: {len(takes)} take(s) already placed. "
                        f"Resolve with `.closeoffer {offer_id} win|lose` instead."
                    )
                    return

                await update_wallet(conn, ctx.guild.id, offer["host_id"], offer["pool"])
                await add_transaction(conn, ctx.guild.id, offer["host_id"], offer["pool"],
                                      "offer_cancel", f"Cancelled offer #{offer_id}")
                await set_offer_status(conn, offer_id, "cancelled")

        await ctx.send(f"Offer #{offer_id} cancelled. Pool of {offer['pool']}{MAIN_CURRENCY_EMOJI} refunded.")

    # ── Browse ──

    @commands.command()
    async def offers(self, ctx):
        """List all open offers in this guild."""
        rows = await get_active_offers(self.pool_conn, ctx.guild.id)
        if not rows:
            await ctx.send("No open offers.")
            return

        embed = discord.Embed(title="Open Offers", color=discord.Color.dark_gold())
        for o in rows:
            host = ctx.guild.get_member(o["host_id"])
            host_name = host.display_name if host else str(o["host_id"])
            stake_range = f"{o['min_stake']}" if o["min_stake"] == o["max_stake"] \
                else f"{o['min_stake']}-{o['max_stake']}"
            embed.add_field(
                name=f"#{o['id']} — {o['description'] or '(no description)'}",
                value=f"Host: {host_name} | Odds: x{_format_odds(o['odds'])} | "
                      f"Stake: {stake_range}{MAIN_CURRENCY_EMOJI} | "
                      f"Pool left: {o['pool_remaining']}/{o['pool']}{MAIN_CURRENCY_EMOJI}",
                inline=False,
            )
        await ctx.send(embed=embed)

    @commands.command()
    async def offerinfo(self, ctx, offer_id: int):
        """Show detailed info about an offer, including all takes."""
        offer = await get_offer(self.pool_conn, offer_id)
        if not offer or offer["guild_id"] != ctx.guild.id:
            await ctx.send("Offer not found.")
            return
        takes = await get_offer_takes(self.pool_conn, offer_id)

        host = ctx.guild.get_member(offer["host_id"])
        host_name = host.display_name if host else str(offer["host_id"])
        stake_range = f"{offer['min_stake']}" if offer["min_stake"] == offer["max_stake"] \
            else f"{offer['min_stake']}-{offer['max_stake']}"

        embed = discord.Embed(
            title=f"Offer #{offer['id']} [{offer['status']}]",
            description=offer["description"] or "*no description*",
            color=discord.Color.dark_gold(),
        )
        embed.add_field(name="Host", value=host_name, inline=True)
        embed.add_field(name="Odds", value=f"x{_format_odds(offer['odds'])}", inline=True)
        embed.add_field(name="Stake", value=f"{stake_range}{MAIN_CURRENCY_EMOJI}", inline=True)
        embed.add_field(name="Pool", value=f"{offer['pool_remaining']}/{offer['pool']}{MAIN_CURRENCY_EMOJI} remaining", inline=True)
        embed.add_field(name="Takes", value=str(len(takes)), inline=True)

        if takes:
            lines = []
            for t in takes[:15]:
                member = ctx.guild.get_member(t["user_id"])
                name = member.display_name if member else str(t["user_id"])
                potential = int(t["stake"] * float(offer["odds"]))
                lines.append(f"{name}: {t['stake']}{MAIN_CURRENCY_EMOJI} (wins {potential})")
            if len(takes) > 15:
                lines.append(f"... and {len(takes) - 15} more")
            embed.add_field(name="Current Takes", value="\n".join(lines), inline=False)
        await ctx.send(embed=embed)
