import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from cogs.predictions.db import (
    create_prediction, get_prediction, get_prediction_options,
    get_option_totals, place_prediction_bet,
    close_prediction, resolve_prediction, get_winning_bets,
    get_active_predictions,
)
from core.checks import require_not_locked, UserLocked
from core.money import parse_amount, AmountError
from config import MAIN_CURRENCY_EMOJI

# In-memory cache: guild_id -> role_id
_predictor_roles: dict[int, int] = {}


class Predictions(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        if isinstance(error, UserLocked):
            return
        raise error

    def can_create(self, ctx) -> bool:
        """Check if user is owner, admin, or has the predictor role."""
        if ctx.author.guild_permissions.administrator:
            return True
        role_id = _predictor_roles.get(ctx.guild.id)
        if role_id:
            return any(r.id == role_id for r in ctx.author.roles)
        return False

    def can_manage(self, ctx, prediction) -> bool:
        """Check if user can close/resolve a prediction (creator or owner)."""
        if ctx.author.id == prediction["creator_id"]:
            return True
        if ctx.author.guild_permissions.administrator:
            return True
        return False

    @commands.command()
    @commands.is_owner()
    async def setpredictorrole(self, ctx, role: discord.Role):
        """Admin: Set which role can create predictions."""
        await self.pool.execute(
            """INSERT INTO guild_settings (guild_id, key, value) VALUES ($1, 'predictor_role', $2)
               ON CONFLICT (guild_id, key) DO UPDATE SET value = $2""",
            ctx.guild.id, str(role.id),
        )
        _predictor_roles[ctx.guild.id] = role.id
        await ctx.send(f"Predictor role set to {role.mention}.")

    @commands.Cog.listener()
    async def on_ready(self):
        """Load predictor roles from DB on startup."""
        rows = await self.pool.fetch(
            "SELECT guild_id, value FROM guild_settings WHERE key = 'predictor_role'",
        )
        for row in rows:
            _predictor_roles[row["guild_id"]] = int(row["value"])

    @commands.command()
    async def predict(self, ctx, *, args: str):
        """Create a prediction. Usage: .predict Question? | Option1 | Option2 | Option3"""
        if not self.can_create(ctx):
            await ctx.send("You don't have permission to create predictions.")
            return

        parts = [p.strip() for p in args.split("|")]
        if len(parts) < 3:
            await ctx.send("You need a question and at least 2 options. Usage: `.predict Question? | Option1 | Option2`")
            return

        question = parts[0]
        options = parts[1:]

        pred, opt_rows = await create_prediction(self.pool, ctx.guild.id, ctx.author.id, question, options)

        embed = discord.Embed(title="Prediction Created!", description=question, color=discord.Color.teal())
        embed.add_field(name="ID", value=str(pred["id"]), inline=True)
        embed.add_field(name="Status", value="Open", inline=True)
        for opt in opt_rows:
            embed.add_field(
                name=f"Option {opt['option_index']}",
                value=opt["label"],
                inline=False,
            )
        embed.set_footer(text=f"Use .pbet {pred['id']} <option#> <amount> to place a bet.")
        await ctx.send(embed=embed)

    @commands.command()
    @require_not_locked()
    async def pbet(self, ctx, prediction_id: int, option: int, amount: str):
        """Bet on a prediction option. Usage: .pbet <id> <option#> <amount|all>"""
        pred = await get_prediction(self.pool, prediction_id)
        if not pred or pred["guild_id"] != ctx.guild.id:
            await ctx.send("Prediction not found.")
            return
        if pred["status"] != "open":
            await ctx.send("This prediction is no longer accepting bets.")
            return

        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        try:
            amount = parse_amount(amount, wallet_balance=wallet["wallet"])
        except AmountError as e:
            await ctx.send(str(e))
            return

        if wallet["wallet"] < amount:
            await ctx.send(f"You don't have enough {MAIN_CURRENCY_EMOJI}.")
            return

        options = await get_prediction_options(self.pool, prediction_id)
        target = None
        for opt in options:
            if opt["option_index"] == option:
                target = opt
                break
        if not target:
            await ctx.send(f"Invalid option. Choose between 1 and {len(options)}.")
            return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -amount)
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, -amount, "prediction_bet",
                              f"Bet on prediction #{prediction_id}")
        await place_prediction_bet(self.pool, prediction_id, target["id"], ctx.guild.id, ctx.author.id, amount)

        await ctx.send(f"{ctx.author.display_name} bet {amount}{MAIN_CURRENCY_EMOJI} on **{target['label']}**!")

    @commands.command()
    async def pclose(self, ctx, prediction_id: int):
        """Close a prediction (no more bets). Creator or admin only."""
        pred = await get_prediction(self.pool, prediction_id)
        if not pred or pred["guild_id"] != ctx.guild.id:
            await ctx.send("Prediction not found.")
            return
        if not self.can_manage(ctx, pred):
            await ctx.send("You don't have permission to close this prediction.")
            return
        if pred["status"] != "open":
            await ctx.send("This prediction is not open.")
            return

        await close_prediction(self.pool, prediction_id)

        options = await get_prediction_options(self.pool, prediction_id)
        totals = {r["option_id"]: r["total"] for r in await get_option_totals(self.pool, prediction_id)}
        pool_total = sum(totals.values())

        embed = discord.Embed(title="Prediction Closed!", description=pred["question"], color=discord.Color.orange())
        embed.add_field(name="Total Pool", value=f"{pool_total}{MAIN_CURRENCY_EMOJI}", inline=True)
        for opt in options:
            opt_total = totals.get(opt["id"], 0)
            pct = f"({opt_total * 100 // pool_total}%)" if pool_total > 0 else ""
            embed.add_field(
                name=f"Option {opt['option_index']}: {opt['label']}",
                value=f"{opt_total}{MAIN_CURRENCY_EMOJI} {pct}",
                inline=False,
            )
        embed.set_footer(text=f"Use .presolve {prediction_id} <option#> to pick the winner.")
        await ctx.send(embed=embed)

    @commands.command()
    async def presolve(self, ctx, prediction_id: int, winning_option: int):
        """Resolve a prediction and pay out winners. Creator or admin only."""
        pred = await get_prediction(self.pool, prediction_id)
        if not pred or pred["guild_id"] != ctx.guild.id:
            await ctx.send("Prediction not found.")
            return
        if not self.can_manage(ctx, pred):
            await ctx.send("You don't have permission to resolve this prediction.")
            return
        if pred["status"] == "resolved":
            await ctx.send("This prediction is already resolved.")
            return

        options = await get_prediction_options(self.pool, prediction_id)
        winner_opt = None
        for opt in options:
            if opt["option_index"] == winning_option:
                winner_opt = opt
                break
        if not winner_opt:
            await ctx.send(f"Invalid option. Choose between 1 and {len(options)}.")
            return

        totals = {r["option_id"]: r["total"] for r in await get_option_totals(self.pool, prediction_id)}
        pool_total = sum(totals.values())
        winner_pool = totals.get(winner_opt["id"], 0)

        await resolve_prediction(self.pool, prediction_id, winner_opt["id"])

        embed = discord.Embed(
            title="Prediction Resolved!",
            description=f"{pred['question']}\n\nWinner: **{winner_opt['label']}**",
            color=discord.Color.green(),
        )
        embed.add_field(name="Total Pool", value=f"{pool_total}{MAIN_CURRENCY_EMOJI}", inline=True)

        if winner_pool > 0 and pool_total > 0:
            winning_bets = await get_winning_bets(self.pool, prediction_id, winner_opt["id"])
            payout_lines = []
            for bet in winning_bets:
                payout = (bet["amount"] * pool_total) // winner_pool
                await update_wallet(self.pool, ctx.guild.id, bet["user_id"], payout)
                await add_transaction(self.pool, ctx.guild.id, bet["user_id"], payout, "prediction_win",
                                      f"Won prediction #{prediction_id}")
                member = ctx.guild.get_member(bet["user_id"])
                name = member.display_name if member else str(bet["user_id"])
                profit = payout - bet["amount"]
                payout_lines.append(f"{name}: +{profit}{MAIN_CURRENCY_EMOJI} (bet {bet['amount']}, got {payout})")
            embed.add_field(name="Payouts", value="\n".join(payout_lines) or "None", inline=False)
        else:
            embed.add_field(name="Payouts", value="No winning bets.", inline=False)

        await ctx.send(embed=embed)

    @commands.command(aliases=["preds"])
    async def predictions(self, ctx):
        """View all active predictions."""
        preds = await get_active_predictions(self.pool, ctx.guild.id)
        if not preds:
            await ctx.send("No active predictions.")
            return

        embed = discord.Embed(title="Active Predictions", color=discord.Color.teal())
        for pred in preds:
            options = await get_prediction_options(self.pool, pred["id"])
            totals = {r["option_id"]: r["total"] for r in await get_option_totals(self.pool, pred["id"])}
            pool_total = sum(totals.values())
            opts_text = "\n".join(
                f"  {o['option_index']}. {o['label']} — {totals.get(o['id'], 0)}{MAIN_CURRENCY_EMOJI}"
                for o in options
            )
            status = pred["status"].capitalize()
            embed.add_field(
                name=f"#{pred['id']} [{status}] {pred['question']}",
                value=f"Pool: {pool_total}{MAIN_CURRENCY_EMOJI}\n{opts_text}",
                inline=False,
            )
        await ctx.send(embed=embed)
