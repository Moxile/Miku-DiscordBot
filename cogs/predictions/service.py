from __future__ import annotations

"""Prediction (parimutuel pool-betting) logic, shared by the .predictions
launcher command (cog.py) and the Miku Menu pages (ui.py).

The house opens a question with options; players bet into a shared pool, and on
resolution the whole pool is split among the winners in proportion to their
stake. Every user-facing failure raises core.errors.UserError.
"""

from dataclasses import dataclass

from cogs.economy.db import add_transaction, ensure_wallet, lock_wallet, update_wallet
from cogs.predictions.db import (
    close_prediction as _db_close, create_prediction as _db_create,
    get_active_predictions, get_option_totals, get_prediction, get_prediction_options,
    get_winning_bets, lock_prediction, place_prediction_bet,
    resolve_prediction as _db_resolve,
)
from core.checks import user_is_locked
from core.errors import UserError
from core.money import parse_amount


@dataclass
class BetResult:
    option_label: str
    amount: int


@dataclass
class CloseResult:
    prediction: object
    options: list
    totals: dict          # option_id -> total
    pool_total: int


@dataclass
class ResolveResult:
    prediction: object
    winner: object
    pool_total: int
    winner_pool: int
    payouts: list         # list of (user_id, amount, payout)


# ── permissions ──

async def can_create(pool, guild, member) -> bool:
    """Admin, or a holder of the configured predictor role (shared with Bets)."""
    if member.guild_permissions.administrator:
        return True
    row = await pool.fetchrow(
        "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'predictor_role'",
        guild.id,
    )
    if not row:
        return False
    role_id = int(row["value"])
    return any(r.id == role_id for r in member.roles)


async def require_can_create(pool, guild, member) -> None:
    if not await can_create(pool, guild, member):
        raise UserError("You don't have permission to create predictions.")


def can_manage(member, prediction) -> bool:
    """The prediction's creator or an admin may close/resolve it."""
    return member.id == prediction["creator_id"] or member.guild_permissions.administrator


async def _ensure_unlocked(pool, guild_id: int, user_id: int) -> None:
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from betting.")


# ── create ──

async def create(pool, guild, member, question: str, options_raw: str):
    """Create a prediction. `options_raw` is one option per line (or the legacy
    `Question? | Option1 | Option2` single string via .predictions text form)."""
    await require_can_create(pool, guild, member)
    question = (question or "").strip()
    if not question:
        raise UserError("Give the prediction a question.")
    options = [line.strip() for line in options_raw.splitlines() if line.strip()]
    if len(options) < 2:
        raise UserError("A prediction needs at least 2 options (one per line).")
    if len(options) > 20:
        raise UserError("A prediction can have at most 20 options.")
    return await _db_create(pool, guild.id, member.id, question, options)


# ── bet ──

async def place_bet(pool, guild, member, prediction_id: int, option_idx: int,
                    raw_amount: str) -> BetResult:
    await _ensure_unlocked(pool, guild.id, member.id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            pred = await lock_prediction(conn, prediction_id)
            if not pred or pred["guild_id"] != guild.id:
                raise UserError("That prediction no longer exists.")
            if pred["status"] != "open":
                raise UserError("This prediction is no longer accepting bets.")

            options = await get_prediction_options(conn, prediction_id)
            target = next((o for o in options if o["option_index"] == option_idx), None)
            if not target:
                raise UserError(f"Invalid option. Choose between 1 and {len(options)}.")

            await ensure_wallet(conn, guild.id, member.id)
            wallet = await lock_wallet(conn, guild.id, member.id)
            amount = parse_amount(raw_amount, wallet_balance=wallet["wallet"])
            if wallet["wallet"] < amount:
                raise UserError(f"You need {amount} but only have {wallet['wallet']}.")

            await update_wallet(conn, guild.id, member.id, -amount)
            await add_transaction(conn, guild.id, member.id, -amount, "prediction_bet",
                                  f"Bet on prediction #{prediction_id}")
            await place_prediction_bet(conn, prediction_id, target["id"], guild.id, member.id, amount)
    return BetResult(option_label=target["label"], amount=amount)


# ── close / resolve ──

async def close(pool, guild, member, prediction_id: int) -> CloseResult:
    async with pool.acquire() as conn:
        async with conn.transaction():
            pred = await lock_prediction(conn, prediction_id)
            if not pred or pred["guild_id"] != guild.id:
                raise UserError("That prediction no longer exists.")
            if not can_manage(member, pred):
                raise UserError("You don't have permission to close this prediction.")
            if pred["status"] != "open":
                raise UserError("This prediction is not open.")
            await _db_close(conn, prediction_id)
            options = await get_prediction_options(conn, prediction_id)
            totals = {r["option_id"]: r["total"] for r in await get_option_totals(conn, prediction_id)}
    return CloseResult(prediction=pred, options=options, totals=totals,
                       pool_total=sum(totals.values()))


async def resolve(pool, guild, member, prediction_id: int, winning_idx: int) -> ResolveResult:
    async with pool.acquire() as conn:
        async with conn.transaction():
            pred = await lock_prediction(conn, prediction_id)
            if not pred or pred["guild_id"] != guild.id:
                raise UserError("That prediction no longer exists.")
            if not can_manage(member, pred):
                raise UserError("You don't have permission to resolve this prediction.")
            if pred["status"] == "resolved":
                raise UserError("This prediction is already resolved.")

            options = await get_prediction_options(conn, prediction_id)
            winner = next((o for o in options if o["option_index"] == winning_idx), None)
            if not winner:
                raise UserError(f"Invalid option. Choose between 1 and {len(options)}.")

            totals = {r["option_id"]: r["total"] for r in await get_option_totals(conn, prediction_id)}
            pool_total = sum(totals.values())
            winner_pool = totals.get(winner["id"], 0)

            await _db_resolve(conn, prediction_id, winner["id"])

            payouts = []
            if winner_pool > 0 and pool_total > 0:
                for bet in await get_winning_bets(conn, prediction_id, winner["id"]):
                    payout = (bet["amount"] * pool_total) // winner_pool
                    await update_wallet(conn, guild.id, bet["user_id"], payout)
                    await add_transaction(conn, guild.id, bet["user_id"], payout,
                                          "prediction_win", f"Won prediction #{prediction_id}")
                    payouts.append((bet["user_id"], bet["amount"], payout))
    return ResolveResult(prediction=pred, winner=winner, pool_total=pool_total,
                         winner_pool=winner_pool, payouts=payouts)


# ── browse ──

async def list_active(pool, guild_id: int):
    return await get_active_predictions(pool, guild_id)


async def get_detail(pool, guild_id: int, prediction_id: int):
    """Return (prediction, options, totals dict); raise UserError if it's gone."""
    pred = await get_prediction(pool, prediction_id)
    if not pred or pred["guild_id"] != guild_id:
        raise UserError("That prediction no longer exists.")
    options = await get_prediction_options(pool, prediction_id)
    totals = {r["option_id"]: r["total"] for r in await get_option_totals(pool, prediction_id)}
    return pred, options, totals
