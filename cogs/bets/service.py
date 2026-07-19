from __future__ import annotations

"""Bookmaker-bet business logic, shared by the .bets launcher command (cog.py)
and the Miku Menu pages (ui.py).

A host funds a pool and offers fixed odds on an outcome; players accept with a
stake and either win stake*odds (host pays from the pool) or lose their stake
(host keeps it). Every user-facing failure raises core.errors.UserError with a
ready-to-show message; callers only format the success case.
"""

from dataclasses import dataclass
from decimal import ROUND_FLOOR, Decimal

from cogs.bets.db import (
    add_bet_option, add_bet_take, create_bet, decrement_bet_pool, get_active_bets,
    get_bet, get_bet_option_by_idx, get_bet_options, get_bet_takes, get_user_take,
    lock_bet, mark_bet_closed, set_bet_status,
)
from cogs.economy.db import add_transaction, ensure_wallet, lock_wallet, update_wallet
from core.checks import user_is_locked
from core.errors import UserError
from core.money import parse_amount


def format_odds(odds) -> str:
    """Render a numeric odds value like 10 or 2.5 without trailing zeros."""
    return f"{float(odds):g}"


def _as_decimal(odds) -> Decimal:
    """Coerce odds (a DB Decimal or a parsed float) to an exact Decimal."""
    return odds if isinstance(odds, Decimal) else Decimal(str(odds))


def payout_for(stake: int, odds) -> int:
    """`stake * odds`, floored, computed with Decimal so a multiplier like 1.29
    can't drift to 128 on a stake of 100 the way binary float `int(100 * 1.29)`
    does. Used for every stake→winnings conversion (payouts and liability)."""
    return int((Decimal(int(stake)) * _as_decimal(odds)).to_integral_value(rounding=ROUND_FLOOR))


def liability_for(stake: int, odds) -> int:
    """The host's exposure on a take: the winnings owed beyond the returned stake."""
    return payout_for(stake, odds) - int(stake)


@dataclass
class TakeResult:
    bet_id: int
    stake: int
    payout: int
    option_label: str | None


@dataclass
class ResolveResult:
    bet: object
    takes: list
    winning_option: object | None
    outcome: str | None          # "win"/"lose" for single-option bets, else None
    payouts: list                # list of (user_id, stake, payout)
    host_gain: int


# ── permissions ──

async def can_create(pool, guild, member, bot=None) -> bool:
    """Admin, or a holder of the configured predictor role (shared with Predictions).

    "Admin" is the bot's own owner check (bot owner, guild owner, Administrator,
    or the configured owner role — Bot.is_owner). Falls back to the raw
    Administrator permission if no bot is supplied.
    """
    if bot is not None:
        if await bot.is_owner(member):
            return True
    elif member.guild_permissions.administrator:
        return True
    row = await pool.fetchrow(
        "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'predictor_role'",
        guild.id,
    )
    if not row:
        return False
    role_id = int(row["value"])
    return any(r.id == role_id for r in member.roles)


async def require_can_create(pool, guild, member, bot=None) -> None:
    if not await can_create(pool, guild, member, bot):
        raise UserError("You don't have permission to create bets.")


async def _ensure_unlocked(pool, guild_id: int, user_id: int) -> None:
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from betting.")


# ── parsing ──

def parse_odds(raw: str) -> float:
    """Accept `x10`, `10`, or `2.5`; raise UserError on anything else."""
    s = str(raw).strip().lower().lstrip("x")
    try:
        odds = float(s)
    except ValueError:
        raise UserError(f"`{raw}` is not valid odds — use a number like `2.5` or `10`.")
    if odds <= 1:
        raise UserError("Odds must be greater than 1.")
    return odds


def parse_optional_amount(raw: str | None) -> int | None:
    """Parse a money amount, or return None when the field is left blank."""
    if raw is None or not str(raw).strip():
        return None
    return parse_amount(raw)


def parse_options(raw: str, *, max_stake: int | None, pool_amt: int | None) -> list[tuple[str, float]]:
    """Parse `<label> x<odds>` lines for a multi-option bet.

    The per-option pool check only applies when both a max stake and a finite
    pool are set; a bot-funded bet (pool_amt=None) or an open-ended max stake
    can't be bounded up front, so the runtime pool check on each take handles it.
    """
    options: list[tuple[str, float]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        tokens = line.split()
        if len(tokens) < 2:
            raise UserError(f"Couldn't read `{line}` — put each option as `<label> x<odds>`.")
        odds = parse_odds(tokens[-1])
        label = " ".join(tokens[:-1])
        if max_stake is not None and pool_amt is not None:
            max_liability = liability_for(max_stake, odds)
            if max_liability > pool_amt:
                raise UserError(
                    f"`{label}` needs the pool to cover {max_liability} at max stake, but the "
                    f"pool is only {pool_amt}. Lower its odds/max stake or raise the pool."
                )
        options.append((label, odds))
    if len(options) < 2:
        raise UserError("A multi-option bet needs at least 2 options.")
    return options


def _validate_stakes(min_stake: int | None, max_stake: int | None) -> None:
    if min_stake is not None and max_stake is not None and max_stake < min_stake:
        raise UserError("Max stake must be at least the min stake.")


def _stake_bounds_error(bet) -> str:
    lo, hi = bet["min_stake"], bet["max_stake"]
    if lo is not None and hi is not None:
        return f"Stake must be between {lo} and {hi}."
    if lo is not None:
        return f"Stake must be at least {lo}."
    return f"Stake must be at most {hi}."


async def _require_admin_for_bot_funded(bot, member, bot_funded: bool) -> None:
    # "Admin" = the bot's own owner check: bot owner, guild owner, Administrator,
    # or the configured owner role (see Bot.is_owner).
    if bot_funded and not await bot.is_owner(member):
        raise UserError("Only admins can create bot-funded bets.")


# ── create ──

async def create_single_bet(pool, guild, member, channel_id: int, *,
                            raw_odds: str, raw_min: str, raw_max: str,
                            raw_pool: str, description: str, bot_funded: bool = False,
                            bot=None):
    await require_can_create(pool, guild, member, bot)
    await _require_admin_for_bot_funded(bot, member, bot_funded)
    await _ensure_unlocked(pool, guild.id, member.id)
    odds = parse_odds(raw_odds)
    min_stake = parse_optional_amount(raw_min)
    max_stake = parse_optional_amount(raw_max)
    _validate_stakes(min_stake, max_stake)

    if bot_funded:
        # No host-funded pool: the bot covers every payout, so nothing is
        # deducted and pool/pool_remaining stay NULL.
        async with pool.acquire() as conn:
            async with conn.transaction():
                return await create_bet(
                    conn, guild.id, channel_id, member.id,
                    (description or "").strip(), odds, min_stake, max_stake, None,
                    bot_funded=True,
                )

    pool_amt = parse_amount(raw_pool)
    # A max stake lets us reject an under-funded pool up front; with no max, the
    # per-take pool check caps how much a player can actually stake.
    if max_stake is not None:
        max_liability = liability_for(max_stake, odds)
        if max_liability > pool_amt:
            raise UserError(
                f"Pool too small: a single max-stake bet would cost {max_liability} but the "
                f"pool is only {pool_amt}. Raise the pool or lower the max stake."
            )

    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild.id, member.id)
            wallet = await lock_wallet(conn, guild.id, member.id)
            if wallet["wallet"] < pool_amt:
                raise UserError(
                    f"You need {pool_amt} to fund this bet's pool but only have {wallet['wallet']}."
                )
            await update_wallet(conn, guild.id, member.id, -pool_amt)
            await add_transaction(conn, guild.id, member.id, -pool_amt, "bet_pool", "Funded bet pool")
            return await create_bet(
                conn, guild.id, channel_id, member.id,
                (description or "").strip(), odds, min_stake, max_stake, pool_amt,
            )


async def create_multi_bet(pool, guild, member, channel_id: int, *,
                           raw_min: str, raw_max: str, raw_pool: str,
                           description: str, raw_options: str, bot_funded: bool = False,
                           bot=None):
    await require_can_create(pool, guild, member, bot)
    await _require_admin_for_bot_funded(bot, member, bot_funded)
    await _ensure_unlocked(pool, guild.id, member.id)
    min_stake = parse_optional_amount(raw_min)
    max_stake = parse_optional_amount(raw_max)
    pool_amt = None if bot_funded else parse_amount(raw_pool)
    _validate_stakes(min_stake, max_stake)
    options = parse_options(raw_options, max_stake=max_stake, pool_amt=pool_amt)

    if bot_funded:
        async with pool.acquire() as conn:
            async with conn.transaction():
                bet = await create_bet(
                    conn, guild.id, channel_id, member.id,
                    (description or "").strip(), None, min_stake, max_stake, None,
                    is_multi=True, bot_funded=True,
                )
                for i, (label, odds) in enumerate(options, start=1):
                    await add_bet_option(conn, bet["id"], i, label, odds)
        return bet, options

    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild.id, member.id)
            wallet = await lock_wallet(conn, guild.id, member.id)
            if wallet["wallet"] < pool_amt:
                raise UserError(
                    f"You need {pool_amt} to fund this bet's pool but only have {wallet['wallet']}."
                )
            await update_wallet(conn, guild.id, member.id, -pool_amt)
            await add_transaction(conn, guild.id, member.id, -pool_amt, "bet_pool",
                                  "Funded multi-option bet pool")
            bet = await create_bet(
                conn, guild.id, channel_id, member.id,
                (description or "").strip(), None, min_stake, max_stake, pool_amt, is_multi=True,
            )
            for i, (label, odds) in enumerate(options, start=1):
                await add_bet_option(conn, bet["id"], i, label, odds)
    return bet, options


# ── accept ──

async def place_take(pool, guild, member, bet_id: int, *, option_idx: int | None,
                     raw_stake: str) -> TakeResult:
    await _ensure_unlocked(pool, guild.id, member.id)
    async with pool.acquire() as conn:
        async with conn.transaction():
            bet = await lock_bet(conn, bet_id)
            if not bet or bet["guild_id"] != guild.id:
                raise UserError("That bet no longer exists.")
            if bet["status"] != "open":
                raise UserError("This bet is no longer accepting entries.")
            if bet["host_id"] == member.id:
                raise UserError("You can't bet on a bet you're hosting.")

            option = None
            if bet["is_multi"]:
                if option_idx is None:
                    raise UserError("Pick an option to bet on.")
                option = await get_bet_option_by_idx(conn, bet_id, option_idx)
                if not option:
                    raise UserError(f"No option #{option_idx} on this bet.")
                existing = await get_user_take(conn, bet_id, member.id)
                if existing and existing["option_id"] != option["id"]:
                    raise UserError(
                        "You've already picked a different option on this bet — "
                        "you can add to your existing pick, but can't switch options."
                    )
                odds = option["odds"]
            else:
                odds = bet["odds"]

            stake = parse_amount(raw_stake)
            if bet["min_stake"] is not None and stake < bet["min_stake"]:
                raise UserError(_stake_bounds_error(bet))
            if bet["max_stake"] is not None and stake > bet["max_stake"]:
                raise UserError(_stake_bounds_error(bet))

            liability = liability_for(stake, odds)
            if not bet["bot_funded"] and liability > bet["pool_remaining"]:
                raise UserError(
                    f"The pool can only cover {bet['pool_remaining']} more in winnings; "
                    f"your bet would need {liability}."
                )

            await ensure_wallet(conn, guild.id, member.id)
            wallet = await lock_wallet(conn, guild.id, member.id)
            if wallet["wallet"] < stake:
                raise UserError(f"You need {stake} but only have {wallet['wallet']}.")

            await update_wallet(conn, guild.id, member.id, -stake)
            await add_transaction(conn, guild.id, member.id, -stake, "bet_take", f"Bet on #{bet_id}")
            if not bet["bot_funded"]:
                await decrement_bet_pool(conn, bet_id, liability)
            await add_bet_take(conn, bet_id, member.id, stake, liability,
                               option_id=option["id"] if option else None)

    return TakeResult(bet_id=bet_id, stake=stake, payout=payout_for(stake, odds),
                      option_label=option["label"] if option else None)


# ── close / resolve / cancel ──

async def close_bet(pool, guild, member, bet_id: int) -> None:
    """Stop new takes on an open bet without resolving it yet.

    Lets a host cut off betting once an outcome looks likely, instead of
    players racing to pile on right before the result is known.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            bet = await lock_bet(conn, bet_id)
            if not bet or bet["guild_id"] != guild.id:
                raise UserError("That bet no longer exists.")
            if bet["host_id"] != member.id and not member.guild_permissions.administrator:
                raise UserError("Only the host (or an admin) can close this bet.")
            if bet["status"] != "open":
                raise UserError("This bet isn't open.")
            await mark_bet_closed(conn, bet_id)


async def resolve_bet(pool, guild, member, bet_id: int, outcome_raw: str) -> ResolveResult:
    async with pool.acquire() as conn:
        async with conn.transaction():
            bet = await lock_bet(conn, bet_id)
            if not bet or bet["guild_id"] != guild.id:
                raise UserError("That bet no longer exists.")
            if bet["host_id"] != member.id and not member.guild_permissions.administrator:
                raise UserError("Only the host (or an admin) can resolve this bet.")
            if bet["status"] not in ("open", "closed"):
                raise UserError("This bet is already resolved.")

            takes = await get_bet_takes(conn, bet_id)
            winning_option = None
            outcome = None

            if bet["is_multi"]:
                try:
                    option_idx = int(str(outcome_raw).strip())
                except (TypeError, ValueError):
                    raise UserError("Pick the winning option number.")
                winning_option = await get_bet_option_by_idx(conn, bet_id, option_idx)
                if not winning_option:
                    raise UserError(f"No option #{option_idx} on this bet.")
                winners = [t for t in takes if t["option_id"] == winning_option["id"]]
                losers = [t for t in takes if t["option_id"] != winning_option["id"]]
                win_odds = winning_option["odds"]
                status = "resolved"
            else:
                outcome = str(outcome_raw).strip().lower()
                if outcome not in ("win", "lose"):
                    raise UserError("Outcome must be `win` (players win) or `lose` (players lose).")
                winners = takes if outcome == "win" else []
                losers = takes if outcome == "lose" else []
                win_odds = bet["odds"]
                status = "won" if outcome == "win" else "lost"

            payouts = []
            for t in winners:
                payout = payout_for(t["stake"], win_odds)
                await update_wallet(conn, guild.id, t["user_id"], payout)
                await add_transaction(conn, guild.id, t["user_id"], payout, "bet_win", f"Won bet #{bet_id}")
                payouts.append((t["user_id"], t["stake"], payout))

            # A bot-funded bet has no host stake: the bot pays winners and absorbs
            # losers' stakes, so the host neither profits nor is refunded.
            if bet["bot_funded"]:
                host_gain = 0
            else:
                # Host keeps whatever pool was never reserved, plus liability freed
                # by losing takes (never paid out), plus the losers' stakes.
                host_gain = (bet["pool_remaining"]
                             + sum(t["liability"] for t in losers)
                             + sum(t["stake"] for t in losers))
            if host_gain > 0:
                reason = "bet_pool_refund" if not losers else "bet_lose"
                await update_wallet(conn, guild.id, bet["host_id"], host_gain)
                await add_transaction(conn, guild.id, bet["host_id"], host_gain, reason,
                                      f"Bet #{bet_id} resolved")

            await set_bet_status(conn, bet_id, status)

    return ResolveResult(bet=bet, takes=takes, winning_option=winning_option,
                         outcome=outcome, payouts=payouts, host_gain=host_gain)


async def cancel_bet(pool, guild, member, bet_id: int) -> int:
    """Cancel an open bet with no takes, refunding the pool. Returns the refund."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            bet = await lock_bet(conn, bet_id)
            if not bet or bet["guild_id"] != guild.id:
                raise UserError("That bet no longer exists.")
            if bet["host_id"] != member.id and not member.guild_permissions.administrator:
                raise UserError("Only the host (or an admin) can cancel this bet.")
            if bet["status"] not in ("open", "closed"):
                raise UserError("This bet is already resolved.")
            takes = await get_bet_takes(conn, bet_id)
            if takes:
                raise UserError(
                    f"Can't cancel: {len(takes)} bet(s) already placed. Resolve it instead."
                )
            # Bot-funded bets took no host pool, so there's nothing to refund.
            refund = 0 if bet["bot_funded"] else bet["pool"]
            if refund > 0:
                await update_wallet(conn, guild.id, bet["host_id"], refund)
                await add_transaction(conn, guild.id, bet["host_id"], refund,
                                      "bet_cancel", f"Cancelled bet #{bet_id}")
            await set_bet_status(conn, bet_id, "cancelled")
    return refund


# ── browse ──

async def list_open_bets(pool, guild_id: int):
    return await get_active_bets(pool, guild_id)


async def get_bet_detail(pool, guild_id: int, bet_id: int):
    """Return (bet, options, takes); raise UserError if the bet is gone."""
    bet = await get_bet(pool, bet_id)
    if not bet or bet["guild_id"] != guild_id:
        raise UserError("That bet no longer exists.")
    options = await get_bet_options(pool, bet_id) if bet["is_multi"] else []
    takes = await get_bet_takes(pool, bet_id)
    return bet, options, takes


async def get_options(pool, bet_id: int):
    return await get_bet_options(pool, bet_id)


def option_totals(takes) -> dict[int, int]:
    """Sum of stakes placed on each option_id, for the multi-bet pool breakdown."""
    totals: dict[int, int] = {}
    for t in takes:
        if t["option_id"] is not None:
            totals[t["option_id"]] = totals.get(t["option_id"], 0) + t["stake"]
    return totals
