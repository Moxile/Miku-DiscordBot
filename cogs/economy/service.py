from __future__ import annotations

"""Economy business logic, shared by the text commands (cog.py) and the
Miku Menu (ui.py).

Functions raise core.errors.UserError with a ready-to-send message for any
user-facing failure (bad amount, cooldown, wrong channel, locked user);
callers only format the success case. Lock and channel restrictions are
enforced here so the menu honours them even without the command decorators.
"""

import datetime
import secrets
from dataclasses import dataclass, field

import discord

from cogs.economy.db import (
    add_jail, add_transaction, ensure_wallet, get_jail_config, get_salary_roles_for,
    update_bank, update_wallet,
)
from config import (
    WORK_COOLDOWN, CRIME_COOLDOWN, DEFAULT_CRIME_SUCCESS_RATE,
    DEFAULT_CRIME_PAYOUT, DEFAULT_CRIME_FINE,
)
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError
from core.money import parse_amount


def _fmt_remaining(seconds: float) -> str:
    minutes, secs = divmod(int(seconds), 60)
    return f"{minutes}m {secs}s"


async def _ensure_unlocked(pool, guild_id: int, user_id: int):
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from using the economy.")


async def _ensure_channel(pool, guild_id: int, channel_id: int, setting_key: str):
    required = await get_required_channel(pool, guild_id, setting_key)
    if required is not None and channel_id != required:
        raise UserError(f"This can only be used in <#{required}>.")


async def _get_cooldown(pool, guild_id: int, user_id: int, command: str):
    """The expiry timestamp of an active cooldown, or None."""
    return await pool.fetchval(
        "SELECT expires_at FROM cooldowns WHERE guild_id = $1 AND user_id = $2 AND command = $3 AND expires_at > now()",
        guild_id, user_id, command,
    )


async def _set_cooldown(pool, guild_id: int, user_id: int, command: str, expires_at):
    await pool.execute(
        """INSERT INTO cooldowns (guild_id, user_id, command, expires_at)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (guild_id, user_id, command) DO UPDATE SET expires_at = EXCLUDED.expires_at""",
        guild_id, user_id, command, expires_at,
    )


async def get_balance(pool, guild_id: int, user_id: int):
    """The user's balance row (wallet, bank), created if missing."""
    return await ensure_wallet(pool, guild_id, user_id)


async def deposit(pool, guild_id: int, user_id: int, raw_amount: str) -> int:
    """Move money wallet → bank. Returns the amount moved."""
    await _ensure_unlocked(pool, guild_id, user_id)
    bal = await ensure_wallet(pool, guild_id, user_id)
    wallet = bal["wallet"]
    amount = parse_amount(raw_amount, wallet_balance=wallet)
    if wallet < amount:
        raise UserError("You can't deposit more than you have in your wallet!")
    await update_wallet(pool, guild_id, user_id, -amount)
    await update_bank(pool, guild_id, user_id, amount)
    await add_transaction(pool, guild_id, user_id, amount, "deposit")
    return amount


async def withdraw(pool, guild_id: int, user_id: int, raw_amount: str) -> int:
    """Move money bank → wallet. Returns the amount moved."""
    await _ensure_unlocked(pool, guild_id, user_id)
    bal = await ensure_wallet(pool, guild_id, user_id)
    bank = bal["bank"]
    amount = parse_amount(raw_amount, wallet_balance=bank)
    if bank < amount:
        raise UserError("You can't withdraw more than you have in your bank account!")
    await update_wallet(pool, guild_id, user_id, amount)
    await update_bank(pool, guild_id, user_id, -amount)
    await add_transaction(pool, guild_id, user_id, amount, "withdraw")
    return amount


async def gift(pool, guild_id: int, giver: discord.Member, recipient: discord.Member,
               raw_amount: str) -> int:
    """Move money from giver's wallet to recipient's wallet. Returns the amount."""
    await _ensure_unlocked(pool, guild_id, giver.id)
    bal = await ensure_wallet(pool, guild_id, giver.id)
    amount = parse_amount(raw_amount, wallet_balance=bal["wallet"])
    if bal["wallet"] < amount:
        raise UserError("You can't give more than you have in your wallet!")
    await ensure_wallet(pool, guild_id, recipient.id)
    await update_wallet(pool, guild_id, giver.id, -amount)
    await update_wallet(pool, guild_id, recipient.id, amount)
    await add_transaction(pool, guild_id, giver.id, -amount, "gift", f"Gift to {recipient}")
    await add_transaction(pool, guild_id, recipient.id, amount, "gift", f"Gift from {giver}")
    return amount


async def work(pool, guild_id: int, user_id: int, channel_id: int) -> int:
    """Do a work shift. Returns the earnings; raises UserError on cooldown."""
    await _ensure_unlocked(pool, guild_id, user_id)
    await _ensure_channel(pool, guild_id, channel_id, "work_channel")

    cooldown = await _get_cooldown(pool, guild_id, user_id, "work")
    if cooldown is not None:
        remaining = cooldown - datetime.datetime.now(datetime.timezone.utc)
        raise UserError(
            f"You need to wait *{_fmt_remaining(remaining.total_seconds())}* before you can work again."
        )

    earnings = secrets.randbelow(201) + 100
    await ensure_wallet(pool, guild_id, user_id)
    await update_wallet(pool, guild_id, user_id, earnings)
    await add_transaction(pool, guild_id, user_id, earnings, "work", "Earnings from work")

    cooldown_seconds = await pool.fetchval(
        "SELECT value FROM guild_settings WHERE guild_id = $1 AND key = 'work_cooldown'",
        guild_id,
    )
    cooldown_seconds = int(cooldown_seconds) if cooldown_seconds is not None else WORK_COOLDOWN
    expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=cooldown_seconds)
    await _set_cooldown(pool, guild_id, user_id, "work", expires)
    return earnings


async def get_crime_config(pool, guild_id: int) -> tuple[int, int, int]:
    """(success_rate, payout, fine) for this guild, falling back to defaults."""
    rows = await pool.fetch(
        "SELECT key, value FROM guild_settings WHERE guild_id = $1 AND key = ANY($2)",
        guild_id, ["crime_success_rate", "crime_payout", "crime_fine"],
    )
    settings = {r["key"]: int(r["value"]) for r in rows}
    return (
        settings.get("crime_success_rate", DEFAULT_CRIME_SUCCESS_RATE),
        settings.get("crime_payout", DEFAULT_CRIME_PAYOUT),
        settings.get("crime_fine", DEFAULT_CRIME_FINE),
    )


@dataclass
class CrimeResult:
    success: bool
    payout: int = 0            # on success, the flat reward
    loss: int = 0              # on failure, the flat fine
    jail_role_id: int = None   # on failure, when a prisoner role is configured
    jail_seconds: int = 0      # how long the prisoner role should be worn


async def crime(pool, guild_id: int, user_id: int, channel_id: int) -> CrimeResult:
    """Attempt a crime. Raises UserError on cooldown; the cooldown starts
    regardless of the outcome."""
    await _ensure_unlocked(pool, guild_id, user_id)
    await _ensure_channel(pool, guild_id, channel_id, "work_channel")

    now = datetime.datetime.now(datetime.timezone.utc)
    cooldown = await _get_cooldown(pool, guild_id, user_id, "crime")
    if cooldown is not None:
        remaining = cooldown - now
        raise UserError(
            f"You're laying low after your last job. Wait *{_fmt_remaining(remaining.total_seconds())}* before your next crime."
        )

    success_rate, payout, fine = await get_crime_config(pool, guild_id)
    bal = await ensure_wallet(pool, guild_id, user_id)
    # Only the solvent may risk it — a failed job can push your wallet into the red,
    # so people already in debt are locked out until they climb back above zero.
    if bal["wallet"] <= 0:
        raise UserError("You're broke — pay off your debt before you can risk another job.")

    await _set_cooldown(pool, guild_id, user_id, "crime",
                        now + datetime.timedelta(seconds=CRIME_COOLDOWN))

    if secrets.randbelow(100) < success_rate:
        await update_wallet(pool, guild_id, user_id, payout)
        await add_transaction(pool, guild_id, user_id, payout, "crime", "Successful crime")
        return CrimeResult(success=True, payout=payout)

    # Failure: pay a flat fine straight out of the wallet. Unlike the old percentage
    # penalty this is not clamped to the balance, so a bad run can leave the wallet
    # negative — the debt then blocks crime (and every other bet) until it's cleared.
    await update_wallet(pool, guild_id, user_id, -fine)
    await add_transaction(pool, guild_id, user_id, -fine, "crime", "Failed crime")

    # Jail: when the guild has bound a prisoner role, record the sentence so the
    # background release task (and the command handler) can apply/remove it.
    jail_role_id, jail_seconds = await get_jail_config(pool, guild_id)
    if jail_role_id is not None:
        release_at = now + datetime.timedelta(seconds=jail_seconds)
        await add_jail(pool, guild_id, user_id, jail_role_id, release_at)

    return CrimeResult(success=False, loss=fine,
                       jail_role_id=jail_role_id, jail_seconds=jail_seconds)


@dataclass
class CollectResult:
    collected: list = field(default_factory=list)    # (role_name, amount)
    on_cooldown: list = field(default_factory=list)  # (role_name, remaining_seconds)

    @property
    def total(self) -> int:
        return sum(amount for _, amount in self.collected)


async def collect(pool, guild: discord.Guild, member: discord.Member,
                  channel_id: int) -> CollectResult:
    """Pay out every ready salary role the member holds.
    Raises UserError if none of their roles pay a salary."""
    await _ensure_unlocked(pool, guild.id, member.id)
    await _ensure_channel(pool, guild.id, channel_id, "work_channel")

    role_ids = [r.id for r in member.roles]
    salary_roles = await get_salary_roles_for(pool, guild.id, role_ids)
    if not salary_roles:
        raise UserError("None of your roles pay a salary.")

    now = datetime.datetime.now(datetime.timezone.utc)
    await ensure_wallet(pool, guild.id, member.id)

    result = CollectResult()
    for sr in salary_roles:
        role = guild.get_role(sr["role_id"])
        role_name = role.name if role else f"role {sr['role_id']}"
        command = f"collect:{sr['role_id']}"
        expires_at = await _get_cooldown(pool, guild.id, member.id, command)
        if expires_at is not None:
            result.on_cooldown.append((role_name, (expires_at - now).total_seconds()))
            continue

        amount = sr["amount"]
        await update_wallet(pool, guild.id, member.id, amount)
        await add_transaction(pool, guild.id, member.id, amount, "salary", f"Salary for {role_name}")
        await _set_cooldown(pool, guild.id, member.id, command,
                            now + datetime.timedelta(seconds=sr["interval_seconds"]))
        result.collected.append((role_name, amount))
    return result


async def fetch_transactions(pool, guild_id: int, user_id: int,
                             counting_detail: bool = False) -> tuple[list[dict], bool]:
    """The user's transaction history, newest first.

    By default the (noisy) counting transactions are collapsed into a single
    summary row; pass counting_detail=True to get only the individual counting
    entries instead. Returns (rows, has_counting) where has_counting says a
    collapsed counting row is present.
    """
    if counting_detail:
        rows = await pool.fetch(
            """SELECT amount, tx_type, description, created_at FROM transactions
               WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'counting'
               ORDER BY created_at DESC""",
            guild_id, user_id,
        )
        return [dict(r) for r in rows], False

    other_rows = await pool.fetch(
        """SELECT amount, tx_type, description, created_at FROM transactions
           WHERE guild_id = $1 AND user_id = $2 AND tx_type != 'counting'
           ORDER BY created_at DESC""",
        guild_id, user_id,
    )
    counting_agg = await pool.fetchrow(
        """SELECT COALESCE(SUM(amount), 0) AS amount, COUNT(*) AS cnt, MAX(created_at) AS created_at
           FROM transactions
           WHERE guild_id = $1 AND user_id = $2 AND tx_type = 'counting'""",
        guild_id, user_id,
    )

    rows = [dict(r) for r in other_rows]
    has_counting = counting_agg and counting_agg["cnt"] > 0
    if has_counting:
        rows.append({
            "amount": counting_agg["amount"],
            "tx_type": "counting",
            "description": f"counting ×{counting_agg['cnt']}",
            "created_at": counting_agg["created_at"],
        })
        rows.sort(key=lambda r: r["created_at"], reverse=True)
    return rows, has_counting
