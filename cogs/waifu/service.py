from __future__ import annotations

"""Waifu service: buying, gifting, and status queries."""

from dataclasses import dataclass

from cogs.economy.db import ensure_wallet, lock_wallet, update_wallet, add_transaction
from cogs.waifu.db import (
    ensure_waifu, get_waifu, get_harem,
    set_waifu_owner, engage_if_mutual,
)
from core.errors import UserError
from config import WAIFU_VALUE_MULTIPLIER, WAIFU_RESALE_RATE


@dataclass
class WaifuBuyResult:
    """Result of purchasing a waifu."""
    target_id: int
    paid: int
    new_value: int
    engaged: bool
    prev_owner_id: int | None = None
    payout: int = 0


@dataclass
class WaifuGiftResult:
    """Result of gifting a waifu away."""
    value: int
    engaged: bool


async def buy_waifu(pool, guild_id: int, buyer_id: int, target_id: int, amount: int | None) -> WaifuBuyResult:
    """Buy a user as a waifu. Raises UserError on validation failure.

    If amount is None, uses the waifu's current value. Otherwise requires amount >= current value.
    Returns WaifuBuyResult with engagement status.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild_id, buyer_id)
            buyer_bal = await lock_wallet(conn, guild_id, buyer_id)
            target_row = await ensure_waifu(conn, guild_id, target_id)

            if target_row["spouse_id"] is not None:
                raise UserError(f"This user is married and cannot be bought.")

            current_value = target_row["value"]
            pay = amount if amount is not None else current_value

            if pay < current_value:
                raise UserError(f"This user is worth {current_value:,}. You must pay at least that much.")
            if buyer_bal["wallet"] < pay:
                raise UserError(f"You don't have enough to pay {pay:,}.")

            prev_owner = target_row["owner_id"]
            new_value = int(max(pay, current_value) * WAIFU_VALUE_MULTIPLIER)
            await update_wallet(conn, guild_id, buyer_id, -pay)
            await add_transaction(conn, guild_id, buyer_id, -pay, "waifu_buy",
                                 f"Bought user {target_id} as waifu")

            # Previous owner is paid a share of the sale; the rest is a money sink.
            payout = 0
            if prev_owner and prev_owner != buyer_id:
                payout = int(pay * WAIFU_RESALE_RATE)
                await ensure_wallet(conn, guild_id, prev_owner)
                await update_wallet(conn, guild_id, prev_owner, payout)
                await add_transaction(conn, guild_id, prev_owner, payout, "waifu_sale",
                                     f"Sold {target_id} to {buyer_id}")

            await set_waifu_owner(conn, guild_id, target_id, buyer_id, new_value)
            engaged = await engage_if_mutual(conn, guild_id, buyer_id, target_id)

    return WaifuBuyResult(
        target_id=target_id,
        paid=pay,
        new_value=new_value,
        engaged=engaged,
        prev_owner_id=prev_owner if payout else None,
        payout=payout,
    )


async def gift_waifu(pool, guild_id: int, giver_id: int, waifu_id: int, recipient_id: int) -> WaifuGiftResult:
    """Gift a waifu to another user. Raises UserError on validation failure."""
    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await get_waifu(conn, guild_id, waifu_id)
            if not row or row["owner_id"] != giver_id:
                raise UserError(f"You don't own this user.")
            if row["spouse_id"] is not None:
                raise UserError(f"This user is married and cannot be gifted.")
            await set_waifu_owner(conn, guild_id, waifu_id, recipient_id, row["value"])
            # A gift can create mutual ownership, which engages the pair.
            engaged = await engage_if_mutual(conn, guild_id, recipient_id, waifu_id)
    return WaifuGiftResult(value=row["value"], engaged=engaged)
