from __future__ import annotations

"""Waifu service: buying, gifting, and status queries."""

from dataclasses import dataclass

from cogs.economy.db import ensure_wallet, lock_wallet, update_wallet, add_transaction
from cogs.waifu.db import (
    ensure_waifu, get_waifu, get_harem,
    set_waifu_owner, set_engagement,
)
from core.errors import UserError
from config import WAIFU_VALUE_MULTIPLIER


@dataclass
class WaifuBuyResult:
    """Result of purchasing a waifu."""
    target_id: int
    paid: int
    new_value: int
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

            new_value = int(max(pay, current_value) * WAIFU_VALUE_MULTIPLIER)
            await update_wallet(conn, guild_id, buyer_id, -pay)
            await add_transaction(conn, guild_id, buyer_id, -pay, "waifu_buy",
                                 f"Bought user {target_id} as waifu")
            await set_waifu_owner(conn, guild_id, target_id, buyer_id, new_value)

            buyer_waifu = await get_waifu(conn, guild_id, buyer_id)
            engaged = buyer_waifu and buyer_waifu["owner_id"] == target_id

            if engaged:
                await set_engagement(conn, guild_id, buyer_id)
                await set_engagement(conn, guild_id, target_id)

    return WaifuBuyResult(
        target_id=target_id,
        paid=pay,
        new_value=new_value,
        engaged=engaged,
    )


async def gift_waifu(pool, guild_id: int, giver_id: int, waifu_id: int, recipient_id: int) -> int:
    """Gift a waifu to another user. Returns the waifu's value. Raises UserError on validation failure."""
    async with pool.acquire() as conn:
        row = await get_waifu(conn, guild_id, waifu_id)
        if not row or row["owner_id"] != giver_id:
            raise UserError(f"You don't own this user.")
        if row["spouse_id"] is not None:
            raise UserError(f"This user is married and cannot be gifted.")
        await set_waifu_owner(conn, guild_id, waifu_id, recipient_id, row["value"])
    return row["value"]
