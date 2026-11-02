from __future__ import annotations

"""Waifu service: buying, gifting, and status queries."""

from dataclasses import dataclass

from cogs.economy.db import ensure_wallet, lock_wallet, update_wallet, add_transaction
from cogs.waifu.db import (
    ensure_waifu, get_waifu, get_harem,
    set_waifu_owner, engage_if_mutual,
)
from core.errors import UserError
from config import WAIFU_ENGAGED_TAX_RATE, WAIFU_VALUE_MULTIPLIER, WAIFU_RESALE_RATE


@dataclass
class WaifuBuyResult:
    """Result of purchasing a waifu."""
    target_id: int
    paid: int
    base_price: int
    tax: int
    new_value: int
    engaged: bool
    prev_owner_id: int | None = None
    payout: int = 0


@dataclass
class WaifuGiftResult:
    """Result of gifting a waifu away."""
    value: int
    engaged: bool


def asking_price(row) -> int:
    """Minimum untaxed price to buy this waifu. An unowned waifu sells for its
    value; an owned one costs one multiplier-step more. Whatever the buyer pays
    becomes the waifu's new value."""
    if row["owner_id"] is None:
        return row["value"]
    return int(row["value"] * WAIFU_VALUE_MULTIPLIER)


def purchase_cost(base_price: int, engaged: bool) -> tuple[int, int]:
    """Return (total cost, tax) without folding the engagement tax into value."""
    tax = int(base_price * WAIFU_ENGAGED_TAX_RATE) if engaged else 0
    return base_price + tax, tax


async def buy_waifu(pool, guild_id: int, buyer_id: int, target_id: int, amount: int | None) -> WaifuBuyResult:
    """Buy a user as a waifu. Raises UserError on validation failure.

    If amount is None, pays the asking price. Otherwise requires amount >= asking price.
    ``amount`` is the untaxed offer and becomes the waifu's new value; an engaged
    target adds a 40% sink on top.
    Returns WaifuBuyResult with engagement status.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            await ensure_wallet(conn, guild_id, buyer_id)
            buyer_bal = await lock_wallet(conn, guild_id, buyer_id)
            target_row = await ensure_waifu(conn, guild_id, target_id)

            if target_row["spouse_id"] is not None:
                raise UserError(f"This user is married and cannot be bought.")

            min_price = asking_price(target_row)
            base_price = amount if amount is not None else min_price

            if base_price < min_price:
                raise UserError(f"This user costs at least {min_price:,}.")
            total_cost, tax = purchase_cost(base_price, target_row["engaged_since"] is not None)
            if buyer_bal["wallet"] < total_cost:
                raise UserError(f"You don't have enough to pay {total_cost:,}.")

            prev_owner = target_row["owner_id"]
            new_value = base_price
            await update_wallet(conn, guild_id, buyer_id, -total_cost)
            await add_transaction(conn, guild_id, buyer_id, -total_cost, "waifu_buy",
                                 f"Bought user {target_id} as waifu (base {base_price}, tax {tax})")

            # Previous owner is paid a share of the sale; the rest is a money sink.
            payout = 0
            if prev_owner and prev_owner != buyer_id:
                payout = int(base_price * WAIFU_RESALE_RATE)
                await ensure_wallet(conn, guild_id, prev_owner)
                await update_wallet(conn, guild_id, prev_owner, payout)
                await add_transaction(conn, guild_id, prev_owner, payout, "waifu_sale",
                                     f"Sold {target_id} to {buyer_id}")

            await set_waifu_owner(conn, guild_id, target_id, buyer_id, new_value)
            engaged = await engage_if_mutual(conn, guild_id, buyer_id, target_id)

    return WaifuBuyResult(
        target_id=target_id,
        paid=total_cost,
        base_price=base_price,
        tax=tax,
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
