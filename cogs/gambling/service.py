from __future__ import annotations

"""Gambling service: bet validation and simple game logic for UI integration."""

import asyncio
import secrets
from dataclasses import dataclass

import discord

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from core.errors import UserError
from core.money import parse_amount, AmountError


COINFLIP_HOUSE_EDGE = 0.95
BETFLIP_HOUSE_EDGE = 0.95


@dataclass
class BetFlipResult:
    """Results of a betflip round(s)."""
    choice: str
    results: list[str]
    wins: int
    losses: int
    net: int
    total_bet: int


async def validate_bet(pool, guild_id: int, user_id: int, amount: int, min_amount: int = 2, max_bet: int | None = None) -> None:
    """Validate that a bet is legal; raise UserError if not.

    Caller must provide max_bet from cog.get_max_bet(guild_id).
    """
    if not isinstance(amount, int) or amount <= 0:
        raise UserError("Bet must be a positive integer.")
    if amount < min_amount:
        raise UserError(f"Minimum bet is {min_amount}.")
    if max_bet is not None and amount > max_bet:
        raise UserError(f"Maximum bet in this server is {max_bet:,}.")
    wallet = await ensure_wallet(pool, guild_id, user_id)
    if wallet["wallet"] < amount:
        raise UserError(f"You don't have enough to bet {amount:,}.")


async def betflip(pool, guild_id: int, user_id: int, choice: str, bet_per_try: int, tries: int, max_bet: int | None = None) -> BetFlipResult:
    """Play betflip game: bet on heads or tails multiple times.

    Returns BetFlipResult with outcome. Raises UserError on validation failure.
    """
    if tries <= 0:
        raise UserError("Tries must be positive.")
    tries = min(tries, 10)
    total_bet = bet_per_try * tries

    await validate_bet(pool, guild_id, user_id, total_bet, min_amount=2, max_bet=max_bet)

    if choice.lower() not in ["h", "t"]:
        raise UserError("Choice must be 'h' or 't'.")

    choice_u = choice.upper()
    results = []
    net = 0
    for _ in range(tries):
        result = "H" if secrets.randbelow(2) == 0 else "T"
        results.append(result)
        if result == choice_u:
            net += bet_per_try
        else:
            net -= bet_per_try

    if net > 0:
        net = int(BETFLIP_HOUSE_EDGE * net)

    await update_wallet(pool, guild_id, user_id, net)
    await add_transaction(pool, guild_id, user_id, net, "betflip",
                         f"{tries} tries at {bet_per_try} each")

    return BetFlipResult(
        choice=choice_u,
        results=results,
        wins=results.count(choice_u),
        losses=tries - results.count(choice_u),
        net=net,
        total_bet=total_bet,
    )
