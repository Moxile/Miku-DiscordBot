from __future__ import annotations

"""Mission funding logic shared by .fund (cog.py) and the Miku Menu (ui.py)."""

from dataclasses import dataclass

import asyncpg

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from cogs.missions.db import add_funding, get_mission, get_mission_by_name, set_mission_status
from core.checks import get_required_channel, user_is_locked
from core.errors import UserError
from core.money import parse_amount


@dataclass
class FundResult:
    mission: asyncpg.Record  # the mission row after funding
    amount: int
    completed: bool          # this contribution pushed it over the goal


async def fund_mission(pool, guild_id: int, user_id: int, raw_amount: str,
                       channel_id: int, *, mission_name: str = None,
                       mission_id: int = None) -> FundResult:
    """Fund a mission from the user's wallet, by name or by id."""
    if await user_is_locked(pool, guild_id, user_id):
        raise UserError("You are locked from using the economy.")
    required = await get_required_channel(pool, guild_id, "missions_channel")
    if required is not None and channel_id != required:
        raise UserError(f"This can only be used in <#{required}>.")

    amount = parse_amount(raw_amount)

    if mission_id is not None:
        mission = await get_mission(pool, guild_id, mission_id)
    else:
        mission = await get_mission_by_name(pool, guild_id, mission_name)
    if not mission:
        label = f"#{mission_id}" if mission_id is not None else f"**{mission_name}**"
        raise UserError(f"No mission named {label} found.")
    if mission["status"] != "active":
        raise UserError(f"Mission **{mission['name']}** is no longer active.")

    async with pool.acquire() as conn:
        async with conn.transaction():
            bal = await ensure_wallet(conn, guild_id, user_id)
            if bal["wallet"] < amount:
                raise UserError(f"You only have {bal['wallet']:,} in your wallet.")

            await update_wallet(conn, guild_id, user_id, -amount)
            await add_transaction(
                conn, guild_id, user_id, -amount, "mission_fund",
                f"Funded mission #{mission['id']}: {mission['name']}",
            )
            updated = await add_funding(conn, mission["id"], guild_id, user_id, amount)

            completed = updated["funded"] >= updated["goal"]
            if completed:
                await set_mission_status(conn, mission["id"], "completed")

    return FundResult(mission=updated, amount=amount, completed=completed)
