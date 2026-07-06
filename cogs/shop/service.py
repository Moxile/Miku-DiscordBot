from __future__ import annotations

"""Shop purchase logic shared by .buy, the shop buy buttons (cog.py), and the
Miku Menu (ui.py)."""

import discord

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from cogs.shop.db import add_to_inventory, grant_temp_role


async def purchase(bot, guild: discord.Guild, member: discord.Member, item) -> tuple[bool, str]:
    """Run a purchase for `member`. Returns (success, message).

    For role items the role is assigned before charging, so a failed grant
    never costs Flowers.
    """
    pool = bot.pool
    cur = bot.get_currency(guild.id)
    wallet = await ensure_wallet(pool, guild.id, member.id)
    if wallet["wallet"] < item["price"]:
        return False, f"You don't have enough! You need {item['price']:,}{cur.emoji}."

    if item["item_type"] == "role" and item["role_given"]:
        role = guild.get_role(item["role_given"])
        if not role:
            return False, "The role for this item no longer exists."
        # Permanent role items keep the original "already owned" guard.
        # Temporary roles can always be re-bought to extend the timer.
        if not item["role_duration"] and role in member.roles:
            return False, "You already have this role!"
        try:
            await member.add_roles(role, reason="Shop purchase")
        except discord.Forbidden:
            return False, "I couldn't assign that role — check my permissions and role position."
        await update_wallet(pool, guild.id, member.id, -item["price"])
        await add_transaction(pool, guild.id, member.id, -item["price"], "shop_buy", f"Bought {item['name']}")
        if item["role_duration"]:
            expires = await grant_temp_role(pool, guild.id, member.id, role.id, item["role_duration"])
            return True, (f"You bought **{item['name']}** — you have {role.mention} until "
                          f"<t:{int(expires.timestamp())}:R>.")
        return True, f"You bought **{item['name']}** and received the {role.mention} role!"

    await update_wallet(pool, guild.id, member.id, -item["price"])
    await add_transaction(pool, guild.id, member.id, -item["price"], "shop_buy", f"Bought {item['name']}")
    await add_to_inventory(pool, guild.id, member.id, item["id"])
    return True, f"You bought **{item['name']}** for {item['price']:,}{cur.emoji}!"
