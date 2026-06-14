import discord
from discord.ext import commands

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from cogs.shop.db import (
    create_item, delete_item, get_item_by_name, get_shop_items,
    get_inventory, add_to_inventory, remove_member_data,
)
from core.checks import require_not_locked, UserLocked
from core.money import parse_amount, AmountError
from config import MAIN_CURRENCY_EMOJI


class Shop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @property
    def pool(self):
        return self.bot.pool

    async def cog_command_error(self, ctx, error):
        if isinstance(error, UserLocked):
            return
        raise error

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Clean up shop inventory when a member leaves, is kicked, or is banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    @commands.command()
    async def shop(self, ctx):
        """View all items available in the shop."""
        items = await get_shop_items(self.pool, ctx.guild.id)
        if not items:
            await ctx.send("The shop is empty!")
            return

        embed = discord.Embed(title="Shop", color=discord.Color.purple())
        for item in items:
            role_info = ""
            if item["item_type"] == "role" and item["role_given"]:
                role = ctx.guild.get_role(item["role_given"])
                role_info = f"\nGrants role: {role.mention}" if role else ""
            embed.add_field(
                name=f"{item['name']} — {item['price']}{MAIN_CURRENCY_EMOJI}",
                value=(item["description"] or "No description") + role_info,
                inline=False,
            )
        embed.set_footer(text="Use .buy <item name> to purchase an item.")
        await ctx.send(embed=embed)

    @commands.command()
    @require_not_locked()
    async def buy(self, ctx, *, name: str):
        """Buy an item from the shop by name."""
        item = await get_item_by_name(self.pool, ctx.guild.id, name)
        if not item:
            await ctx.send("Item not found in the shop.")
            return

        wallet = await ensure_wallet(self.pool, ctx.guild.id, ctx.author.id)
        if wallet["wallet"] < item["price"]:
            await ctx.send(f"You don't have enough! You need {item['price']}{MAIN_CURRENCY_EMOJI}.")
            return

        if item["item_type"] == "role" and item["role_given"]:
            role = ctx.guild.get_role(item["role_given"])
            if not role:
                await ctx.send("The role for this item no longer exists.")
                return
            if role in ctx.author.roles:
                await ctx.send("You already have this role!")
                return
            await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -item["price"])
            await add_transaction(self.pool, ctx.guild.id, ctx.author.id, -item["price"], "shop_buy", f"Bought {item['name']}")
            await ctx.author.add_roles(role)
            await ctx.send(f"You bought **{item['name']}** and received the {role.mention} role!")
            return

        await update_wallet(self.pool, ctx.guild.id, ctx.author.id, -item["price"])
        await add_transaction(self.pool, ctx.guild.id, ctx.author.id, -item["price"], "shop_buy", f"Bought {item['name']}")
        await add_to_inventory(self.pool, ctx.guild.id, ctx.author.id, item["id"])
        await ctx.send(f"You bought **{item['name']}** for {item['price']}{MAIN_CURRENCY_EMOJI}!")

    @commands.command(aliases=["inv"])
    async def inventory(self, ctx, member: discord.Member = None):
        """View your inventory or another member's inventory."""
        member = member or ctx.author
        items = await get_inventory(self.pool, ctx.guild.id, member.id)
        if not items:
            await ctx.send(f"{member.display_name} has no items.")
            return

        embed = discord.Embed(title=f"{member.display_name}'s Inventory", color=discord.Color.purple())
        for item in items:
            embed.add_field(
                name=f"{item['name']} x{item['quantity']}",
                value=item["description"] or "No description",
                inline=False,
            )
        await ctx.send(embed=embed)

    # ── Owner Commands ──

    @commands.command()
    @commands.is_owner()
    async def additem(self, ctx, price: str, *, name: str):
        """Admin: Add a new item to the shop. Usage: .additem <price> <name>"""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        try:
            item = await create_item(self.pool, ctx.guild.id, name, price)
            await ctx.send(f"**{item['name']}** added to the shop for {price}{MAIN_CURRENCY_EMOJI}. Use `.itemdesc {name} <description>` to add a description.")
        except Exception:
            await ctx.send("An item with that name already exists.")

    @commands.command()
    @commands.is_owner()
    async def addrole(self, ctx, price: str, role: discord.Role, *, name: str):
        """Admin: Add a role item to the shop. Usage: .addrole <price> @role <name>"""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        try:
            item = await create_item(self.pool, ctx.guild.id, name, price, item_type="role", role_given=role.id)
            await ctx.send(f"**{item['name']}** (grants {role.mention}) added to the shop for {price}{MAIN_CURRENCY_EMOJI}.")
        except Exception:
            await ctx.send("An item with that name already exists.")

    @commands.command()
    @commands.is_owner()
    async def itemdesc(self, ctx, name: str, *, description: str):
        """Admin: Set a description for a shop item. Usage: .itemdesc <name> <description>"""
        result = await self.pool.execute(
            "UPDATE items SET description = $3 WHERE guild_id = $1 AND LOWER(name) = LOWER($2)",
            ctx.guild.id, name, description,
        )
        if result == "UPDATE 0":
            await ctx.send("Item not found.")
        else:
            await ctx.send(f"Description for **{name}** updated.")

    @commands.command()
    @commands.is_owner()
    async def removeitem(self, ctx, *, name: str):
        """Admin: Remove an item from the shop. Usage: .removeitem <name>"""
        deleted = await delete_item(self.pool, ctx.guild.id, name)
        if deleted:
            await ctx.send(f"**{deleted['name']}** has been removed from the shop.")
        else:
            await ctx.send("Item not found.")
