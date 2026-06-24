import math

import discord
from discord.ext import commands, tasks

from cogs.economy.db import ensure_wallet, update_wallet, add_transaction
from cogs.shop.db import (
    create_item, delete_item, get_item_by_name, get_item_by_id, get_shop_items,
    get_inventory, add_to_inventory, remove_member_data,
    grant_temp_role, get_expired_temp_roles, delete_temp_role,
)
from core.checks import require_not_locked, user_is_locked
from core.money import parse_amount, AmountError
from core.time_utils import parse_duration, humanize_duration
from core.names import format_name

SHOP_COLOR = discord.Color.from_rgb(57, 197, 187)  # Miku teal
MIN_TEMP_ROLE_SECONDS = 60
SHOP_ITEMS_PER_PAGE = 5  # one row of buy buttons per page (Discord caps a row at 5)


def _is_role_item(item) -> bool:
    return item["item_type"] == "role" and bool(item["role_given"])


def _item_text(guild: discord.Guild, item) -> str:
    """Markdown body for one item's card (the left side of a Section)."""
    if _is_role_item(item):
        role = guild.get_role(item["role_given"])
        bits = [f"🎭 **{item['name']}**"]
        if role:
            bits.append(role.mention)
        bits.append(
            f"⏳ {humanize_duration(item['role_duration'])}"
            if item["role_duration"] else "Permanent"
        )
        head = " · ".join(bits)
    else:
        head = f"📦 **{item['name']}**"
    desc = item["description"] or "No description"
    return f"{head}\n-# {desc}"


class ShopView(discord.ui.LayoutView):
    """Interactive store rendered with Components V2: each item is a card
    (Section) with its Buy button as a side accessory, all wrapped in a colored
    Container, with Previous/Next pagination at the bottom.

    Anyone can use it — buying replies privately to the clicker; paging updates
    the shared message."""

    def __init__(self, cog: "Shop", guild: discord.Guild, items, *,
                 per_page: int = SHOP_ITEMS_PER_PAGE, timeout: float = 180):
        super().__init__(timeout=timeout)
        self.cog = cog
        self.guild = guild
        self.items = items
        self.per_page = per_page
        self.page = 0
        self.message: discord.Message | None = None
        self._build()

    @property
    def total_pages(self) -> int:
        return max(1, math.ceil(len(self.items) / self.per_page))

    def _page_items(self):
        start = self.page * self.per_page
        return self.items[start:start + self.per_page]

    def _build(self) -> None:
        """Rebuild the whole layout for the current page."""
        self.clear_items()
        cur = self.cog.bot.get_currency(self.guild.id)

        container = discord.ui.Container(accent_colour=SHOP_COLOR)
        container.add_item(discord.ui.TextDisplay(
            f"## {cur.emoji} {self.guild.name} Shop\n"
            "Click a **Buy** button to purchase instantly, or use `.buy <name>`."
        ))
        container.add_item(discord.ui.Separator())

        for item in self._page_items():
            buy_btn = discord.ui.Button(
                label=f"{item['price']:,}",
                emoji=cur.emoji,
                style=discord.ButtonStyle.success,
                custom_id=f"shop_buy:{item['id']}",
            )
            buy_btn.callback = self._make_buy_callback(item["id"])
            container.add_item(discord.ui.Section(
                discord.ui.TextDisplay(_item_text(self.guild, item)),
                accessory=buy_btn,
            ))
            container.add_item(discord.ui.Separator())

        if self.total_pages > 1:
            container.add_item(discord.ui.TextDisplay(f"-# Page {self.page + 1}/{self.total_pages}"))
        self.add_item(container)

        if self.total_pages > 1:
            prev_btn = discord.ui.Button(
                label="Previous Page", style=discord.ButtonStyle.secondary,
                disabled=self.page == 0, custom_id="shop_prev",
            )
            next_btn = discord.ui.Button(
                label="Next Page", style=discord.ButtonStyle.primary,
                disabled=self.page >= self.total_pages - 1, custom_id="shop_next",
            )
            prev_btn.callback = self._prev
            next_btn.callback = self._next
            self.add_item(discord.ui.ActionRow(prev_btn, next_btn))

    def _make_buy_callback(self, item_id: int):
        async def callback(interaction: discord.Interaction):
            item = await get_item_by_id(self.cog.pool, self.guild.id, item_id)
            if not item:
                await interaction.response.send_message(
                    "That item is no longer available.", ephemeral=True)
                return
            if await user_is_locked(self.cog.pool, self.guild.id, interaction.user.id):
                await interaction.response.send_message(
                    "You're locked out of the economy.", ephemeral=True)
                return
            _, message = await self.cog._purchase(self.guild, interaction.user, item)
            await interaction.response.send_message(message, ephemeral=True)
        return callback

    async def _prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build()
        await interaction.response.edit_message(view=self)

    async def _next(self, interaction: discord.Interaction):
        self.page = min(self.total_pages - 1, self.page + 1)
        self._build()
        await interaction.response.edit_message(view=self)

    async def on_timeout(self) -> None:
        for child in self.walk_children():
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        if self.message is not None:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass


class Shop(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.expire_roles.start()

    def cog_unload(self):
        self.expire_roles.cancel()

    @property
    def pool(self):
        return self.bot.pool

    # ── Background task ──

    @tasks.loop(seconds=60)
    async def expire_roles(self):
        async with self.pool.acquire() as conn:
            due = await get_expired_temp_roles(conn)
        for row in due:
            guild = self.bot.get_guild(row["guild_id"])
            member = guild.get_member(row["user_id"]) if guild else None
            role = guild.get_role(row["role_id"]) if guild else None
            if member and role and role in member.roles:
                try:
                    await member.remove_roles(role, reason="Temporary shop role expired")
                except discord.Forbidden:
                    pass
            async with self.pool.acquire() as conn:
                await delete_temp_role(conn, row["id"])

    @expire_roles.before_loop
    async def before_expire_roles(self):
        await self.bot.wait_until_ready()

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Clean up shop inventory when a member leaves, is kicked, or is banned."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await remove_member_data(conn, member.guild.id, member.id)

    async def _purchase(self, guild: discord.Guild, member: discord.Member, item):
        """Run a purchase for `member`. Returns (success, message).

        Shared by the `.buy` command and the shop buy buttons. For role items the
        role is assigned before charging, so a failed grant never costs Flowers.
        """
        cur = self.bot.get_currency(guild.id)
        wallet = await ensure_wallet(self.pool, guild.id, member.id)
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
            await update_wallet(self.pool, guild.id, member.id, -item["price"])
            await add_transaction(self.pool, guild.id, member.id, -item["price"], "shop_buy", f"Bought {item['name']}")
            if item["role_duration"]:
                expires = await grant_temp_role(self.pool, guild.id, member.id, role.id, item["role_duration"])
                return True, (f"You bought **{item['name']}** — you have {role.mention} until "
                              f"<t:{int(expires.timestamp())}:R>.")
            return True, f"You bought **{item['name']}** and received the {role.mention} role!"

        await update_wallet(self.pool, guild.id, member.id, -item["price"])
        await add_transaction(self.pool, guild.id, member.id, -item["price"], "shop_buy", f"Bought {item['name']}")
        await add_to_inventory(self.pool, guild.id, member.id, item["id"])
        return True, f"You bought **{item['name']}** for {item['price']:,}{cur.emoji}!"

    @commands.command()
    async def shop(self, ctx):
        """View all items available in the shop."""
        items = await get_shop_items(self.pool, ctx.guild.id)
        if not items:
            await ctx.send("The shop is empty!")
            return

        # Roles first, then other goods, so the interactive store lists them grouped.
        roles = [i for i in items if _is_role_item(i)]
        goods = [i for i in items if not _is_role_item(i)]

        view = ShopView(self, ctx.guild, roles + goods)
        view.message = await ctx.send(view=view)

    @commands.command()
    @require_not_locked()
    async def buy(self, ctx, *, name: str):
        """Buy an item from the shop by name."""
        item = await get_item_by_name(self.pool, ctx.guild.id, name)
        if not item:
            await ctx.send("Item not found in the shop.")
            return
        _, message = await self._purchase(ctx.guild, ctx.author, item)
        await ctx.send(message)

    @commands.command(aliases=["inv"])
    async def inventory(self, ctx, member: discord.Member = None):
        """View your inventory or another member's inventory."""
        member = member or ctx.author
        items = await get_inventory(self.pool, ctx.guild.id, member.id)
        if not items:
            await ctx.send(f"{format_name(member)} has no items.")
            return

        embed = discord.Embed(title=f"{format_name(member)}'s Inventory", color=SHOP_COLOR)
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
        """Add a new item to the shop. Usage: .additem <price> <name>"""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        cur = self.bot.get_currency(ctx.guild.id)
        try:
            item = await create_item(self.pool, ctx.guild.id, name, price)
            await ctx.send(f"**{item['name']}** added to the shop for {price}{cur.emoji}. Use `.itemdesc {name} <description>` to add a description.")
        except Exception:
            await ctx.send("An item with that name already exists.")

    @commands.command()
    @commands.is_owner()
    async def addrole(self, ctx, price: str, role: discord.Role, *, name: str):
        """Add a role item to the shop. Usage: .addrole <price> @role <name>"""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        cur = self.bot.get_currency(ctx.guild.id)
        try:
            item = await create_item(self.pool, ctx.guild.id, name, price, item_type="role", role_given=role.id)
            await ctx.send(f"**{item['name']}** (grants {role.mention}) added to the shop for {price}{cur.emoji}.")
        except Exception:
            await ctx.send("An item with that name already exists.")

    @commands.command()
    @commands.is_owner()
    async def addtemprole(self, ctx, price: str, role: discord.Role, duration: str, *, name: str):
        """Add a temporary role item. Usage: .addtemprole <price> @role <duration> <name>"""
        try:
            price = parse_amount(price)
        except AmountError as e:
            await ctx.send(str(e))
            return
        delta = parse_duration(duration)
        if delta is None:
            await ctx.send("Invalid duration. Use a number followed by s/m/h/d (e.g. `7d`, `12h`).")
            return
        seconds = int(delta.total_seconds())
        if seconds < MIN_TEMP_ROLE_SECONDS:
            await ctx.send(f"Minimum duration is {humanize_duration(MIN_TEMP_ROLE_SECONDS)}.")
            return
        cur = self.bot.get_currency(ctx.guild.id)
        try:
            item = await create_item(
                self.pool, ctx.guild.id, name, price,
                item_type="role", role_given=role.id, role_duration=seconds,
            )
            await ctx.send(
                f"**{item['name']}** (grants {role.mention} for {humanize_duration(seconds)}) "
                f"added to the shop for {price}{cur.emoji}."
            )
        except Exception:
            await ctx.send("An item with that name already exists.")

    @commands.command()
    @commands.is_owner()
    async def itemdesc(self, ctx, name: str, *, description: str):
        """Set a description for a shop item. Usage: .itemdesc <name> <description>"""
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
        """Remove an item from the shop. Usage: .removeitem <name>"""
        deleted = await delete_item(self.pool, ctx.guild.id, name)
        if deleted:
            await ctx.send(f"**{deleted['name']}** has been removed from the shop.")
        else:
            await ctx.send("Item not found.")
