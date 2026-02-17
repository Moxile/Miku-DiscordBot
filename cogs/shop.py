import discord
import aiosqlite
from discord.ext import commands
from utils import is_guild_owner, check_channel_allowed

DB_PATH = "data/economy.db"


class Shop(commands.Cog):
    _owner_commands = {"createitem", "deleteitem", "edititem"}

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db: aiosqlite.Connection = None

    async def cog_check(self, ctx: commands.Context) -> bool:
        if ctx.command.name in self._owner_commands:
            return True
        return await check_channel_allowed(
            self.db, ctx.guild.id, "shop", ctx.channel.id, ctx.command.name
        )

    async def cog_load(self):
        self.db = await aiosqlite.connect(DB_PATH)
        await self.db.execute("PRAGMA journal_mode=WAL")
        await self.db.execute("PRAGMA busy_timeout=5000")
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS shop_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                price INTEGER NOT NULL,
                type TEXT NOT NULL,
                role_id INTEGER,
                rebuyable INTEGER DEFAULT 1
            )"""
        )
        await self.db.execute(
            """CREATE TABLE IF NOT EXISTS inventory (
                user_id INTEGER NOT NULL,
                guild_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, guild_id, item_id)
            )"""
        )
        await self.db.commit()

    async def cog_unload(self):
        if self.db:
            await self.db.close()

    # --- Create Item (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def createitem(self, ctx: commands.Context, name: str, price: int):
        """Create a shop item. Usage: {prefix}createitem "Item Name" 500. Server owner only."""
        if price < 0:
            await ctx.send("Price must be positive.")
            return

        async with self.db.execute(
            "SELECT id FROM shop_items WHERE guild_id = ? AND LOWER(name) = LOWER(?)",
            (ctx.guild.id, name),
        ) as cursor:
            existing = await cursor.fetchone()

        if existing:
            await ctx.send(f"An item called **{name}** already exists.")
            return

        async with self.db.execute(
            """INSERT INTO shop_items (guild_id, name, description, price, type, role_id, rebuyable)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (ctx.guild.id, name, "No description.", price, "inventory", None, 1),
        ) as cursor:
            item_id = cursor.lastrowid
        await self.db.commit()

        embed = discord.Embed(
            title="Item Created",
            description=f"**{name}** has been added to the shop for **${price:,}**.",
            color=discord.Color.green(),
        )
        embed.add_field(name="ID", value=str(item_id))
        embed.set_footer(text=f"Use {ctx.prefix}edititem to set description, type, role, rebuyable")
        await ctx.send(embed=embed)

    # --- Delete Item (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def deleteitem(self, ctx: commands.Context, *, name: str):
        """Remove an item from the shop. Usage: {prefix}deleteitem "Item Name". Server owner only."""
        async with self.db.execute(
            "SELECT id, name FROM shop_items WHERE guild_id = ? AND LOWER(name) = LOWER(?)",
            (ctx.guild.id, name),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            await ctx.send(f"No item called **{name}** found.")
            return

        item_id = row[0]
        await self.db.execute("DELETE FROM shop_items WHERE id = ?", (item_id,))
        await self.db.execute(
            "DELETE FROM inventory WHERE item_id = ? AND guild_id = ?",
            (item_id, ctx.guild.id),
        )
        await self.db.commit()

        embed = discord.Embed(
            title="Item Deleted",
            description=f"**{row[1]}** has been removed from the shop.",
            color=discord.Color.red(),
        )
        await ctx.send(embed=embed)

    # --- Edit Item (Owner only) ---

    @commands.command()
    @is_guild_owner()
    async def edititem(self, ctx: commands.Context, field: str, name: str, *, value: str):
        """Edit a shop item. Usage: {prefix}edititem <field> "Item Name" <value>. Server owner only.
        Fields: name, description, price, type, role, rebuyable."""
        allowed_fields = {"name", "description", "price", "type", "role", "rebuyable"}
        field = field.lower()
        if field not in allowed_fields:
            await ctx.send(f"Invalid field. Choose from: {', '.join(allowed_fields)}")
            return

        async with self.db.execute(
            "SELECT id FROM shop_items WHERE guild_id = ? AND LOWER(name) = LOWER(?)",
            (ctx.guild.id, name),
        ) as cursor:
            row = await cursor.fetchone()

        if not row:
            await ctx.send(f"No item called **{name}** found.")
            return

        item_id = row[0]
        db_field = field

        if field == "price":
            try:
                value = int(value)
                if value < 0:
                    await ctx.send("Price must be positive.")
                    return
            except ValueError:
                await ctx.send("Invalid number.")
                return
        elif field == "type":
            value = value.lower()
            if value not in ("role", "inventory"):
                await ctx.send("Type must be `role` or `inventory`.")
                return
        elif field == "role":
            db_field = "role_id"
            value = value.strip("<@&>")
            try:
                role_id = int(value)
            except ValueError:
                await ctx.send("Invalid role. Mention the role or paste the role ID.")
                return
            role = ctx.guild.get_role(role_id)
            if role is None:
                await ctx.send("Could not find that role.")
                return
            value = role_id
        elif field == "rebuyable":
            if value.lower() in ("yes", "1", "true"):
                value = 1
            elif value.lower() in ("no", "0", "false"):
                value = 0
            else:
                await ctx.send("Rebuyable must be `yes` or `no`.")
                return

        await self.db.execute(
            f"UPDATE shop_items SET {db_field} = ? WHERE id = ?",
            (value, item_id),
        )
        await self.db.commit()

        display_value = value
        if field == "role":
            display_value = f"<@&{value}>"
        elif field == "rebuyable":
            display_value = "Yes" if value else "No"

        embed = discord.Embed(
            title="Item Updated",
            description=f"**{name}** — `{field}` set to **{display_value}**.",
            color=discord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    # --- Shop ---

    @commands.command()
    async def shop(self, ctx: commands.Context):
        """Browse the server's shop."""
        async with self.db.execute(
            "SELECT id, name, description, price, type, role_id, rebuyable FROM shop_items WHERE guild_id = ?",
            (ctx.guild.id,),
        ) as cursor:
            items = await cursor.fetchall()

        if not items:
            await ctx.send("The shop is empty.")
            return

        embed = discord.Embed(
            title=f"{ctx.guild.name}'s Shop",
            color=discord.Color.gold(),
        )

        for item_id, name, description, price, item_type, role_id, rebuyable in items:
            details = f"{description}\n**Price:** ${price:,} | **Type:** {item_type}"
            if item_type == "role" and role_id:
                details += f" | **Role:** <@&{role_id}>"
            if not rebuyable:
                details += " | *One-time purchase*"
            embed.add_field(
                name=f"#{item_id} — {name}",
                value=details,
                inline=False,
            )

        await ctx.send(embed=embed)

    # --- Buy ---

    @commands.command()
    async def buy(self, ctx: commands.Context, item_id: int):
        """Buy an item from the shop."""
        # Fetch item
        async with self.db.execute(
            "SELECT name, price, type, role_id, rebuyable FROM shop_items WHERE id = ? AND guild_id = ?",
            (item_id, ctx.guild.id),
        ) as cursor:
            item = await cursor.fetchone()

        if not item:
            await ctx.send("Item not found.")
            return

        name, price, item_type, role_id, rebuyable = item

        # Check rebuyable
        if not rebuyable:
            if item_type == "role" and role_id:
                role = ctx.guild.get_role(role_id)
                if role and role in ctx.author.roles:
                    await ctx.send("You already have this role.")
                    return
            elif item_type == "inventory":
                async with self.db.execute(
                    "SELECT quantity FROM inventory WHERE user_id = ? AND guild_id = ? AND item_id = ?",
                    (ctx.author.id, ctx.guild.id, item_id),
                ) as cursor:
                    inv_row = await cursor.fetchone()
                if inv_row:
                    await ctx.send("You already own this item.")
                    return

        # Check cash (read from economy table in same DB)
        async with self.db.execute(
            "SELECT cash FROM economy WHERE user_id = ?", (ctx.author.id,)
        ) as cursor:
            row = await cursor.fetchone()

        cash = row[0] if row else 0
        if cash < price:
            await ctx.send(f"You don't have enough cash. You need **${price:,}** but only have **${cash:,}**.")
            return

        # Deduct cash
        await self.db.execute(
            "UPDATE economy SET cash = cash - ? WHERE user_id = ?",
            (price, ctx.author.id),
        )

        # Apply item
        if item_type == "role" and role_id:
            role = ctx.guild.get_role(role_id)
            if role is None:
                await ctx.send("The role for this item no longer exists. Purchase cancelled.")
                await self.db.rollback()
                return
            if role >= ctx.guild.me.top_role:
                await ctx.send(
                    f"I can't assign **{role.name}** because it's above my highest role. "
                    "Ask an admin to move my role higher in the role list."
                )
                await self.db.rollback()
                return
            try:
                await ctx.author.add_roles(role, reason=f"Purchased shop item: {name}")
            except discord.Forbidden:
                await ctx.send("I don't have permission to assign that role. Purchase cancelled.")
                await self.db.rollback()
                return
        elif item_type == "inventory":
            await self.db.execute(
                """INSERT INTO inventory (user_id, guild_id, item_id, quantity)
                   VALUES (?, ?, ?, 1)
                   ON CONFLICT(user_id, guild_id, item_id)
                   DO UPDATE SET quantity = quantity + 1""",
                (ctx.author.id, ctx.guild.id, item_id),
            )

        await self.db.commit()

        embed = discord.Embed(
            title="Purchase Successful",
            description=f"You bought **{name}** for **${price:,}**!",
            color=discord.Color.green(),
        )
        await ctx.send(embed=embed)

    # --- Inventory ---

    @commands.command(aliases=["inv"])
    async def inventory(self, ctx: commands.Context):
        """View your inventory."""
        async with self.db.execute(
            """SELECT s.name, i.quantity
               FROM inventory i
               JOIN shop_items s ON i.item_id = s.id
               WHERE i.user_id = ? AND i.guild_id = ?""",
            (ctx.author.id, ctx.guild.id),
        ) as cursor:
            items = await cursor.fetchall()

        if not items:
            await ctx.send("Your inventory is empty.")
            return

        embed = discord.Embed(
            title=f"{ctx.author.display_name}'s Inventory",
            color=discord.Color.blue(),
        )

        for name, quantity in items:
            embed.add_field(name=name, value=f"x{quantity}", inline=True)

        await ctx.send(embed=embed)

    # --- Error Handler ---

    @commands.Cog.listener()
    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        if ctx.command is None or ctx.command.cog_name != self.__cog_name__:
            return

        if isinstance(error, commands.CheckFailure):
            if str(error) == "channel_restricted":
                return
            await ctx.send("Only the server owner can use this command.")
        elif isinstance(error, commands.BadArgument):
            await ctx.send(f"Invalid argument. Check `{ctx.prefix}help {ctx.command}` for usage.")
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(Shop(bot))
