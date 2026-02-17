import os
import aiosqlite
import discord
from discord.ext import commands
from dotenv import load_dotenv
from utils import is_guild_owner, VALID_CATEGORIES, PREFIX

load_dotenv()

DB_PATH = "data/economy.db"

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents, help_command=None)
bot._settings_db = None


@bot.command()
async def help(ctx: commands.Context, *, command_name: str = None):
    """Show all available commands, grouped by role."""
    p = ctx.prefix

    if command_name:
        cmd = bot.get_command(command_name)
        if cmd is None:
            await ctx.send(f"Unknown command `{p}{command_name}`.")
            return
        embed = discord.Embed(
            title=f"{p}{cmd.qualified_name}",
            description=cmd.help or "No description.",
            color=discord.Color.blurple(),
        )
        if cmd.aliases:
            embed.add_field(
                name="Aliases",
                value=", ".join(f"`{p}{a}`" for a in cmd.aliases),
                inline=False,
            )
        await ctx.send(embed=embed)
        return

    # ── Owner Commands (Server Owner only) ────────────────────────
    owner_embed = discord.Embed(
        title="Owner Commands",
        description="Only the **server owner** can use these.",
        color=discord.Color.red(),
    )
    owner_embed.add_field(
        name="Economy Settings",
        value=(
            f"`{p}setcooldown <hours>` — Set work cooldown\n"
            f"`{p}setworkpay <min> <max>` — Set work earnings range\n"
            f"`{p}add <@user> <amount>` — Give flowers to a user\n"
            f"`{p}take <@user> <amount>` — Take flowers from a user"
        ),
        inline=False,
    )
    owner_embed.add_field(
        name="Shop Management",
        value=(
            f"`{p}createitem \"Name\" <price>` — Create a shop item\n"
            f"`{p}deleteitem <Name>` — Remove a shop item\n"
            f"`{p}edititem <field> \"Name\" <value>` — Edit a shop item\n"
            "  Fields: name, description, price, type, role, rebuyable"
        ),
        inline=False,
    )
    owner_embed.add_field(
        name="Gambling Settings",
        value=(
            f"`{p}setminbet <amount>` — Set minimum bet\n"
            f"`{p}setmaxbet <amount>` — Set maximum bet"
        ),
        inline=False,
    )
    owner_embed.add_field(
        name="Market Management",
        value=(
            f"`{p}ipo #channel <price>` — Register a channel as a company\n"
            f"`{p}delist #channel` — Remove a company from the market\n"
            f"`{p}setdividend <percent>` — Set dividend payout %\n"
            f"`{p}companyinfo #channel` — View company diagnostics"
        ),
        inline=False,
    )
    owner_embed.add_field(
        name="Missions Management",
        value=(
            f"`{p}createmission \"Title\" <cost>` — Create a mission\n"
            f"`{p}deletemission <Title>` — Delete a mission\n"
            f"`{p}editmission <field> \"Title\" <value>` — Edit a mission\n"
            "  Fields: title, description, cost"
        ),
        inline=False,
    )
    owner_embed.add_field(
        name="Channel Restrictions",
        value=(
            f"`{p}setchannel <category> #channel` — Restrict commands to a channel\n"
            f"`{p}unsetchannel <category> #channel` — Remove a channel restriction\n"
            f"`{p}channels` — View all channel restrictions\n"
            f"`{p}unrestrict <command>` — Allow a command everywhere\n"
            f"`{p}rerestrict <command>` — Re-apply channel restrictions\n"
            f"  Categories: economy, gambling, shop, market, missions"
        ),
        inline=False,
    )

    # ── Admin Commands (Requires Permissions) ─────────────────────
    admin_embed = discord.Embed(
        title="Admin Commands",
        description="Requires **moderation permissions** (kick, ban, timeout).",
        color=discord.Color.orange(),
    )
    admin_embed.add_field(
        name="Moderation",
        value=(
            f"`{p}kick <@user> [reason]` — Kick a member\n"
            f"`{p}ban <@user> [reason]` — Ban a member\n"
            f"`{p}unban <user_id>` — Unban a user by ID\n"
            f"`{p}mute <@user> <duration> [reason]` — Timeout a member\n"
            f"`{p}unmute <@user>` — Remove timeout\n"
            "  Duration format: `10s`, `5m`, `1h`, `1d`"
        ),
        inline=False,
    )

    # ── User Commands (Everyone) ──────────────────────────────────
    user_embed = discord.Embed(
        title="User Commands",
        description="Available to **everyone**.",
        color=discord.Color.green(),
    )
    user_embed.add_field(
        name="Economy",
        value=(
            f"`{p}balance` / `{p}bal` — Check your balance\n"
            f"`{p}work` — Earn flowers (cooldown applies)\n"
            f"`{p}deposit <amount|all>` / `{p}dep` — Deposit flowers to bank\n"
            f"`{p}withdraw <amount|all>` / `{p}with` — Withdraw from bank\n"
            f"`{p}give <@user> <amount>` / `{p}pay` — Send flowers to someone"
        ),
        inline=False,
    )
    user_embed.add_field(
        name="Shop",
        value=(
            f"`{p}shop` — Browse the server shop\n"
            f"`{p}buy \"Name\"` — Buy an item\n"
            f"`{p}inventory` / `{p}inv` — View your inventory"
        ),
        inline=False,
    )
    user_embed.add_field(
        name="Gambling",
        value=(
            f"`{p}coinflip` / `{p}cf` — Flip a coin or bet on it\n"
            f"  `{p}cf` — flip for fun · `{p}cf h 100` — bet 100 🌸 on heads\n"
            f"`{p}blackjack <bet>` / `{p}bj` — Start a blackjack game\n"
            f"  `{p}hit` · `{p}stand` · `{p}double` · `{p}split`\n"
            f"`{p}russianroulette <bet>` / `{p}rr` — Start Russian Roulette\n"
            f"  `{p}rr join` — Join an active game\n"
            f"`{p}rbet <type> <amount>` — Place a roulette bet\n"
            f"  Types: number, red, black, odd, even, high, low, 1st/2nd/3rd, green\n"
            f"`{p}rclear` — Cancel your roulette bets"
        ),
        inline=False,
    )
    user_embed.add_field(
        name="Stock Market",
        value=(
            f"`{p}market` — List all companies\n"
            f"`{p}stockinfo #channel [7d/30d/all]` — View stock info & chart\n"
            f"`{p}mbuy #channel <shares>` — Market buy shares\n"
            f"`{p}msell #channel <shares>` — Market sell shares\n"
            f"`{p}limitbuy #channel <shares> <price>` — Limit buy order\n"
            f"`{p}limitsell #channel <shares> <price>` — Limit sell order\n"
            f"`{p}cancel <order_id>` — Cancel an open order\n"
            f"`{p}orderbook #channel` — View the order book\n"
            f"`{p}portfolio` — View your stock portfolio\n"
            f"`{p}myorders` — View your open orders"
        ),
        inline=False,
    )
    user_embed.add_field(
        name="Missions",
        value=(
            f"`{p}missions` — View active missions\n"
            f"`{p}completedmissions` — View completed missions\n"
            f"`{p}fund \"Title\" <amount|all>` — Contribute to a mission"
        ),
        inline=False,
    )
    user_embed.add_field(
        name="Other",
        value=f"`{p}ping` — Pong!\n`{p}help [command]` — Show this menu or details for a command",
        inline=False,
    )

    await ctx.send(embeds=[owner_embed, admin_embed, user_embed])


@bot.event
async def setup_hook():
    bot._settings_db = await aiosqlite.connect(DB_PATH)
    await bot._settings_db.execute("PRAGMA journal_mode=WAL")
    await bot._settings_db.execute("PRAGMA busy_timeout=5000")
    await bot._settings_db.execute(
        """CREATE TABLE IF NOT EXISTS allowed_channels (
            guild_id   INTEGER NOT NULL,
            category   TEXT NOT NULL,
            channel_id INTEGER NOT NULL,
            PRIMARY KEY (guild_id, category, channel_id)
        )"""
    )
    await bot._settings_db.execute(
        """CREATE TABLE IF NOT EXISTS unrestricted_commands (
            guild_id INTEGER NOT NULL,
            command  TEXT NOT NULL,
            PRIMARY KEY (guild_id, command)
        )"""
    )
    await bot._settings_db.commit()

    await bot.load_extension("cogs.moderation")
    await bot.load_extension("cogs.economy")
    await bot.load_extension("cogs.shop")
    await bot.load_extension("cogs.gambling")
    await bot.load_extension("cogs.market")
    await bot.load_extension("cogs.missions")


@bot.event
async def on_ready():
    print(f"Logged in as {bot.user}")


@bot.command()
async def ping(ctx):
    await ctx.send("Pong!")


@bot.command()
@is_guild_owner()
async def setchannel(ctx: commands.Context, category: str, channel: discord.TextChannel):
    """Restrict a command category to a channel. Usage: {prefix}setchannel economy #bot-cmds"""
    category = category.lower()
    if category not in VALID_CATEGORIES:
        await ctx.send(f"Invalid category. Valid: {', '.join(sorted(VALID_CATEGORIES))}")
        return
    await bot._settings_db.execute(
        "INSERT OR IGNORE INTO allowed_channels (guild_id, category, channel_id) "
        "VALUES (?, ?, ?)",
        (ctx.guild.id, category, channel.id),
    )
    await bot._settings_db.commit()
    await ctx.send(f"**{category}** commands are now allowed in {channel.mention}.")


@bot.command()
@is_guild_owner()
async def unsetchannel(ctx: commands.Context, category: str, channel: discord.TextChannel):
    """Remove a channel restriction. Usage: {prefix}unsetchannel economy #bot-cmds"""
    category = category.lower()
    if category not in VALID_CATEGORIES:
        await ctx.send(f"Invalid category. Valid: {', '.join(sorted(VALID_CATEGORIES))}")
        return
    await bot._settings_db.execute(
        "DELETE FROM allowed_channels WHERE guild_id = ? AND category = ? AND channel_id = ?",
        (ctx.guild.id, category, channel.id),
    )
    await bot._settings_db.commit()
    await ctx.send(f"Removed {channel.mention} restriction for **{category}**.")


@bot.command()
@is_guild_owner()
async def channels(ctx: commands.Context):
    """View all channel restrictions."""
    async with bot._settings_db.execute(
        "SELECT category, channel_id FROM allowed_channels "
        "WHERE guild_id = ? ORDER BY category",
        (ctx.guild.id,),
    ) as cur:
        rows = await cur.fetchall()

    if not rows:
        await ctx.send("No channel restrictions configured. Commands work everywhere.")
        return

    by_category = {}
    for category, channel_id in rows:
        by_category.setdefault(category, []).append(channel_id)

    embed = discord.Embed(title="Channel Restrictions", color=discord.Color.blurple())
    for category in sorted(by_category):
        mentions = [f"<#{cid}>" for cid in by_category[category]]
        embed.add_field(name=category.capitalize(), value=", ".join(mentions), inline=False)

    async with bot._settings_db.execute(
        "SELECT command FROM unrestricted_commands WHERE guild_id = ? ORDER BY command",
        (ctx.guild.id,),
    ) as cur:
        cmd_rows = await cur.fetchall()
    if cmd_rows:
        cmds = ", ".join(f"`{ctx.prefix}{r[0]}`" for r in cmd_rows)
        embed.add_field(name="Unrestricted Commands", value=cmds, inline=False)

    await ctx.send(embed=embed)


@bot.command()
@is_guild_owner()
async def unrestrict(ctx: commands.Context, command_name: str):
    """Allow a command everywhere, bypassing category restrictions.
    Usage: {prefix}unrestrict rr"""
    command_name = command_name.lower().lstrip(PREFIX)
    cmd = bot.get_command(command_name)
    if not cmd:
        await ctx.send(f"Unknown command `{ctx.prefix}{command_name}`.")
        return
    # Always store the primary command name so it matches ctx.command.name in cog checks
    command_name = cmd.name
    await bot._settings_db.execute(
        "INSERT OR IGNORE INTO unrestricted_commands (guild_id, command) VALUES (?, ?)",
        (ctx.guild.id, command_name),
    )
    await bot._settings_db.commit()
    await ctx.send(f"`{ctx.prefix}{command_name}` is now allowed everywhere.")


@bot.command()
@is_guild_owner()
async def rerestrict(ctx: commands.Context, command_name: str):
    """Re-restrict a command to its category channels.
    Usage: {prefix}rerestrict rr"""
    command_name = command_name.lower().lstrip(PREFIX)
    cmd = bot.get_command(command_name)
    if cmd:
        command_name = cmd.name
    await bot._settings_db.execute(
        "DELETE FROM unrestricted_commands WHERE guild_id = ? AND command = ?",
        (ctx.guild.id, command_name),
    )
    await bot._settings_db.commit()
    await ctx.send(f"`{ctx.prefix}{command_name}` now follows its category channel restrictions again.")


bot.run(os.getenv("DISCORD_TOKEN"))
