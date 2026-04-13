import os
import asyncpg
import discord
from discord.ext import commands
from dotenv import load_dotenv

from config import PREFIX

load_dotenv()

async def init_db(pool: asyncpg.Pool):
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS balances (
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            wallet     BIGINT NOT NULL DEFAULT 0,
            bank        BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id          BIGSERIAL PRIMARY KEY,
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            amount      BIGINT NOT NULL,
            tx_type     TEXT NOT NULL,
            description TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            FOREIGN KEY (guild_id, user_id) REFERENCES balances(guild_id, user_id)
        );

        CREATE TABLE IF NOT EXISTS items (
            id           SERIAL PRIMARY KEY,
            guild_id     BIGINT NOT NULL,
            name         TEXT NOT NULL,
            description  TEXT,
            price        BIGINT NOT NULL,
            sell_price   BIGINT NOT NULL DEFAULT 0,
            item_type    TEXT NOT NULL DEFAULT 'item',
            metadata     JSONB DEFAULT '{}',
            is_available BOOLEAN NOT NULL DEFAULT TRUE,
            role_given   BIGINT,
            UNIQUE (guild_id, name)
        );

        CREATE TABLE IF NOT EXISTS inventory (
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            item_id     INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
            quantity    INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (guild_id, user_id, item_id)
        );
                       
        CREATE TABLE IF NOT EXISTS cooldowns (
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            command     TEXT NOT NULL,
            expires_at  TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (guild_id, user_id, command)
        );
                       
        CREATE TABLE IF NOT EXISTS companies (
            guild_id             BIGINT NOT NULL,
            stock_channel_id     BIGINT NOT NULL,
            name                 TEXT NOT NULL,
            total_shares         INTEGER NOT NULL DEFAULT 100,
            available_ipo_shares INTEGER NOT NULL DEFAULT 100,
            ipo_price            INTEGER NOT NULL DEFAULT 100,
            listed_by            BIGINT NOT NULL,
            listed_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (guild_id, stock_channel_id),
            UNIQUE (guild_id, name)
        );

        CREATE TABLE IF NOT EXISTS portfolios (
            guild_id         BIGINT NOT NULL,
            user_id          BIGINT NOT NULL,
            stock_channel_id BIGINT NOT NULL,
            quantity         INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, user_id, stock_channel_id),
            FOREIGN KEY (guild_id, stock_channel_id) REFERENCES companies(guild_id, stock_channel_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS orders (
            id               BIGSERIAL PRIMARY KEY,
            guild_id         BIGINT NOT NULL,
            stock_channel_id BIGINT NOT NULL,
            user_id          BIGINT NOT NULL,
            side             TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
            quantity         INTEGER NOT NULL,
            remaining        INTEGER NOT NULL,
            price            INTEGER NOT NULL,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            FOREIGN KEY (guild_id, stock_channel_id) REFERENCES companies(guild_id, stock_channel_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS trade_history (
            id               BIGSERIAL PRIMARY KEY,
            guild_id         BIGINT NOT NULL,
            stock_channel_id BIGINT NOT NULL,
            buyer_id         BIGINT NOT NULL,
            seller_id        BIGINT,
            quantity         INTEGER NOT NULL,
            price            INTEGER NOT NULL,
            trade_type       TEXT NOT NULL DEFAULT 'market',
            traded_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            FOREIGN KEY (guild_id, stock_channel_id) REFERENCES companies(guild_id, stock_channel_id) ON DELETE CASCADE
        );

    """)

    # Revenue system tables and columns (idempotent migrations)
    await pool.execute("""
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS treasury BIGINT NOT NULL DEFAULT 0;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_level INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE companies ADD COLUMN IF NOT EXISTS revenue_multiplier INTEGER NOT NULL DEFAULT 10;

        CREATE TABLE IF NOT EXISTS channel_activity (
            guild_id         BIGINT NOT NULL,
            stock_channel_id BIGINT NOT NULL,
            user_id          BIGINT NOT NULL,
            activity_date    DATE NOT NULL,
            char_count       BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, stock_channel_id, user_id, activity_date),
            FOREIGN KEY (guild_id, stock_channel_id) REFERENCES companies(guild_id, stock_channel_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS company_revenue (
            guild_id         BIGINT NOT NULL,
            stock_channel_id BIGINT NOT NULL,
            revenue_date     DATE NOT NULL,
            revenue          BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (guild_id, stock_channel_id, revenue_date),
            FOREIGN KEY (guild_id, stock_channel_id) REFERENCES companies(guild_id, stock_channel_id) ON DELETE CASCADE
        );
    """)

    # Reminders system
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS reminders (
            id          BIGSERIAL PRIMARY KEY,
            guild_id    BIGINT NOT NULL,
            user_id     BIGINT NOT NULL,
            channel_id  BIGINT NOT NULL,
            message     TEXT,
            remind_at   TIMESTAMPTZ NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    # Waifu system
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS waifus (
            guild_id       BIGINT NOT NULL,
            user_id        BIGINT NOT NULL,
            owner_id       BIGINT,
            value          BIGINT NOT NULL DEFAULT 5000,
            last_bought_at TIMESTAMPTZ,
            spouse_id      BIGINT,
            engaged_since  TIMESTAMPTZ,
            PRIMARY KEY (guild_id, user_id)
        );
    """)

    # Predictions system
    await pool.execute("""
        CREATE TABLE IF NOT EXISTS predictions (
            id           BIGSERIAL PRIMARY KEY,
            guild_id     BIGINT NOT NULL,
            creator_id   BIGINT NOT NULL,
            question     TEXT NOT NULL,
            status       TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed', 'resolved')),
            winner_option_id BIGINT,
            created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS prediction_options (
            id            BIGSERIAL PRIMARY KEY,
            prediction_id BIGINT NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
            label         TEXT NOT NULL,
            option_index  INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS prediction_bets (
            id            BIGSERIAL PRIMARY KEY,
            prediction_id BIGINT NOT NULL REFERENCES predictions(id) ON DELETE CASCADE,
            option_id     BIGINT NOT NULL REFERENCES prediction_options(id) ON DELETE CASCADE,
            guild_id      BIGINT NOT NULL,
            user_id       BIGINT NOT NULL,
            amount        BIGINT NOT NULL CHECK (amount > 0),
            placed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id BIGINT NOT NULL,
            key      TEXT NOT NULL,
            value    TEXT NOT NULL,
            PRIMARY KEY (guild_id, key)
        );
    """)

    # Safety constraints — prevent negative balances/holdings as a last line of defense.
    # These are idempotent: if the constraint already exists, DO NOTHING catches the error.
    for stmt in [
        "ALTER TABLE balances ADD CONSTRAINT wallet_non_negative CHECK (wallet >= 0)",
        "ALTER TABLE balances ADD CONSTRAINT bank_non_negative CHECK (bank >= 0)",
        "ALTER TABLE portfolios ADD CONSTRAINT quantity_non_negative CHECK (quantity >= 0)",
        "ALTER TABLE companies ADD CONSTRAINT ipo_shares_non_negative CHECK (available_ipo_shares >= 0)",
        "ALTER TABLE companies ADD CONSTRAINT treasury_non_negative CHECK (treasury >= 0)",
        "ALTER TABLE orders ADD CONSTRAINT remaining_non_negative CHECK (remaining >= 0)",
    ]:
        try:
            await pool.execute(stmt)
        except asyncpg.DuplicateObjectError:
            pass


COG_COLORS = {
    "Economy": discord.Color.green(),
    "Gambling": discord.Color.gold(),
    "Market": discord.Color.blue(),
    "Moderation": discord.Color.red(),
    "Shop": discord.Color.purple(),
    "Predictions": discord.Color.teal(),
    "Reminders": discord.Color.from_rgb(255, 182, 193),
    "Waifu": discord.Color.from_rgb(255, 105, 180),
    "Leaderboard": discord.Color.from_rgb(255, 215, 0),
}


class Help(commands.HelpCommand):
    async def command_callback(self, ctx, /, *, command=None):
        """Override to support case-insensitive cog lookup."""
        if command:
            bot = ctx.bot
            # Try case-insensitive cog match
            for name, cog in bot.cogs.items():
                if name.lower() == command.lower():
                    return await self.send_cog_help(cog)
        return await super().command_callback(ctx, command=command)

    async def send_bot_help(self, mapping):
        ctx = self.context
        embed = discord.Embed(title="Help", description="Use `.help <category>` or `.help <command>` for details.", color=discord.Color.blurple())
        for cog, cmds in mapping.items():
            filtered = await self.filter_commands(cmds, sort=True)
            if not filtered:
                continue
            name = cog.qualified_name if cog else "Other"
            value = ", ".join(f"`{c.name}`" for c in filtered)
            embed.add_field(name=name, value=value, inline=False)
        try:
            await ctx.author.send(embed=embed)
            await ctx.message.add_reaction("\u2705")
        except discord.Forbidden:
            await ctx.send(embed=embed)

    async def send_cog_help(self, cog):
        color = COG_COLORS.get(cog.qualified_name, discord.Color.greyple())
        embed = discord.Embed(title=f"{cog.qualified_name} Commands", color=color)
        filtered = await self.filter_commands(cog.get_commands(), sort=True)
        for cmd in filtered:
            label = cmd.name
            if cmd.aliases:
                label += " (" + ", ".join(cmd.aliases) + ")"
            for check in cmd.checks:
                if "is_owner" in str(check):
                    label += " [Owner]"
                    break
                if "has_permissions" in str(check):
                    label += " [Admin]"
                    break
            embed.add_field(
                name=label,
                value=cmd.short_doc or "No description",
                inline=False,
            )
        embed.set_footer(text=f"Use .help <command> for more info on a command.")
        await self.get_destination().send(embed=embed)

    async def send_command_help(self, command):
        cog = command.cog
        color = COG_COLORS.get(cog.qualified_name, discord.Color.greyple()) if cog else discord.Color.greyple()
        embed = discord.Embed(title=f".{command.name}", description=command.help or "No description", color=color)
        embed.add_field(name="Usage", value=f"`{self.get_command_signature(command)}`", inline=False)
        if command.aliases:
            embed.add_field(name="Aliases", value=", ".join(f"`{a}`" for a in command.aliases), inline=False)
        if cog:
            siblings = [c.name for c in cog.get_commands() if c != command and not c.hidden]
            if siblings:
                embed.set_footer(text=f"Related: {', '.join(siblings)}")
        await self.get_destination().send(embed=embed)

    async def send_error_message(self, error):
        embed = discord.Embed(title="Not Found", description=error, color=discord.Color.red())
        await self.get_destination().send(embed=embed)


class MikuBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix=PREFIX, intents=intents)
        self.pool: asyncpg.Pool | None = None

    async def setup_hook(self):
        self.pool = await asyncpg.create_pool(
            dsn=os.getenv("DATABASE_URL"),
            min_size=2,
            max_size=10,
        )
        await init_db(self.pool)
        await self.load_extension("cogs.moderation")
        await self.load_extension("cogs.economy")
        await self.load_extension("cogs.gambling")
        await self.load_extension("cogs.market")
        await self.load_extension("cogs.shop")
        await self.load_extension("cogs.predictions")
        await self.load_extension("cogs.reminders")
        await self.load_extension("cogs.waifu")
        await self.load_extension("cogs.leaderboard")

    async def close(self):
        if self.pool:
            await self.pool.close()
        await super().close()

bot = MikuBot()
bot.help_command = Help()
bot.run(os.getenv("DISCORD_TOKEN"))
