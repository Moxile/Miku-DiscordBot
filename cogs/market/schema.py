from config import REVENUE_BASE_MULTIPLIER


SCHEMA = """
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
"""

MIGRATIONS = [
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS treasury BIGINT NOT NULL DEFAULT 0",
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_level INTEGER NOT NULL DEFAULT 0",
    f"ALTER TABLE companies ADD COLUMN IF NOT EXISTS revenue_multiplier INTEGER NOT NULL DEFAULT {REVENUE_BASE_MULTIPLIER}",
    # Migrate all companies to the current multiplier formula: BASE * 2^level
    f"UPDATE companies SET revenue_multiplier = ({REVENUE_BASE_MULTIPLIER} * POWER(2, company_level))::INTEGER",
    # Price floor used by the dilution system — set once at IPO, never changed
    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS base_ipo_price INTEGER NOT NULL DEFAULT 100",
    "UPDATE companies SET base_ipo_price = ipo_price WHERE base_ipo_price = 100",
]

CONSTRAINTS = [
    "ALTER TABLE portfolios ADD CONSTRAINT quantity_non_negative CHECK (quantity >= 0)",
    "ALTER TABLE companies ADD CONSTRAINT ipo_shares_non_negative CHECK (available_ipo_shares >= 0)",
    "ALTER TABLE companies ADD CONSTRAINT treasury_non_negative CHECK (treasury >= 0)",
    "ALTER TABLE orders ADD CONSTRAINT remaining_non_negative CHECK (remaining >= 0)",
]
