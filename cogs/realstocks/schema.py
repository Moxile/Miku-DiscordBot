SCHEMA = """
    CREATE TABLE IF NOT EXISTS real_symbols (
        symbol    TEXT PRIMARY KEY,
        name      TEXT NOT NULL,
        lot_size  INTEGER NOT NULL DEFAULT 1,
        added_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS guild_real_stocks (
        guild_id   BIGINT NOT NULL,
        symbol     TEXT NOT NULL REFERENCES real_symbols(symbol) ON DELETE CASCADE,
        enabled_by BIGINT NOT NULL,
        enabled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (guild_id, symbol)
    );

    CREATE TABLE IF NOT EXISTS real_holdings (
        guild_id BIGINT NOT NULL,
        user_id  BIGINT NOT NULL,
        symbol   TEXT NOT NULL,
        quantity INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id, symbol),
        FOREIGN KEY (guild_id, symbol) REFERENCES guild_real_stocks(guild_id, symbol) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS real_trades (
        id        BIGSERIAL PRIMARY KEY,
        guild_id  BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        symbol    TEXT NOT NULL,
        side      TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
        quantity  INTEGER NOT NULL,
        price     INTEGER NOT NULL,
        traded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (guild_id, symbol) REFERENCES guild_real_stocks(guild_id, symbol) ON DELETE CASCADE
    );

    CREATE TABLE IF NOT EXISTS real_price_history (
        id          BIGSERIAL PRIMARY KEY,
        symbol      TEXT NOT NULL REFERENCES real_symbols(symbol) ON DELETE CASCADE,
        price       INTEGER NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS real_price_history_symbol_time_idx
        ON real_price_history (symbol, recorded_at DESC);
"""

MIGRATIONS = [
    # Cached company fundamentals (Finnhub profile2/metric), refreshed periodically —
    # see RealStocks.refresh_profiles. NULL until first fetched.
    "ALTER TABLE real_symbols ADD COLUMN IF NOT EXISTS industry TEXT",
    "ALTER TABLE real_symbols ADD COLUMN IF NOT EXISTS domain TEXT",
    "ALTER TABLE real_symbols ADD COLUMN IF NOT EXISTS market_cap DOUBLE PRECISION",
    "ALTER TABLE real_symbols ADD COLUMN IF NOT EXISTS eps DOUBLE PRECISION",
    "ALTER TABLE real_symbols ADD COLUMN IF NOT EXISTS profile_updated_at TIMESTAMPTZ",
]

CONSTRAINTS = [
    "ALTER TABLE real_holdings ADD CONSTRAINT real_quantity_non_negative CHECK (quantity >= 0)",
]
