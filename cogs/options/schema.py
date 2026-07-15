SCHEMA = """
    CREATE TABLE IF NOT EXISTS option_positions (
        id           BIGSERIAL PRIMARY KEY,
        guild_id     BIGINT NOT NULL,
        user_id      BIGINT NOT NULL,
        symbol       TEXT NOT NULL,
        opt_type     TEXT NOT NULL CHECK (opt_type IN ('call', 'put')),
        strike       DOUBLE PRECISION NOT NULL,   -- USD
        expiry       TIMESTAMPTZ NOT NULL,
        contracts    INTEGER NOT NULL,
        multiplier   INTEGER NOT NULL,            -- shares per contract, frozen at purchase
        entry_spot   DOUBLE PRECISION NOT NULL,   -- underlying price at purchase (display)
        iv           DOUBLE PRECISION NOT NULL,   -- volatility used to price the premium
        premium_paid BIGINT NOT NULL,             -- total coins paid up front
        opened_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status       TEXT NOT NULL DEFAULT 'open'
                     CHECK (status IN ('open', 'exercised', 'expired', 'closed')),
        settle_spot  DOUBLE PRECISION,            -- underlying price at settlement/close
        payout       BIGINT,                      -- coins returned (NULL while open)
        realized_pl  BIGINT,                      -- payout - premium_paid
        closed_at    TIMESTAMPTZ,
        -- Cascades on delisting (mirrors real_holdings). Open options are force-settled
        -- first in RealStocks.removestock, so the cascade only removes settled rows.
        FOREIGN KEY (guild_id, symbol) REFERENCES guild_real_stocks(guild_id, symbol) ON DELETE CASCADE
    );

    -- The settlement loop scans open positions by expiry.
    CREATE INDEX IF NOT EXISTS option_positions_open_expiry_idx
        ON option_positions (expiry) WHERE status = 'open';

    CREATE INDEX IF NOT EXISTS option_positions_user_idx
        ON option_positions (guild_id, user_id);
"""
