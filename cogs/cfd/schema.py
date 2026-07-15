SCHEMA = """
    CREATE TABLE IF NOT EXISTS cfd_positions (
        id                BIGSERIAL PRIMARY KEY,
        guild_id          BIGINT NOT NULL,
        user_id           BIGINT NOT NULL,
        symbol            TEXT NOT NULL,
        direction         TEXT NOT NULL CHECK (direction IN ('long', 'short')),
        notional          BIGINT NOT NULL,
        leverage          INTEGER NOT NULL,
        entry_price       DOUBLE PRECISION NOT NULL,   -- USD price at open
        margin            BIGINT NOT NULL,             -- coins locked = notional / leverage
        liquidation_price DOUBLE PRECISION NOT NULL,   -- fee-free liquidation level (for display)
        financing_accrued BIGINT NOT NULL DEFAULT 0,   -- overnight fees owed so far, coins
        last_financed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        opened_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status            TEXT NOT NULL DEFAULT 'open'
                          CHECK (status IN ('open', 'closed', 'liquidated')),
        close_price       DOUBLE PRECISION,            -- USD price at close (NULL while open)
        realized_pl       BIGINT,                      -- payout - margin (net profit, incl. financing)
        closed_at         TIMESTAMPTZ,
        -- FK to the per-guild enablement row so delisting a stock cascades here too
        -- (mirrors real_holdings / real_trades). Open positions are force-closed first
        -- in RealStocks.removestock, so the cascade only ever removes settled rows.
        FOREIGN KEY (guild_id, symbol) REFERENCES guild_real_stocks(guild_id, symbol) ON DELETE CASCADE
    );

    -- The background loop's working set: every open position, grouped by symbol.
    CREATE INDEX IF NOT EXISTS cfd_positions_open_idx
        ON cfd_positions (symbol) WHERE status = 'open';

    CREATE INDEX IF NOT EXISTS cfd_positions_user_idx
        ON cfd_positions (guild_id, user_id);
"""
