SCHEMA = """
    -- Periodic net-worth snapshots for the profile menu's "Net Worth" graph — populated by
    -- the Profile cog's record_net_worth_snapshots background job. Unlike wallet+bank (which
    -- is reconstructed from the transaction log) there's no historical record of stock/waifu
    -- holdings value, so that graph can only show history from whenever this was deployed.
    -- ON DELETE CASCADE mirrors economy.transactions: deleting a member's balance row
    -- (on_member_remove) cleans up their snapshots automatically.
    CREATE TABLE IF NOT EXISTS net_worth_snapshots (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        net_worth   BIGINT NOT NULL,
        recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        FOREIGN KEY (guild_id, user_id) REFERENCES balances(guild_id, user_id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS net_worth_snapshots_user_idx
        ON net_worth_snapshots (guild_id, user_id, recorded_at);
"""
