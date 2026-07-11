SCHEMA = """
    -- The bookmaker-bet tables were originally named offers/offer_options/offer_takes.
    -- Rename them in place (renames preserve all data) before creating the current
    -- shape. Guarded so it fires only when the legacy tables still exist and the new
    -- ones don't: a no-op on fresh installs and on already-migrated databases.
    DO $$
    BEGIN
        IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'offers')
           AND NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bets') THEN
            ALTER TABLE offers RENAME TO bets;
        END IF;
        IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'offer_options')
           AND NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bet_options') THEN
            ALTER TABLE offer_options RENAME TO bet_options;
            ALTER TABLE bet_options RENAME COLUMN offer_id TO bet_id;
        END IF;
        IF EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'offer_takes')
           AND NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'bet_takes') THEN
            ALTER TABLE offer_takes RENAME TO bet_takes;
            ALTER TABLE bet_takes RENAME COLUMN offer_id TO bet_id;
        END IF;
    END $$;

    CREATE TABLE IF NOT EXISTS bets (
        id              BIGSERIAL PRIMARY KEY,
        guild_id        BIGINT NOT NULL,
        channel_id      BIGINT NOT NULL,
        host_id         BIGINT NOT NULL,
        description     TEXT NOT NULL DEFAULT '',
        odds            NUMERIC(10, 4) CHECK (odds IS NULL OR odds > 1),
        min_stake       BIGINT NOT NULL CHECK (min_stake > 0),
        max_stake       BIGINT NOT NULL CHECK (max_stake >= min_stake),
        pool            BIGINT NOT NULL CHECK (pool > 0),
        pool_remaining  BIGINT NOT NULL CHECK (pool_remaining >= 0),
        is_multi        BOOLEAN NOT NULL DEFAULT FALSE,
        status          TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'won', 'lost', 'resolved', 'cancelled')),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        closed_at       TIMESTAMPTZ
    );

    -- Options for a multi-option (is_multi=TRUE) bet, e.g. one entrant per
    -- tournament participant, each with its own odds. idx is the 1-based
    -- number players/hosts reference when placing/resolving.
    CREATE TABLE IF NOT EXISTS bet_options (
        id       BIGSERIAL PRIMARY KEY,
        bet_id   BIGINT NOT NULL REFERENCES bets(id) ON DELETE CASCADE,
        idx      INTEGER NOT NULL,
        label    TEXT NOT NULL,
        odds     NUMERIC(10, 4) NOT NULL CHECK (odds > 1),
        UNIQUE (bet_id, idx)
    );

    CREATE TABLE IF NOT EXISTS bet_takes (
        id         BIGSERIAL PRIMARY KEY,
        bet_id     BIGINT NOT NULL REFERENCES bets(id) ON DELETE CASCADE,
        option_id  BIGINT REFERENCES bet_options(id) ON DELETE CASCADE,
        user_id    BIGINT NOT NULL,
        stake      BIGINT NOT NULL CHECK (stake > 0),
        liability  BIGINT NOT NULL CHECK (liability >= 0),
        placed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
"""

# Kept idempotent so a database migrated from an older `offers` shape still gains
# any columns added after its snapshot. (ADD COLUMN IF NOT EXISTS is a no-op once
# the column is present.)
MIGRATIONS = [
    "ALTER TABLE bets ADD COLUMN IF NOT EXISTS is_multi BOOLEAN NOT NULL DEFAULT FALSE",
    "ALTER TABLE bet_takes ADD COLUMN IF NOT EXISTS option_id BIGINT "
    "REFERENCES bet_options(id) ON DELETE CASCADE",
]
