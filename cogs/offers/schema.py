SCHEMA = """
    CREATE TABLE IF NOT EXISTS offers (
        id              BIGSERIAL PRIMARY KEY,
        guild_id        BIGINT NOT NULL,
        channel_id      BIGINT NOT NULL,
        host_id         BIGINT NOT NULL,
        description     TEXT NOT NULL DEFAULT '',
        odds            NUMERIC(10, 4) NOT NULL CHECK (odds > 1),
        min_stake       BIGINT NOT NULL CHECK (min_stake > 0),
        max_stake       BIGINT NOT NULL CHECK (max_stake >= min_stake),
        pool            BIGINT NOT NULL CHECK (pool > 0),
        pool_remaining  BIGINT NOT NULL CHECK (pool_remaining >= 0),
        status          TEXT NOT NULL DEFAULT 'open'
                        CHECK (status IN ('open', 'won', 'lost', 'cancelled')),
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        closed_at       TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS offer_takes (
        id         BIGSERIAL PRIMARY KEY,
        offer_id   BIGINT NOT NULL REFERENCES offers(id) ON DELETE CASCADE,
        user_id    BIGINT NOT NULL,
        stake      BIGINT NOT NULL CHECK (stake > 0),
        liability  BIGINT NOT NULL CHECK (liability >= 0),
        placed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
"""
