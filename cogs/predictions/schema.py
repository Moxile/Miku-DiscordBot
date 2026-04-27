SCHEMA = """
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
"""
