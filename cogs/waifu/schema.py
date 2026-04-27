SCHEMA = """
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
"""
