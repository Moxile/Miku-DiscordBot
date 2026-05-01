SCHEMA = """
    CREATE TABLE IF NOT EXISTS bot_reactions (
        id          SERIAL PRIMARY KEY,
        guild_id    BIGINT  NOT NULL,
        trigger     TEXT    NOT NULL,
        response    TEXT    NOT NULL,
        role_id     BIGINT,
        created_by  BIGINT  NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (guild_id, trigger)
    );
"""
