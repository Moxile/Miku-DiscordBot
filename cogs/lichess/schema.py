SCHEMA = """
    CREATE TABLE IF NOT EXISTS lichess_connections (
        guild_id          BIGINT NOT NULL,
        discord_user_id   BIGINT NOT NULL,
        lichess_username   TEXT   NOT NULL,
        access_token      TEXT,
        connected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (guild_id, discord_user_id)
    );

    CREATE TABLE IF NOT EXISTS lichess_oauth_pending (
        state             TEXT PRIMARY KEY,
        discord_user_id   BIGINT NOT NULL,
        guild_id          BIGINT NOT NULL,
        channel_id        BIGINT NOT NULL,
        code_verifier     TEXT NOT NULL,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
"""
