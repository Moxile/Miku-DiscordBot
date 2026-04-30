SCHEMA = """
    CREATE TABLE IF NOT EXISTS lichess_accounts (
        user_id          BIGINT PRIMARY KEY,
        lichess_id       TEXT   NOT NULL UNIQUE,
        lichess_username TEXT   NOT NULL,
        access_token     TEXT   NOT NULL,
        refresh_token    TEXT,
        token_expires_at TIMESTAMPTZ,
        linked_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_synced_at   TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS lichess_ratings (
        user_id    BIGINT  NOT NULL REFERENCES lichess_accounts(user_id) ON DELETE CASCADE,
        variant    TEXT    NOT NULL,
        rating     INT     NOT NULL,
        games      INT     NOT NULL,
        prov       BOOLEAN NOT NULL DEFAULT FALSE,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (user_id, variant)
    );

    CREATE TABLE IF NOT EXISTS lichess_rating_roles (
        guild_id  BIGINT NOT NULL,
        variant   TEXT   NOT NULL,
        tier      INT    NOT NULL,
        role_id   BIGINT NOT NULL,
        PRIMARY KEY (guild_id, variant, tier)
    );

    CREATE TABLE IF NOT EXISTS lichess_rating_role_config (
        guild_id   BIGINT  NOT NULL,
        variant    TEXT    NOT NULL,
        min_rating INT     NOT NULL DEFAULT 2000,
        step       INT     NOT NULL DEFAULT 100,
        max_rating INT     NOT NULL DEFAULT 2700,
        enabled    BOOLEAN NOT NULL DEFAULT TRUE,
        PRIMARY KEY (guild_id, variant)
    );

    CREATE TABLE IF NOT EXISTS chess_profiles (
        user_id          BIGINT PRIMARY KEY,
        style            TEXT   NOT NULL DEFAULT 'default',
        favorite_variant TEXT,
        bio              TEXT
    );
"""

MIGRATIONS = []
CONSTRAINTS = []
