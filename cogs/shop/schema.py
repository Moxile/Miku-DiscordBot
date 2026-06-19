SCHEMA = """
    CREATE TABLE IF NOT EXISTS items (
        id           SERIAL PRIMARY KEY,
        guild_id     BIGINT NOT NULL,
        name         TEXT NOT NULL,
        description  TEXT,
        price        BIGINT NOT NULL,
        sell_price   BIGINT NOT NULL DEFAULT 0,
        item_type    TEXT NOT NULL DEFAULT 'item',
        metadata     JSONB DEFAULT '{}',
        is_available BOOLEAN NOT NULL DEFAULT TRUE,
        role_given   BIGINT,
        UNIQUE (guild_id, name)
    );

    CREATE TABLE IF NOT EXISTS inventory (
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        item_id     INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
        quantity    INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (guild_id, user_id, item_id)
    );

    CREATE TABLE IF NOT EXISTS temporary_roles (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        role_id     BIGINT NOT NULL,
        expires_at  TIMESTAMPTZ NOT NULL,
        UNIQUE (guild_id, user_id, role_id)
    );
"""

MIGRATIONS = [
    # Seconds a granted role lasts; NULL = permanent.
    "ALTER TABLE items ADD COLUMN IF NOT EXISTS role_duration INTEGER;",
]
