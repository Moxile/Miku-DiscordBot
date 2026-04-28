SCHEMA = """
    CREATE TABLE IF NOT EXISTS reaction_roles (
        guild_id    BIGINT NOT NULL,
        channel_id  BIGINT NOT NULL,
        message_id  BIGINT NOT NULL,
        emoji       TEXT   NOT NULL,
        is_custom   BOOLEAN NOT NULL,
        role_id     BIGINT NOT NULL,
        created_by  BIGINT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (guild_id, message_id, emoji)
    );
    CREATE INDEX IF NOT EXISTS idx_rr_message ON reaction_roles(message_id);

    CREATE TABLE IF NOT EXISTS reaction_role_defaults (
        guild_id    BIGINT NOT NULL,
        channel_id  BIGINT NOT NULL,
        message_id  BIGINT NOT NULL,
        role_id     BIGINT NOT NULL,
        created_by  BIGINT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (guild_id, message_id)
    );
"""
