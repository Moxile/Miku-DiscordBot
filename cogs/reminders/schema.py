SCHEMA = """
    CREATE TABLE IF NOT EXISTS reminders (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        channel_id  BIGINT NOT NULL,
        message     TEXT,
        remind_at   TIMESTAMPTZ NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
"""
