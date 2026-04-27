SCHEMA = """
    CREATE TABLE IF NOT EXISTS counting (
        guild_id   BIGINT PRIMARY KEY,
        channel_id BIGINT NOT NULL,
        count      INTEGER DEFAULT 0,
        last_user  BIGINT  DEFAULT NULL
    );
"""
