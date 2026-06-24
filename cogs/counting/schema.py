SCHEMA = """
    CREATE TABLE IF NOT EXISTS counting (
        guild_id   BIGINT PRIMARY KEY,
        channel_id BIGINT NOT NULL,
        count      INTEGER DEFAULT 0,
        last_user  BIGINT  DEFAULT NULL
    );

    CREATE TABLE IF NOT EXISTS counting_fails (
        guild_id BIGINT NOT NULL,
        user_id  BIGINT NOT NULL,
        fails    INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS counting_fail_roles (
        guild_id  BIGINT PRIMARY KEY,
        role_id   BIGINT NOT NULL,
        threshold INTEGER NOT NULL
    );
"""
