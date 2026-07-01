SCHEMA = """
    CREATE TABLE IF NOT EXISTS lb_excluded (
        guild_id  BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS reaction_lb_config (
        guild_id    BIGINT  NOT NULL PRIMARY KEY,
        emoji_key   TEXT    NOT NULL,
        is_custom   BOOLEAN NOT NULL,
        emoji_display TEXT  NOT NULL
    );

    CREATE TABLE IF NOT EXISTS reaction_lb_counts (
        guild_id  BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        count     BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    );
"""
