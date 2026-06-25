SCHEMA = """
    CREATE TABLE IF NOT EXISTS lb_excluded (
        guild_id  BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    );
"""
