"""Schema for cross-feature shared tables."""

GUILD_SETTINGS = """
    CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id BIGINT NOT NULL,
        key      TEXT NOT NULL,
        value    TEXT NOT NULL,
        PRIMARY KEY (guild_id, key)
    );
"""
