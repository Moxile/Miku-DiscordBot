"""Schema for cross-feature shared tables."""

GUILD_SETTINGS = """
    CREATE TABLE IF NOT EXISTS guild_settings (
        guild_id BIGINT NOT NULL,
        key      TEXT NOT NULL,
        value    TEXT NOT NULL,
        PRIMARY KEY (guild_id, key)
    );
"""

DISABLED_COGS = """
    CREATE TABLE IF NOT EXISTS disabled_cogs (
        guild_id BIGINT NOT NULL,
        cog_name TEXT NOT NULL,
        PRIMARY KEY (guild_id, cog_name)
    );
"""

SCHEMA = GUILD_SETTINGS + DISABLED_COGS
