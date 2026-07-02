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

GUILD_CURRENCY = """
    CREATE TABLE IF NOT EXISTS guild_currency (
        guild_id BIGINT PRIMARY KEY,
        name     TEXT NOT NULL,
        emoji    TEXT NOT NULL
    );
"""

IGNORED_CHANNELS = """
    CREATE TABLE IF NOT EXISTS ignored_channels (
        guild_id   BIGINT NOT NULL,
        channel_id BIGINT NOT NULL,
        PRIMARY KEY (guild_id, channel_id)
    );
"""

SCHEMA = GUILD_SETTINGS + DISABLED_COGS + GUILD_CURRENCY + IGNORED_CHANNELS
