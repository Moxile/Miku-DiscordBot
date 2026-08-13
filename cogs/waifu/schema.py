SCHEMA = """
    CREATE TABLE IF NOT EXISTS waifus (
        guild_id       BIGINT NOT NULL,
        user_id        BIGINT NOT NULL,
        owner_id       BIGINT,
        value          BIGINT NOT NULL DEFAULT 5000,
        last_bought_at TIMESTAMPTZ,
        last_gifted_at TIMESTAMPTZ,
        spouse_id      BIGINT,
        engaged_since  TIMESTAMPTZ,
        last_begged_at TIMESTAMPTZ,
        PRIMARY KEY (guild_id, user_id)
    );
"""

MIGRATIONS = [
    # Tracks the last time an owner gifted money to this waifu; gifting daily
    # pauses value decay (see decay_waifu_values).
    "ALTER TABLE waifus ADD COLUMN IF NOT EXISTS last_gifted_at TIMESTAMPTZ",
    # Tracks the last time an owner begged this specific waifu (see .beg — cooldown
    # is per owned waifu, not global to the owner).
    "ALTER TABLE waifus ADD COLUMN IF NOT EXISTS last_begged_at TIMESTAMPTZ",
    # Backfill: engagement was never set on the gift path, so pairs that came to
    # mutually own each other via .waifugift stayed un-engaged. Mark any such
    # (non-married) mutual-ownership pair as engaged. Idempotent — only touches
    # rows with engaged_since still NULL.
    """
    UPDATE waifus w
    SET engaged_since = NOW()
    WHERE w.engaged_since IS NULL
      AND w.spouse_id IS NULL
      AND w.owner_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM waifus o
          WHERE o.guild_id = w.guild_id
            AND o.user_id = w.owner_id
            AND o.owner_id = w.user_id
      )
    """,
]
