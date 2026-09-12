SCHEMA = """
    -- Poker stacks are wallet funds temporarily held by a live table.  The value is
    -- updated between hands (never during one), so startup recovery safely rolls an
    -- interrupted hand back to its last completed chip counts.
    CREATE TABLE IF NOT EXISTS poker_escrow (
        guild_id  BIGINT NOT NULL,
        channel_id BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        chips     BIGINT NOT NULL CHECK (chips >= 0),
        fee_paid  BIGINT NOT NULL DEFAULT 0 CHECK (fee_paid >= 0),
        PRIMARY KEY (guild_id, user_id)
    );
"""

MIGRATIONS = [
    "ALTER TABLE poker_escrow ADD COLUMN IF NOT EXISTS fee_paid BIGINT NOT NULL DEFAULT 0",
]
