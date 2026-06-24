SCHEMA = """
    CREATE TABLE IF NOT EXISTS wheel_prizes (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        kind        TEXT NOT NULL,
        weight      INT NOT NULL DEFAULT 1,
        amount      BIGINT,
        role_id     BIGINT,
        text        TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS wheel_spins (
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        last_spin   DATE NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    );
"""

MIGRATIONS = [
    "ALTER TABLE wheel_prizes ADD COLUMN IF NOT EXISTS role_id BIGINT",
    # Widen the kind check to allow 'role' prizes; the constraint name is fixed so we can
    # drop it by name before re-adding with the extra value.
    "ALTER TABLE wheel_prizes DROP CONSTRAINT IF EXISTS wheel_prizes_kind_check",
]

CONSTRAINTS = [
    "ALTER TABLE wheel_prizes ADD CONSTRAINT wheel_prizes_kind_check "
    "CHECK (kind IN ('currency', 'message', 'role'))",
    "ALTER TABLE wheel_prizes ADD CONSTRAINT wheel_prizes_weight_positive CHECK (weight > 0)",
]
