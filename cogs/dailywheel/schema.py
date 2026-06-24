SCHEMA = """
    CREATE TABLE IF NOT EXISTS wheel_prizes (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        kind        TEXT NOT NULL,
        weight      INT NOT NULL DEFAULT 1,
        amount      BIGINT,
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

CONSTRAINTS = [
    "ALTER TABLE wheel_prizes ADD CONSTRAINT wheel_prizes_kind_check "
    "CHECK (kind IN ('currency', 'message'))",
    "ALTER TABLE wheel_prizes ADD CONSTRAINT wheel_prizes_weight_positive CHECK (weight > 0)",
]
