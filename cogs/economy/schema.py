SCHEMA = """
    CREATE TABLE IF NOT EXISTS locked_users (
        guild_id  BIGINT NOT NULL,
        user_id   BIGINT NOT NULL,
        locked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        PRIMARY KEY (guild_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS balances (
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        wallet     BIGINT NOT NULL DEFAULT 0,
        bank        BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (guild_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS transactions (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        amount      BIGINT NOT NULL,
        tx_type     TEXT NOT NULL,
        description TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        FOREIGN KEY (guild_id, user_id) REFERENCES balances(guild_id, user_id)
    );

    CREATE TABLE IF NOT EXISTS cooldowns (
        guild_id    BIGINT NOT NULL,
        user_id     BIGINT NOT NULL,
        command     TEXT NOT NULL,
        expires_at  TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (guild_id, user_id, command)
    );
"""

CONSTRAINTS = [
    "ALTER TABLE balances ADD CONSTRAINT wallet_non_negative CHECK (wallet >= 0)",
    "ALTER TABLE balances ADD CONSTRAINT bank_non_negative CHECK (bank >= 0)",
]
