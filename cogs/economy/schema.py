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

    CREATE TABLE IF NOT EXISTS salary_roles (
        guild_id         BIGINT NOT NULL,
        role_id          BIGINT NOT NULL,
        interval_seconds BIGINT NOT NULL,
        amount           BIGINT NOT NULL,
        PRIMARY KEY (guild_id, role_id)
    );

    -- Active .crime jail sentences: the prisoner role a member is wearing and when
    -- it should be taken back off. A background task (Economy.release_jails) removes
    -- the role once release_at passes, so sentences survive bot restarts. role_id is
    -- stored per-row so the right role is removed even if the guild's config changes.
    CREATE TABLE IF NOT EXISTS crime_jails (
        guild_id   BIGINT NOT NULL,
        user_id    BIGINT NOT NULL,
        role_id    BIGINT NOT NULL,
        release_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (guild_id, user_id)
    );
"""

MIGRATIONS = [
    # Recreate the transactions -> balances FK with ON DELETE CASCADE so that deleting a
    # member's balance also clears their transaction log. Without the cascade, the member
    # cleanup in on_member_remove fails with an FK violation (any active member has rows
    # in transactions), rolling back the whole cleanup and leaving them on the leaderboard.
    # The original FK's name is auto-generated, so drop whatever FK is on the table by name.
    """DO $$
    DECLARE cname text;
    BEGIN
        SELECT conname INTO cname FROM pg_constraint
        WHERE conrelid = 'transactions'::regclass AND contype = 'f';
        IF cname IS NOT NULL THEN
            EXECUTE 'ALTER TABLE transactions DROP CONSTRAINT ' || quote_ident(cname);
        END IF;
    END $$;""",
    "ALTER TABLE transactions ADD CONSTRAINT transactions_balances_fkey "
    "FOREIGN KEY (guild_id, user_id) REFERENCES balances(guild_id, user_id) ON DELETE CASCADE",
]

CONSTRAINTS = [
    "ALTER TABLE balances ADD CONSTRAINT wallet_non_negative CHECK (wallet >= 0)",
    "ALTER TABLE balances ADD CONSTRAINT bank_non_negative CHECK (bank >= 0)",
]
