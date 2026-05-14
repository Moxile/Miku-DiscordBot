SCHEMA = """
    CREATE TABLE IF NOT EXISTS missions (
        id          BIGSERIAL PRIMARY KEY,
        guild_id    BIGINT NOT NULL,
        name        TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        goal        BIGINT NOT NULL,
        funded      BIGINT NOT NULL DEFAULT 0,
        status      TEXT NOT NULL DEFAULT 'active',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS mission_contributions (
        id             BIGSERIAL PRIMARY KEY,
        mission_id     BIGINT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
        guild_id       BIGINT NOT NULL,
        user_id        BIGINT NOT NULL,
        amount         BIGINT NOT NULL,
        contributed_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
"""
