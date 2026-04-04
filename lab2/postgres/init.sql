CREATE TABLE IF NOT EXISTS casino_games_processed (
    id              BIGSERIAL PRIMARY KEY,
    casino          TEXT,
    game            TEXT,
    provider        TEXT,
    rtp             DOUBLE PRECISION,
    volatility      TEXT,
    jackpot         TEXT,
    game_type       TEXT,
    game_category   TEXT,
    min_bet         DOUBLE PRECISION,
    max_win         DOUBLE PRECISION,
    release_year    INTEGER,
    mobile_compatible BOOLEAN,
    max_multiplier  DOUBLE PRECISION,
    risk_tier       TEXT,
    kafka_offset    BIGINT,
    kafka_partition INT,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Агрегаты по категории игр в рамках одного микробатча Spark
CREATE TABLE IF NOT EXISTS casino_category_batch_agg (
    batch_id        BIGINT NOT NULL,
    game_category   TEXT NOT NULL,
    event_count     BIGINT NOT NULL,
    avg_rtp         DOUBLE PRECISION,
    avg_max_win     DOUBLE PRECISION,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, game_category)
);

CREATE INDEX IF NOT EXISTS idx_casino_processed_category
    ON casino_games_processed (game_category);
