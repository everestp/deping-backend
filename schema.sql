CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE users (
    id            SERIAL PRIMARY KEY,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    wallet_pubkey VARCHAR(44)  UNIQUE NOT NULL,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at    TIMESTAMP NULL
);

CREATE TABLE monitors (
    id                     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id               INT REFERENCES users(id),
    target_url             VARCHAR(512) NOT NULL,
    check_interval_seconds INT DEFAULT 60,
    credit_balance_checks  BIGINT DEFAULT 0,
    total_spent_tokens     NUMERIC(12,4) DEFAULT 0.0000,
    is_active              BOOLEAN DEFAULT TRUE,
    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at             TIMESTAMP NULL
);

CREATE TABLE runner_nodes (
    id                           SERIAL PRIMARY KEY,
    owner_email                  VARCHAR(255) REFERENCES users(email),
    owner_pubkey                 VARCHAR(128) NOT NULL,  -- ed25519 hex pubkey
     node_pubkey                 VARCHAR(128) NOT NULL,
    region                       VARCHAR(20)  NOT NULL DEFAULT 'unknown',
    latitude                     NUMERIC(9,6) NOT NULL,
    node_pda                     VARCHAR(128) NULL
    longitude                    NUMERIC(9,6) NOT NULL,
    offchain_accumulated_tokens  NUMERIC(12,4) DEFAULT 0.0000,
    total_earned_tokens_all_time NUMERIC(12,4) DEFAULT 0.0000,
    pending_solana_sync          BOOLEAN DEFAULT FALSE,
    last_seen_timestamp          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at                   TIMESTAMP NULL
);

CREATE TABLE solana_sync_events (
    id            SERIAL PRIMARY KEY,
    runner_pubkey VARCHAR(128) NOT NULL,
    tx_signature  VARCHAR(128) UNIQUE NOT NULL,
    amount_raw    BIGINT NOT NULL,
    confirmed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ping_logs: RANGE partitioned weekly.
-- Phase latencies stored in microseconds (matching ProbeResult from monitor.proto).
-- latency_ms is a derived column (total_us / 1000) kept for simple queries.
CREATE TABLE ping_logs (
    id            BIGSERIAL,
    monitor_id    UUID        NOT NULL,
    runner_pubkey VARCHAR(128) NOT NULL,
    -- ProbeResult phase latencies (microseconds)
    dns_us        BIGINT      NOT NULL DEFAULT 0,
    tcp_us        BIGINT      NOT NULL DEFAULT 0,
    tls_us        BIGINT      NOT NULL DEFAULT 0,  -- 0 for plain HTTP
    ttfb_us       BIGINT      NOT NULL DEFAULT 0,
    total_us      BIGINT      NOT NULL DEFAULT 0,
    -- Derived millisecond value for backwards-compatible uptime queries
    latency_ms    INT         NOT NULL DEFAULT 0,
    status_code   INT         NOT NULL DEFAULT 0,
    success       BOOLEAN     NOT NULL DEFAULT FALSE,
    error_kind    VARCHAR(400) NOT NULL DEFAULT '',  -- empty on success
    geo_region    VARCHAR(50) NOT NULL,
    timestamp     TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Pre-create 8 weekly partitions (partition_cron.go creates more automatically)
CREATE TABLE ping_logs_2025_w21 PARTITION OF ping_logs FOR VALUES FROM ('2025-05-19') TO ('2025-05-26');
CREATE TABLE ping_logs_2025_w22 PARTITION OF ping_logs FOR VALUES FROM ('2025-05-26') TO ('2025-06-02');
CREATE TABLE ping_logs_2025_w23 PARTITION OF ping_logs FOR VALUES FROM ('2025-06-02') TO ('2025-06-09');
CREATE TABLE ping_logs_2025_w24 PARTITION OF ping_logs FOR VALUES FROM ('2025-06-09') TO ('2025-06-16');
CREATE INDEX ON ping_logs_2025_w21 (monitor_id, timestamp DESC);
CREATE INDEX ON ping_logs_2025_w22 (monitor_id, timestamp DESC);
CREATE INDEX ON ping_logs_2025_w23 (monitor_id, timestamp DESC);
CREATE INDEX ON ping_logs_2025_w24 (monitor_id, timestamp DESC);

-- Core indexes
CREATE UNIQUE INDEX idx_users_wallet   ON users        (wallet_pubkey)               WHERE deleted_at IS NULL;
CREATE UNIQUE INDEX idx_runner_pubkey  ON runner_nodes (owner_pubkey)                WHERE deleted_at IS NULL;
CREATE INDEX idx_runner_region         ON runner_nodes (region)                      WHERE deleted_at IS NULL;
CREATE INDEX idx_monitor_active        ON monitors     (is_active)                   WHERE deleted_at IS NULL;
CREATE INDEX idx_sync_events_runner    ON solana_sync_events (runner_pubkey);

-- Auto-update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at() RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_users_updated    BEFORE UPDATE ON users        FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_monitors_updated BEFORE UPDATE ON monitors     FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_runners_updated  BEFORE UPDATE ON runner_nodes FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Atomic reward accumulation — single DB round-trip, preserves overflow fraction.
CREATE OR REPLACE FUNCTION accumulate_runner_reward(
    p_pubkey    VARCHAR,
    p_delta     NUMERIC,
    p_threshold NUMERIC
) RETURNS TABLE(new_balance NUMERIC, did_sync BOOLEAN) AS $$
DECLARE
    v_balance NUMERIC;
    v_synced  BOOLEAN := FALSE;
BEGIN
    UPDATE runner_nodes
    SET offchain_accumulated_tokens  = offchain_accumulated_tokens  + p_delta,
        total_earned_tokens_all_time = total_earned_tokens_all_time + p_delta
    WHERE owner_pubkey = p_pubkey AND deleted_at IS NULL
    RETURNING offchain_accumulated_tokens INTO v_balance;

    IF v_balance >= p_threshold THEN
        UPDATE runner_nodes
        SET offchain_accumulated_tokens = v_balance - p_threshold
        WHERE owner_pubkey = p_pubkey;
        v_balance := v_balance - p_threshold;
        v_synced  := TRUE;
    END IF;
    RETURN QUERY SELECT v_balance, v_synced;
END;
$$ LANGUAGE plpgsql;


ALTER TABLE runner_nodes
ADD COLUMN node_pubkey VARCHAR(128) NOT NULL UNIQUE;


















-- Dedicated function to keep miners marked active in the current hour window
CREATE OR REPLACE FUNCTION trigger_set_runner_heartbeat()
RETURNS TRIGGER AS $$
BEGIN
  NEW.last_seen_timestamp = NOW();
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_runners_heartbeat ON runner_nodes;

CREATE TRIGGER trg_runners_heartbeat
    BEFORE UPDATE OF region, latitude, longitude ON runner_nodes
    FOR EACH ROW
    EXECUTE FUNCTION trigger_set_runner_heartbeat();












UPDATE monitors
SET is_active = FALSE
WHERE target_url LIKE '%broken%' OR target_url LIKE '%unstable%';






ALTER TABLE ping_logs ALTER COLUMN error_kind TYPE VARCHAR(400);


ALTER TABLE ping_logs ADD COLUMN latitude NUMERIC(9,6) DEFAULT 0.0;
ALTER TABLE ping_logs ADD COLUMN longitude NUMERIC(9,6) DEFAULT 0.0;








-- ═══════════════════════════════════════════════════════════════════════════
-- Telegram Bot Schema
-- Requires: update_updated_at() function from base schema
-- ═══════════════════════════════════════════════════════════════════════════

-- ── Telegram account linkage ──────────────────────────────────────────────
-- One user can link exactly one Telegram account.
-- Verification flow: web generates code → user sends to bot → bot confirms.
CREATE TABLE telegram_users (
    id                  SERIAL PRIMARY KEY,
    user_id             INT    UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    telegram_chat_id    BIGINT UNIQUE,                    -- NULL until verified
    telegram_username   VARCHAR(255),
    verification_code   VARCHAR(32),                      -- 6-digit code, cleared after use
    is_verified         BOOLEAN NOT NULL DEFAULT FALSE,
    last_reminded_at    TIMESTAMP NULL,                   -- Throttle for weekly credit reminders
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at          TIMESTAMP NULL
);

-- ── Per-user credit ledger ────────────────────────────────────────────────
-- Free: 3 messages/day (resets at midnight UTC).
-- Premium: deducted from purchased_credits when free credits exhausted.
CREATE TABLE telegram_credits (
    id                    SERIAL PRIMARY KEY,
    user_id               INT UNIQUE REFERENCES users(id) ON DELETE CASCADE,
    free_credits_used     INT  NOT NULL DEFAULT 0,
    free_reset_date       DATE NOT NULL DEFAULT CURRENT_DATE, -- last reset date
    purchased_credits     INT  NOT NULL DEFAULT 0,            -- bought via Solana SPL
    total_purchased_ever  INT  NOT NULL DEFAULT 0,            -- audit: lifetime purchased
    created_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── Per-monitor health state (Two-Packet state machine) ───────────────────
-- Tracks consecutive failures to implement the Two-Packet rule:
-- Only alert on 2nd consecutive failure; only send RESTORED if DOWN alert was sent.
CREATE TABLE monitor_health_state (
    monitor_id           UUID PRIMARY KEY REFERENCES monitors(id) ON DELETE CASCADE,
    is_down              BOOLEAN NOT NULL DEFAULT FALSE,
    consecutive_failures INT     NOT NULL DEFAULT 0,
    alert_sent           BOOLEAN NOT NULL DEFAULT FALSE,  -- TRUE = DOWN alert was dispatched
    last_alerted_at      TIMESTAMP NULL,
    updated_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── Per-monitor notification subscriptions ───────────────────────────────
-- Dashboard toggle: user enables/disables alerts for a specific monitor.
CREATE TABLE telegram_subscriptions (
    id                       SERIAL PRIMARY KEY,
    user_id                  INT  NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    monitor_id               UUID NOT NULL REFERENCES monitors(id) ON DELETE CASCADE,
    is_notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at               TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (user_id, monitor_id)
);

-- ── Triggers ──────────────────────────────────────────────────────────────
CREATE TRIGGER trg_telegram_users_updated
    BEFORE UPDATE ON telegram_users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_telegram_credits_updated
    BEFORE UPDATE ON telegram_credits
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_telegram_subs_updated
    BEFORE UPDATE ON telegram_subscriptions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_health_state_updated
    BEFORE UPDATE ON monitor_health_state
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ── Indexes ───────────────────────────────────────────────────────────────
CREATE INDEX idx_telegram_users_chat_id ON telegram_users (telegram_chat_id) WHERE deleted_at IS NULL;
CREATE INDEX idx_telegram_users_user_id ON telegram_users (user_id)          WHERE deleted_at IS NULL;
CREATE INDEX idx_telegram_subs_monitor  ON telegram_subscriptions (monitor_id);
CREATE INDEX idx_telegram_subs_user     ON telegram_subscriptions (user_id);
CREATE INDEX idx_health_state_down      ON monitor_health_state (is_down) WHERE is_down = TRUE;

-- ═══════════════════════════════════════════════════════════════════════════
-- Atomic credit deduction (thread-safe, single round-trip)
-- Returns: (success BOOL, credit_type TEXT)
--   credit_type: 'free' | 'purchased' | 'no_credits'
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION deduct_telegram_credit(
    p_user_id INT
) RETURNS TABLE(success BOOLEAN, credit_type TEXT) AS $$
DECLARE
    v_free_used  INT;
    v_reset_date DATE;
    v_purchased  INT;
    v_today      DATE := CURRENT_DATE;
BEGIN
    -- Ensure row exists (idempotent upsert)
    INSERT INTO telegram_credits (user_id)
    VALUES (p_user_id)
    ON CONFLICT (user_id) DO NOTHING;

    -- Lock the row for this transaction
    SELECT free_credits_used, free_reset_date, purchased_credits
    INTO v_free_used, v_reset_date, v_purchased
    FROM telegram_credits
    WHERE user_id = p_user_id
    FOR UPDATE;

    -- Daily reset: if stored reset_date is before today, wipe free usage
    IF v_reset_date < v_today THEN
        UPDATE telegram_credits
        SET free_credits_used = 0,
            free_reset_date   = v_today
        WHERE user_id = p_user_id;
        v_free_used := 0;
    END IF;

    -- Tier 1: Free (3/day)
    IF v_free_used < 3 THEN
        UPDATE telegram_credits
        SET free_credits_used = free_credits_used + 1
        WHERE user_id = p_user_id;
        RETURN QUERY SELECT TRUE, 'free'::TEXT;
        RETURN;
    END IF;

    -- Tier 2: Purchased credits
    IF v_purchased > 0 THEN
        UPDATE telegram_credits
        SET purchased_credits = purchased_credits - 1
        WHERE user_id = p_user_id;
        RETURN QUERY SELECT TRUE, 'purchased'::TEXT;
        RETURN;
    END IF;

    -- No credits available
    RETURN QUERY SELECT FALSE, 'no_credits'::TEXT;
END;
$$ LANGUAGE plpgsql;

-- ═══════════════════════════════════════════════════════════════════════════
-- Add purchased credits (called after Solana SPL payment confirmed)
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION add_purchased_credits(
    p_user_id INT,
    p_amount  INT
) RETURNS INT AS $$
DECLARE
    v_new_balance INT;
BEGIN
    INSERT INTO telegram_credits (user_id, purchased_credits, total_purchased_ever)
    VALUES (p_user_id, p_amount, p_amount)
    ON CONFLICT (user_id) DO UPDATE
        SET purchased_credits    = telegram_credits.purchased_credits + p_amount,
            total_purchased_ever = telegram_credits.total_purchased_ever + p_amount
    RETURNING purchased_credits INTO v_new_balance;

    RETURN v_new_balance;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM users;


SELECT timestamp_ms, typeof(timestamp_ms) FROM ping_logs LIMIT 1;

-- 1. Add the Unix Millisecond column
ALTER TABLE ping_logs ADD COLUMN timestamp_ms BIGINT NOT NULL DEFAULT 0;

-- 2. Populate it from your existing TIMESTAMP column
UPDATE ping_logs
SET timestamp_ms = (EXTRACT(EPOCH FROM timestamp) * 1000)::BIGINT;


ALTER TABLE ping_logs ADD COLUMN timestamp_ms BIGINT NOT NULL DEFAULT 0;

UPDATE ping_logs
SET timestamp_ms = (EXTRACT(EPOCH FROM timestamp) * 1000)::BIGINT;









ALTER TABLE runner_nodes
    ADD COLUMN is_validator BOOLEAN NOT NULL DEFAULT FALSE,
    -- Using NUMERIC(20,0) to hold raw u64 token values precisely without rounding errors
    ADD COLUMN staked_amount NUMERIC(20,0) NOT NULL DEFAULT 0,
    ADD COLUMN unstake_request_at TIMESTAMP NULL DEFAULT NULL;

-- Dynamic multiplier performance index
CREATE INDEX idx_runners_stake_weight ON runner_nodes (is_validator, staked_amount) WHERE deleted_at IS NULL;
















CREATE OR REPLACE FUNCTION accumulate_runner_reward(
    p_pubkey    VARCHAR,
    p_delta     NUMERIC,
    p_threshold NUMERIC
) RETURNS TABLE(new_balance NUMERIC, did_sync BOOLEAN) AS $$
DECLARE
    v_balance        NUMERIC;
    v_synced         BOOLEAN := FALSE;
    v_multiplier     NUMERIC := 1.0;
    v_is_val         BOOLEAN;
    v_raw_stake      NUMERIC;
    v_clean_tokens   NUMERIC;
BEGIN
    -- 1. Fetch live validation parameters
    SELECT is_validator, staked_amount INTO v_is_val, v_raw_stake
    FROM runner_nodes
    WHERE owner_pubkey = p_pubkey AND deleted_at IS NULL;

    -- 2. Process staking configurations adjusted for 9 decimals (20 Tokens = 20_000_000_000)
    IF v_is_val AND v_raw_stake >= 20000000000 THEN
        -- Convert raw u64 (9 decimals) to a human-readable clean token count
        v_clean_tokens := v_raw_stake / 1000000000.0;

        -- Multiplier growth curve: Base 1.5x + 0.01x per token above 20 tokens
        v_multiplier := 1.5 + ((v_clean_tokens - 20.0) * 0.01);

        -- Protection ceiling cap
        IF v_multiplier > 5.0 THEN
            v_multiplier := 5.0;
        END IF;
    END IF;

    -- 3. Atomic execution
    UPDATE runner_nodes
    SET offchain_accumulated_tokens  = offchain_accumulated_tokens  + (p_delta * v_multiplier),
        total_earned_tokens_all_time = total_earned_tokens_all_time + (p_delta * v_multiplier)
    WHERE owner_pubkey = p_pubkey AND deleted_at IS NULL
    RETURNING offchain_accumulated_tokens INTO v_balance;

    -- 4. Payout cycle milestone evaluation
    IF v_balance >= p_threshold THEN
        UPDATE runner_nodes
        SET offchain_accumulated_tokens = v_balance - p_threshold,
            pending_solana_sync         = TRUE
        WHERE owner_pubkey = p_pubkey;

        v_balance := v_balance - p_threshold;
        v_synced  := TRUE;
    END IF;

    RETURN QUERY SELECT v_balance, v_synced;
END;
$$ LANGUAGE plpgsql;












