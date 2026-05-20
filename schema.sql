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
    region                       VARCHAR(20)  NOT NULL DEFAULT 'unknown',
    latitude                     NUMERIC(9,6) NOT NULL,
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
    error_kind    VARCHAR(64) NOT NULL DEFAULT '',  -- empty on success
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
