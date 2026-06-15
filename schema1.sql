-- Add the columns to the parent table; PostgreSQL handles the partitions automatically.
ALTER TABLE ping_logs
ADD COLUMN latitude NUMERIC(9,6) NOT NULL DEFAULT 0.0,
ADD COLUMN longitude NUMERIC(9,6) NOT NULL DEFAULT 0.0;

-- Optional: Add an index for faster geospatial filtering later
CREATE INDEX idx_ping_logs_geo ON ping_logs (latitude, longitude);


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

    -- Safety check: if user not found, return empty results
    IF NOT FOUND THEN
        RETURN QUERY SELECT 0.0::NUMERIC, FALSE;
        RETURN;
    END IF;

    -- 2. Process staking configurations
    IF v_is_val AND v_raw_stake >= 20000000000 THEN
        v_clean_tokens := v_raw_stake / 1000000000.0;
        v_multiplier := 1.5 + ((v_clean_tokens - 20.0) * 0.01);
        IF v_multiplier > 5.0 THEN v_multiplier := 5.0; END IF;
    END IF;

    -- 3. Atomic execution
    UPDATE runner_nodes
    SET offchain_accumulated_tokens  = offchain_accumulated_tokens + (p_delta * v_multiplier),
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