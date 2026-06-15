CREATE TABLE IF NOT EXISTS solana_sync_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_pubkey TEXT NOT NULL,
    amount NUMERIC NOT NULL,

    status TEXT NOT NULL DEFAULT 'PENDING',

    retry_count INT NOT NULL DEFAULT 0,

    tx_signature TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_solana_sync_status
ON solana_sync_events(status);


CREATE INDEX idx_solana_sync_created
ON solana_sync_events(created_at);


CREATE INDEX idx_solana_sync_runner
ON solana_sync_events(owner_pubkey);


CREATE OR REPLACE FUNCTION create_payout_event_if_threshold(
    p_pubkey TEXT,
    p_threshold NUMERIC
)
RETURNS TABLE(created BOOLEAN, amount NUMERIC)
AS $$
DECLARE
    v_balance NUMERIC;
BEGIN
    SELECT offchain_accumulated_tokens
    INTO v_balance
    FROM runner_nodes
    WHERE owner_pubkey = p_pubkey
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE::BOOLEAN, 0::NUMERIC;
        RETURN;
    END IF;

    IF v_balance < p_threshold THEN
        RETURN QUERY SELECT FALSE::BOOLEAN, 0::NUMERIC;
        RETURN;
    END IF;

    INSERT INTO solana_sync_events (
        owner_pubkey,
        amount,
        status,
        created_at,
        updated_at
    ) VALUES (
        p_pubkey,
        v_balance,
        'PENDING',
        NOW(),
        NOW()
    );

    UPDATE runner_nodes
    SET offchain_accumulated_tokens = 0,
        updated_at = NOW()
    WHERE owner_pubkey = p_pubkey;

    RETURN QUERY SELECT TRUE::BOOLEAN, v_balance::NUMERIC;
END;
$$ LANGUAGE plpgsql;


















CREATE OR REPLACE FUNCTION create_payout_event_if_threshold(
    p_pubkey TEXT,
    p_threshold NUMERIC
)
RETURNS TABLE(
    created BOOLEAN,
    amount NUMERIC,
    reward_delta NUMERIC,
    owner_pubkey TEXT
)
AS $$
DECLARE
    v_balance NUMERIC;
    v_stake NUMERIC;
    v_multiplier NUMERIC := 1.0;
    v_base_reward NUMERIC := 0.010;
    v_reward NUMERIC;
BEGIN

    -- Lock runner row (CRITICAL)
    SELECT offchain_accumulated_tokens, staked_amount
    INTO v_balance, v_stake
    FROM runner_nodes
    WHERE owner_pubkey = p_pubkey
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 0, 0, NULL;
        RETURN;
    END IF;

    -- not enough balance
    IF v_balance < p_threshold THEN
        RETURN QUERY SELECT FALSE, 0, 0, p_pubkey;
        RETURN;
    END IF;

    -- -----------------------------
    -- STAKE BASED MULTIPLIER
    -- -----------------------------

    -- convert 9 decimals stake to tokens
    v_stake := v_stake / 1000000000.0;

    IF v_stake < 20 THEN
        v_multiplier := 1.0;
    ELSE
        v_multiplier := 1.5 + ((v_stake - 20) * 0.01);
    END IF;

    -- cap multiplier effect
    IF v_multiplier > 5 THEN
        v_multiplier := 5;
    END IF;

    -- final reward per payout cycle
    v_reward := v_base_reward * v_multiplier;

    -- cap reward per cycle
    IF v_reward > 0.50 THEN
        v_reward := 0.50;
    END IF;

    -- --------------------------------
    -- INSERT MULTIPLE PAYOUT EVENTS
    -- --------------------------------

    INSERT INTO solana_sync_events (
        owner_pubkey,
        amount,
        status,
        created_at,
        updated_at
    )
    SELECT
        p_pubkey,
        v_reward,
        'PENDING',
        NOW(),
        NOW()
    FROM generate_series(1, FLOOR(v_balance / p_threshold)::INT);

    -- --------------------------------
    -- RESET BALANCE AFTER QUEUE
    -- --------------------------------

    UPDATE runner_nodes
    SET offchain_accumulated_tokens = 0,
        updated_at = NOW()
    WHERE owner_pubkey = p_pubkey;

    -- return final info
    RETURN QUERY SELECT
        TRUE,
        v_reward,
        v_reward,
        p_pubkey;

END;
$$ LANGUAGE plpgsql;



CREATE FUNCTION create_payout_event_if_threshold(
    p_pubkey TEXT,
    p_threshold NUMERIC
)
RETURNS TABLE(
    created BOOLEAN,
    amount NUMERIC,
    reward_delta NUMERIC,
    owner_pubkey TEXT
)
AS $$
DECLARE
    v_balance NUMERIC;
    v_stake NUMERIC;
    v_multiplier NUMERIC := 1.0;
    v_base_reward NUMERIC := 0.010;
    v_reward NUMERIC;
BEGIN

    SELECT offchain_accumulated_tokens, staked_amount
    INTO v_balance, v_stake
    FROM runner_nodes
    WHERE owner_pubkey = p_pubkey
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 0, 0, NULL;
        RETURN;
    END IF;

    IF v_balance < p_threshold THEN
        RETURN QUERY SELECT FALSE, 0, 0, p_pubkey;
        RETURN;
    END IF;

    v_stake := v_stake / 1000000000.0;

    IF v_stake < 20 THEN
        v_multiplier := 1.0;
    ELSE
        v_multiplier := 1.5 + ((v_stake - 20) * 0.01);
    END IF;

    IF v_multiplier > 5 THEN
        v_multiplier := 5;
    END IF;

    v_reward := v_base_reward * v_multiplier;

    IF v_reward > 0.50 THEN
        v_reward := 0.50;
    END IF;

    INSERT INTO solana_sync_events (
        owner_pubkey,
        amount,
        status,
        created_at,
        updated_at
    )
    SELECT
        p_pubkey,
        v_reward,
        'PENDING',
        NOW(),
        NOW()
    FROM generate_series(1, FLOOR(v_balance / p_threshold)::INT);

    UPDATE runner_nodes
    SET offchain_accumulated_tokens = 0,
        updated_at = NOW()
    WHERE owner_pubkey = p_pubkey;

    RETURN QUERY SELECT
        TRUE,
        v_reward,
        v_reward,
        p_pubkey;

END;
$$ LANGUAGE plpgsql;