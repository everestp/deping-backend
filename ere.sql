CREATE OR REPLACE FUNCTION create_payout_event_if_threshold(
    p_pubkey TEXT,
    p_threshold NUMERIC
)
RETURNS TABLE(
    created BOOLEAN,
    amount NUMERIC,
    reward_delta NUMERIC,
    node_pubkey TEXT  -- renamed from owner_pubkey
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_staked_amount NUMERIC;
    v_current_offchain NUMERIC;
    v_base_reward NUMERIC := 0.010;
    v_multiplier NUMERIC := 1.0;
    v_new_reward NUMERIC;
    v_new_balance NUMERIC;
BEGIN
    SELECT staked_amount, offchain_accumulated_tokens
    INTO v_staked_amount, v_current_offchain
    FROM runner_nodes
    WHERE runner_nodes.owner_pubkey = p_pubkey
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT FALSE, 0.0::NUMERIC, 0.0::NUMERIC, NULL::TEXT;
        RETURN;
    END IF;

    IF v_staked_amount >= 20 THEN
        v_multiplier := 1.5 + ((v_staked_amount - 20) * 0.01);
    END IF;

    IF v_multiplier > 5 THEN v_multiplier := 5; END IF;

    v_new_reward := v_base_reward * v_multiplier;
    IF v_new_reward > 0.50 THEN v_new_reward := 0.50; END IF;

    v_new_balance := v_current_offchain + v_new_reward;

    UPDATE runner_nodes
    SET 
        offchain_accumulated_tokens = CASE WHEN v_new_balance >= p_threshold THEN 0 ELSE v_new_balance END,
        total_earned_tokens_all_time = total_earned_tokens_all_time + v_new_reward,
        updated_at = NOW()
    WHERE runner_nodes.owner_pubkey = p_pubkey;

    IF v_new_balance >= p_threshold THEN
        INSERT INTO solana_sync_events (
            owner_pubkey, amount, status, created_at, updated_at
        )
        VALUES (p_pubkey, v_new_balance, 'PENDING', NOW(), NOW());

        RETURN QUERY SELECT TRUE, v_new_balance, v_new_reward, p_pubkey;
    ELSE
        RETURN QUERY SELECT FALSE, 0.0::NUMERIC, v_new_reward, p_pubkey;
    END IF;
END;
$$;