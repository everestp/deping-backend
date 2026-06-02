package repositories

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type solanaSyncRepo struct {
	pool *pgxpool.Pool
}

func (r *solanaSyncRepo) RecordSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error {
	const q = `
		INSERT INTO solana_sync_events (runner_pubkey, tx_signature, amount_raw, confirmed_at)
		VALUES ($1, $2, $3, NOW())`

	_, err := r.pool.Exec(ctx, q, runnerPubkey, txSignature, amountRaw)
	if err != nil {
		return fmt.Errorf("solanaSyncRepo.RecordSync: %w", err)
	}
	return nil
}

func (r *solanaSyncRepo) ExistsBySignature(ctx context.Context, txSignature string) (bool, error) {
	const q = `SELECT EXISTS(SELECT 1 FROM solana_sync_events WHERE tx_signature = $1)`

	var exists bool
	err := r.pool.QueryRow(ctx, q, txSignature).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("solanaSyncRepo.ExistsBySignature: %w", err)
	}
	return exists, nil
}

func (r *solanaSyncRepo) FinalizeSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error {
    tx, err := r.pool.Begin(ctx)
    if err != nil {
        return err
    }
    defer tx.Rollback(ctx)

    // 1. Log the sync event (Fixed query)
    const logQ = `INSERT INTO solana_sync_events (runner_pubkey, tx_signature, amount_raw, confirmed_at) VALUES ($1, $2, $3, NOW())`
    _, err = tx.Exec(ctx, logQ, runnerPubkey, txSignature, amountRaw)
    if err != nil {
        return fmt.Errorf("failed to insert sync event: %w", err)
    }

    // 2. Clear the pending flag and decrement balance (Fixed query)
    // We cast amountRaw to numeric to match your NUMERIC(12,4) column
    const updateQ = `
        UPDATE runner_nodes
        SET pending_solana_sync = FALSE,
            offchain_accumulated_tokens = offchain_accumulated_tokens - ($1::numeric / 1000000000.0)
        WHERE owner_pubkey = $2`

    _, err = tx.Exec(ctx, updateQ, amountRaw, runnerPubkey)
    if err != nil {
        return fmt.Errorf("failed to update runner balance: %w", err)
    }

    return tx.Commit(ctx)
}
