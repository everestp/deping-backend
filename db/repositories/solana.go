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
