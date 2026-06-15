package repositories

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type runnerRepo struct {
	pool *pgxpool.Pool
}

// NewRunnerRepository instantiates the explicit concrete repository implementation.
func NewRunnerRepository(pool *pgxpool.Pool) RunnerRepository {
	return &runnerRepo{pool: pool}
}

// Register inserts a new runner node or updates an existing one on key public key conflicts.
func (r *runnerRepo) Register(
	ctx context.Context,
	ownerEmail,
	ownerPubkey,
	region string,
	lat,
	lng float64,
) (*RunnerNode, error) {

	const q = `
		INSERT INTO runner_nodes (
			owner_email,
			owner_pubkey,
			region,
			latitude,
			longitude
		)
		VALUES ($1, $2, $3, $4, $5)

		ON CONFLICT (owner_pubkey)
		DO UPDATE SET
			last_seen_timestamp = NOW(),
			region = EXCLUDED.region,
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			updated_at = NOW()

		RETURNING
			id,
			owner_email,
			owner_pubkey,
			node_pubkey,
			region,
			latitude,
			longitude,
			offchain_accumulated_tokens,
			total_earned_tokens_all_time,
			pending_solana_sync,
			last_seen_timestamp,
			created_at,
			updated_at,
			deleted_at,
			is_validator,
			staked_amount,
			unstake_request_at
	`

	n := &RunnerNode{}

	err := r.pool.QueryRow(
		ctx,
		q,
		ownerEmail,
		ownerPubkey,
		region,
		lat,
		lng,
	).Scan(
		&n.ID,
		&n.OwnerEmail,
		&n.OwnerPubkey,
		&n.NodePubkey,
		&n.Region,
		&n.Latitude,
		&n.Longitude,
		&n.OffchainAccumulatedTokens,
		&n.TotalEarnedTokensAllTime,
		&n.PendingSolanaSync,
		&n.LastSeenTimestamp,
		&n.CreatedAt,
		&n.UpdatedAt,
		&n.DeletedAt,
		&n.IsValidator,
		&n.StakedAmount,
		&n.UnstakeRequestAt,
	)

	if err != nil {
		return nil, fmt.Errorf("runnerRepo.Register: %w", err)
	}

	return n, nil
}

// FindByPubkey retrieves a single active runner matching the targeted public key string.
func (r *runnerRepo) FindByPubkey(ctx context.Context, pubkey string) (*RunnerNode, error) {
	const q = `
        SELECT id, owner_email, owner_pubkey, node_pubkey, region, latitude, longitude,
               offchain_accumulated_tokens, total_earned_tokens_all_time,
               pending_solana_sync, last_seen_timestamp
        FROM runner_nodes WHERE owner_pubkey = $1 AND deleted_at IS NULL`

	n := &RunnerNode{}
err := r.pool.QueryRow(ctx, q, pubkey).
    Scan(&n.ID, &n.OwnerEmail, &n.OwnerPubkey, &n.NodePubkey, &n.Region, &n.Latitude, &n.Longitude,
        &n.OffchainAccumulatedTokens, &n.TotalEarnedTokensAllTime,
        &n.PendingSolanaSync, &n.LastSeenTimestamp)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.FindByPubkey: %w", err)
	}
	return n, nil
}
// FindByNodePDA retrieves a single active runner matching the targeted public key string.
func (r *runnerRepo) FindByNodePDA(ctx context.Context, nodePDA string) (*RunnerNode, error) {
	const q = `
        SELECT id, owner_email, owner_pubkey, node_pubkey, region, latitude, longitude,
               offchain_accumulated_tokens, total_earned_tokens_all_time,
               pending_solana_sync, last_seen_timestamp
        FROM runner_nodes WHERE node_pda = $1 AND deleted_at IS NULL`

	n := &RunnerNode{}
	err := r.pool.QueryRow(ctx, q, nodePDA).
		Scan(&n.ID, &n.OwnerEmail, &n.OwnerPubkey, &n.NodePubkey, &n.Region, &n.Latitude, &n.Longitude,
			&n.OffchainAccumulatedTokens, &n.TotalEarnedTokensAllTime, &n.NodePubkey,
			&n.PendingSolanaSync, &n.LastSeenTimestamp)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.FindByNodePDA: %w", err)
	}
	return n, nil
}
// FindByNodePubKey: Ensure this specifically queries the node_pubkey column.
func (r *runnerRepo) FindByNodePubKey(ctx context.Context, nodePubkey string) (*RunnerNode, error) {
    const q = `
        SELECT id, owner_email, owner_pubkey, node_pubkey, region, latitude, longitude,
               offchain_accumulated_tokens, total_earned_tokens_all_time,
               pending_solana_sync, last_seen_timestamp
        FROM runner_nodes WHERE node_pubkey = $1 AND deleted_at IS NULL`

    n := &RunnerNode{}
    // Use the passed argument 'nodePubkey'
    err := r.pool.QueryRow(ctx, q, nodePubkey).
        Scan(&n.ID, &n.OwnerEmail, &n.OwnerPubkey, &n.NodePubkey, &n.Region, &n.Latitude, &n.Longitude,
            &n.OffchainAccumulatedTokens, &n.TotalEarnedTokensAllTime,
            &n.PendingSolanaSync, &n.LastSeenTimestamp)
            
    if err != nil {
        // This will now report the correct function name in your logs
        return nil, fmt.Errorf("runnerRepo.FindByNodePubKey: %w", err)
    }
    return n, nil
}

// UpdateHeartbeat changes the timestamp of the last verified off-chain ping transmission.
func (r *runnerRepo) UpdateHeartbeat(ctx context.Context, nodePubkey string) error {
	const q = `UPDATE runner_nodes SET last_seen_timestamp = NOW() WHERE node_pubkey = $1`
	_, err := r.pool.Exec(ctx, q, nodePubkey)
	if err != nil {
		return fmt.Errorf("runnerRepo.UpdateHeartbeat: %w", err)
	}
	return nil
}

// AccumulateReward safely calculates off-chain balances or signals RabbitMQ loops via a database procedure.
func (r *runnerRepo) AccumulateReward(
	ctx context.Context,
	pubkey string,
	delta, threshold float64,
) (*AccumulateResult, error) {

	const q = `
		SELECT created, amount
		FROM create_payout_event_if_threshold($1, $2)
	`

	var res AccumulateResult

	err := r.pool.QueryRow(ctx, q, pubkey, threshold).Scan(
		&res.DidSync,
		&res.NewBalance,
	)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.AccumulateReward: %w", err)
	}

	return &res, nil
}

// SetPendingSync flags a worker as locking to prevent concurrent synchronization races.
func (r *runnerRepo) SetPendingSync(ctx context.Context, pubkey string, pending bool) error {
	const q = `UPDATE runner_nodes SET pending_solana_sync = $1 WHERE owner_pubkey = $2`
	_, err := r.pool.Exec(ctx, q, pending, pubkey)
	if err != nil {
		return fmt.Errorf("runnerRepo.SetPendingSync: %w", err)
	}
	return nil
}

// FindByEmailAndPubkey fulfills authentication context parameters matching web identity profiles.
func (r *runnerRepo) FindByEmailAndPubkey(ctx context.Context, email, pubkey string) ([]*RunnerNode, error) {
	const q = `
        SELECT id, owner_email, owner_pubkey, node_pubkey, region, latitude, longitude,
               offchain_accumulated_tokens, total_earned_tokens_all_time,
               pending_solana_sync, last_seen_timestamp
        FROM runner_nodes
        WHERE owner_email = $1 AND owner_pubkey = $2 AND deleted_at IS NULL
        ORDER BY created_at DESC`

	rows, err := r.pool.Query(ctx, q, email, pubkey)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.FindByEmailAndPubkey: %w", err)
	}
	defer rows.Close()

	var result []*RunnerNode
	for rows.Next() {
		n := &RunnerNode{}
		err := rows.Scan(&n.ID, &n.OwnerEmail, &n.OwnerPubkey, &n.NodePubkey, &n.Region, &n.Latitude, &n.Longitude,
			&n.OffchainAccumulatedTokens, &n.TotalEarnedTokensAllTime,
			&n.PendingSolanaSync, &n.LastSeenTimestamp)
		if err != nil {
			return nil, fmt.Errorf("runnerRepo.FindByEmailAndPubkey scan error: %w", err)
		}
		result = append(result, n)
	}
	return result, rows.Err()
}
