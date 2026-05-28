package repositories

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type monitorRepo struct {
	pool *pgxpool.Pool
}

// NewMonitorRepository instantiates the explicit concrete repository implementation.
func NewMonitorRepository(pool *pgxpool.Pool) MonitorRepository {
	return &monitorRepo{pool: pool}
}

// Create inserts a new configuration target monitor tracking entry block.
func (r *monitorRepo) Create(ctx context.Context, ownerID int, targetURL string, intervalSeconds int) (*Monitor, error) {
	const q = `
        INSERT INTO monitors (owner_id, target_url, check_interval_seconds)
        VALUES ($1, $2, $3)
        RETURNING id, owner_id, target_url, check_interval_seconds, credit_balance_checks, total_spent_tokens, is_active, created_at`

	m := &Monitor{}
	err := r.pool.QueryRow(ctx, q, ownerID, targetURL, intervalSeconds).
		Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("monitorRepo.Create: %w", err)
	}
	return m, nil
}

// FindByOwner pulls a slice of history contexts matching a target web customer profile identity.
func (r *monitorRepo) FindByOwner(ctx context.Context, ownerID int) ([]*Monitor, error) {
	const q = `
        SELECT id, owner_id, target_url, check_interval_seconds,
               credit_balance_checks, total_spent_tokens, is_active, created_at
        FROM monitors
        WHERE owner_id = $1 AND deleted_at IS NULL
        ORDER BY created_at DESC`

	rows, err := r.pool.Query(ctx, q, ownerID)
	if err != nil {
		return nil, fmt.Errorf("monitorRepo.FindByOwner: %w", err)
	}
	defer rows.Close()

	var result []*Monitor
	for rows.Next() {
		m := &Monitor{}
		if err := rows.Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("monitorRepo.FindByOwner scan error: %w", err)
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// FindActive returns all tracking jobs currently scheduled for runtime validation.
func (r *monitorRepo) FindActive(ctx context.Context) ([]*Monitor, error) {
	const q = `
        SELECT id, owner_id, target_url, check_interval_seconds,
               credit_balance_checks, total_spent_tokens, is_active, created_at
        FROM monitors
        WHERE is_active = TRUE AND deleted_at IS NULL AND credit_balance_checks > 0`

	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("monitorRepo.FindActive: %w", err)
	}
	defer rows.Close()

	var result []*Monitor
	for rows.Next() {
		m := &Monitor{}
		if err := rows.Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("monitorRepo.FindActive scan error: %w", err)
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// FindByJobID extracts the target monitor ID prefix sequence from dynamic string keys.
func (r *monitorRepo) FindByJobID(ctx context.Context, jobID string) (*Monitor, error) {
	parts := strings.Split(jobID, ":")
	if len(parts) == 0 || parts[0] == "" {
		return nil, fmt.Errorf("monitorRepo.FindByJobID: malformed job_id %q", jobID)
	}
	return r.FindByID(ctx, parts[0])
}

// FindByMonitorID resolves directly through proxy reference redirects.
func (r *monitorRepo) FindByMonitorID(ctx context.Context, monitorId string) (*Monitor, error) {
	return r.FindByID(ctx, monitorId)
}

// FindByID returns a single clean entry matching a designated tracking record ID.
func (r *monitorRepo) FindByID(ctx context.Context, id string) (*Monitor, error) {
	const q = `
        SELECT id, owner_id, target_url, check_interval_seconds,
               credit_balance_checks, total_spent_tokens, is_active, created_at
        FROM monitors WHERE id = $1 AND deleted_at IS NULL`

	m := &Monitor{}
	err := r.pool.QueryRow(ctx, q, id).
		Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("monitorRepo.FindByID: %w", err)
	}
	return m, nil
}

// FindMany extracts multiple active items safely using native positional multi-argument strings.
func (r *monitorRepo) FindMany(ctx context.Context, ids []string) ([]*Monitor, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	q := fmt.Sprintf(`
        SELECT id, owner_id, target_url, check_interval_seconds,
               credit_balance_checks, total_spent_tokens, is_active, created_at
        FROM monitors
        WHERE id IN (%s) AND is_active = TRUE AND deleted_at IS NULL`, strings.Join(placeholders, ","))

	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("monitorRepo.FindMany: %w", err)
	}
	defer rows.Close()

	var result []*Monitor
	for rows.Next() {
		m := &Monitor{}
		if err := rows.Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt); err != nil {
			return nil, fmt.Errorf("monitorRepo.FindMany scan error: %w", err)
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// UpdateActive changes runtime tracking authorization rules.
func (r *monitorRepo) UpdateActive(ctx context.Context, id string, isActive bool) error {
	const q = `UPDATE monitors SET is_active = $1 WHERE id = $2 AND deleted_at IS NULL`
	_, err := r.pool.Exec(ctx, q, isActive, id)
	if err != nil {
		return fmt.Errorf("monitorRepo.UpdateActive: %w", err)
	}
	return nil
}

// DeductCredit handles standalone non-atomic credit balances adjustments.
func (r *monitorRepo) DeductCredit(ctx context.Context, id string, tokenCost float64) error {
	const q = `
        UPDATE monitors SET credit_balance_checks = credit_balance_checks - 1,
         total_spent_tokens = total_spent_tokens + $1
         WHERE id = $2 AND credit_balance_checks > 0 AND deleted_at IS NULL`

	_, err := r.pool.Exec(ctx, q, tokenCost, id)
	if err != nil {
		return fmt.Errorf("monitorRepo.DeductCredit: %w", err)
	}
	return nil
}

// Delete processes a soft deletion update tag to preserve analytical relational metrics history.
func (r *monitorRepo) Delete(ctx context.Context, id string, ownerID int) error {
	const q = `UPDATE monitors SET deleted_at = NOW() WHERE id = $1 AND owner_id = $2`
	_, err := r.pool.Exec(ctx, q, id, ownerID)
	if err != nil {
		return fmt.Errorf("monitorRepo.Delete: %w", err)
	}
	return nil
}
