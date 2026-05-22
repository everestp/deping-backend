package repositories

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type monitorRepo struct {
	pool *pgxpool.Pool
}

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
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

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
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}
func (r *monitorRepo) FindMany(ctx context.Context, ids []string) ([]*Monitor, error) {
    if len(ids) == 0 {
        return nil, nil
    }

    // Using ANY($1) is the most efficient way to query a slice of IDs in Postgres
    const q = `
        SELECT id, owner_id, target_url, check_interval_seconds,
               credit_balance_checks, total_spent_tokens, is_active, created_at
        FROM monitors
        WHERE id = ANY($1) AND is_active = TRUE AND deleted_at IS NULL`

    rows, err := r.pool.Query(ctx, q, ids)
    if err != nil {
        return nil, fmt.Errorf("monitorRepo.FindMany: %w", err)
    }
    defer rows.Close()

    var result []*Monitor
    for rows.Next() {
        m := &Monitor{}
        if err := rows.Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
            &m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt); err != nil {
            return nil, err
        }
        result = append(result, m)
    }
    return result, rows.Err()
}

func (r *monitorRepo) UpdateActive(ctx context.Context, id string, isActive bool) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE monitors SET is_active = $1 WHERE id = $2 AND deleted_at IS NULL`,
		isActive, id)
	return err
}


func (r *monitorRepo) DeductCredit(ctx context.Context, id string, tokenCost float64) error {
    _, err := r.pool.Exec(ctx,
        `UPDATE monitors SET credit_balance_checks = credit_balance_checks - 1,
         total_spent_tokens = total_spent_tokens + $1
         WHERE id = $2 AND credit_balance_checks > 0 AND deleted_at IS NULL`,
        tokenCost, id)
    return err
}

func (r *monitorRepo) Delete(ctx context.Context, id string, ownerID int) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE monitors SET deleted_at = NOW() WHERE id = $1 AND owner_id = $2`,
		id, ownerID)
	return err
}
