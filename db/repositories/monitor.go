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

func NewMonitorRepository(pool *pgxpool.Pool) MonitorRepository {
	return &monitorRepo{pool: pool}
}

func (r *monitorRepo) Create(ctx context.Context, ownerID int, targetURL string, intervalSeconds int) (*Monitor, error) {
	const q = `
		INSERT INTO monitors (owner_id, target_url, check_interval_seconds)
		VALUES ($1, $2, $3)
		RETURNING id, owner_id, target_url, check_interval_seconds, total_spent_tokens, is_active, created_at`

	m := &Monitor{}
	err := r.pool.QueryRow(ctx, q, ownerID, targetURL, intervalSeconds).
		Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.TotalSpentTokens, &m.IsActive, &m.CreatedAt)
	return m, err
}

func (r *monitorRepo) FindByOwner(ctx context.Context, ownerID int) ([]*Monitor, error) {
	const q = `SELECT id, owner_id, target_url, check_interval_seconds, total_spent_tokens, is_active, created_at 
               FROM monitors WHERE owner_id = $1 AND deleted_at IS NULL ORDER BY created_at DESC`
	return r.queryMultiple(ctx, q, ownerID)
}

func (r *monitorRepo) FindActive(ctx context.Context) ([]*Monitor, error) {
	const q = `SELECT m.id, m.owner_id, m.target_url, m.check_interval_seconds, m.total_spent_tokens, m.is_active, m.created_at
		FROM monitors m JOIN users u ON m.owner_id = u.id
		WHERE m.is_active = TRUE AND m.deleted_at IS NULL AND u.credit_balance > 0`
	return r.queryMultiple(ctx, q)
}

func (r *monitorRepo) FindMany(ctx context.Context, ids []string) ([]*Monitor, error) {
    if len(ids) == 0 { return nil, nil }

    placeholders := make([]string, len(ids))
    args := make([]any, len(ids))
    for i, id := range ids {
        placeholders[i] = fmt.Sprintf("$%d", i+1)
        args[i] = id
    }

    // Now JOIN users to filter by credit_balance
    q := fmt.Sprintf(`
        SELECT m.id, m.owner_id, m.target_url, m.check_interval_seconds, m.total_spent_tokens, m.is_active, m.created_at 
        FROM monitors m
        JOIN users u ON m.owner_id = u.id
        WHERE m.id IN (%s) 
          AND m.is_active = TRUE 
          AND m.deleted_at IS NULL 
          AND u.credit_balance > 0`, strings.Join(placeholders, ","))

    return r.queryMultiple(ctx, q, args...)
}

func (r *monitorRepo) FindByMonitorID(ctx context.Context, id string) (*Monitor, error) {
	return r.FindByID(ctx, id)
}

func (r *monitorRepo) FindByID(ctx context.Context, id string) (*Monitor, error) {
	const q = `SELECT id, owner_id, target_url, check_interval_seconds, total_spent_tokens, is_active, created_at 
               FROM monitors WHERE id = $1 AND deleted_at IS NULL`
	m := &Monitor{}
	err := r.pool.QueryRow(ctx, q, id).
		Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt)
	return m, err
}

func (r *monitorRepo) UpdateActive(ctx context.Context, id string, isActive bool) error {
	_, err := r.pool.Exec(ctx, `UPDATE monitors SET is_active = $1 WHERE id = $2 AND deleted_at IS NULL`, isActive, id)
	return err
}

func (r *monitorRepo) DeductCredit(ctx context.Context, id string, tokenCost float64) error {
	_, err := r.pool.Exec(ctx, `UPDATE monitors SET total_spent_tokens = total_spent_tokens + $1 WHERE id = $2`, tokenCost, id)
	return err
}

func (r *monitorRepo) Delete(ctx context.Context, id string, ownerID int) error {
	_, err := r.pool.Exec(ctx, `UPDATE monitors SET deleted_at = NOW() WHERE id = $1 AND owner_id = $2`, id, ownerID)
	return err
}

func (r *monitorRepo) queryMultiple(ctx context.Context, q string, args ...any) ([]*Monitor, error) {
	rows, err := r.pool.Query(ctx, q, args...)
	if err != nil { return nil, err }
	defer rows.Close()

	var result []*Monitor
	for rows.Next() {
		m := &Monitor{}
		if err := rows.Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}