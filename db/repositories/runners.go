package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ── Runner ─────────────────────────────────────────────────────────────────

type runnerRepo struct{ pool *pgxpool.Pool }

func (r *runnerRepo) Register(ctx context.Context, ownerEmail, ownerPubkey, region string, lat, lng float64) (*RunnerNode, error) {
	const q = `
		INSERT INTO runner_nodes (owner_email, owner_pubkey, region, latitude, longitude)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (owner_pubkey) DO UPDATE
		  SET last_seen_timestamp = NOW(), region = EXCLUDED.region,
		      latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
		RETURNING id, owner_email, owner_pubkey, region, latitude, longitude,
		          offchain_accumulated_tokens, total_earned_tokens_all_time,
		          pending_solana_sync, last_seen_timestamp`
	n := &RunnerNode{}
	err := r.pool.QueryRow(ctx, q, ownerEmail, ownerPubkey, region, lat, lng).
		Scan(&n.ID, &n.OwnerEmail, &n.OwnerPubkey, &n.Region, &n.Latitude, &n.Longitude,
			&n.OffchainAccumulatedTokens, &n.TotalEarnedTokensAllTime,
			&n.PendingSolanaSync, &n.LastSeenTimestamp)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.Register: %w", err)
	}
	return n, nil
}

func (r *runnerRepo) FindByPubkey(ctx context.Context, pubkey string) (*RunnerNode, error) {
	const q = `
		SELECT id, owner_email, owner_pubkey, region, latitude, longitude,
		       offchain_accumulated_tokens, total_earned_tokens_all_time,
		       pending_solana_sync, last_seen_timestamp
		FROM runner_nodes WHERE owner_pubkey = $1 AND deleted_at IS NULL`
	n := &RunnerNode{}
	err := r.pool.QueryRow(ctx, q, pubkey).
		Scan(&n.ID, &n.OwnerEmail, &n.OwnerPubkey, &n.Region, &n.Latitude, &n.Longitude,
			&n.OffchainAccumulatedTokens, &n.TotalEarnedTokensAllTime,
			&n.PendingSolanaSync, &n.LastSeenTimestamp)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.FindByPubkey: %w", err)
	}
	return n, nil
}

func (r *runnerRepo) UpdateHeartbeat(ctx context.Context, pubkey string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE runner_nodes SET last_seen_timestamp = NOW() WHERE owner_pubkey = $1`, pubkey)
	return err
}

func (r *runnerRepo) AccumulateReward(ctx context.Context, pubkey string, delta, threshold float64) (*AccumulateResult, error) {
	res := &AccumulateResult{}
	err := r.pool.QueryRow(ctx,
		`SELECT new_balance, did_sync FROM accumulate_runner_reward($1, $2, $3)`,
		pubkey, delta, threshold).Scan(&res.NewBalance, &res.DidSync)
	if err != nil {
		return nil, fmt.Errorf("runnerRepo.AccumulateReward: %w", err)
	}
	return res, nil
}

func (r *runnerRepo) SetPendingSync(ctx context.Context, pubkey string, pending bool) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE runner_nodes SET pending_solana_sync = $1 WHERE owner_pubkey = $2`, pending, pubkey)
	return err
}

// ── PingLog ────────────────────────────────────────────────────────────────

type pingLogRepo struct{ pool *pgxpool.Pool }

// BulkInsert uses pgx.CopyFrom — handles tens of thousands of rows/second.
// Stores all ProbeResult phase latencies in microseconds.
func (r *pingLogRepo) BulkInsert(ctx context.Context, logs []*PingLog) error {
	if len(logs) == 0 {
		return nil
	}
	rows := make([][]any, len(logs))
	for i, l := range logs {
		rows[i] = []any{
			l.MonitorID, l.RunnerPubkey,
			l.DnsUs, l.TcpUs, l.TlsUs, l.TtfbUs, l.TotalUs,
			l.LatencyMs, l.StatusCode, l.Success, l.ErrorKind,
			l.GeoRegion, l.Timestamp,
		}
	}
	_, err := r.pool.CopyFrom(ctx,
		pgx.Identifier{"ping_logs"},
		[]string{
			"monitor_id", "runner_pubkey",
			"dns_us", "tcp_us", "tls_us", "ttfb_us", "total_us",
			"latency_ms", "status_code", "success", "error_kind",
			"geo_region", "timestamp",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf("pingLogRepo.BulkInsert: %w", err)
	}
	return nil
}

func (r *pingLogRepo) FindByMonitor(ctx context.Context, monitorID string, limit int) ([]*PingLog, error) {
	const q = `
		SELECT id, monitor_id, runner_pubkey,
		       dns_us, tcp_us, tls_us, ttfb_us, total_us,
		       latency_ms, status_code, success, error_kind, geo_region, timestamp
		FROM ping_logs WHERE monitor_id = $1 ORDER BY timestamp DESC LIMIT $2`
	rows, err := r.pool.Query(ctx, q, monitorID, limit)
	if err != nil {
		return nil, fmt.Errorf("pingLogRepo.FindByMonitor: %w", err)
	}
	defer rows.Close()
	var result []*PingLog
	for rows.Next() {
		l := &PingLog{}
		if err := rows.Scan(&l.ID, &l.MonitorID, &l.RunnerPubkey,
			&l.DnsUs, &l.TcpUs, &l.TlsUs, &l.TtfbUs, &l.TotalUs,
			&l.LatencyMs, &l.StatusCode, &l.Success, &l.ErrorKind,
			&l.GeoRegion, &l.Timestamp); err != nil {
			return nil, err
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

func (r *pingLogRepo) UptimePercentage(ctx context.Context, monitorID string, since time.Time) (float64, error) {
	const q = `
		SELECT COALESCE(
		  100.0 * SUM(CASE WHEN success THEN 1 ELSE 0 END)::float / NULLIF(COUNT(*), 0),
		0) FROM ping_logs WHERE monitor_id = $1 AND timestamp >= $2`
	var pct float64
	if err := r.pool.QueryRow(ctx, q, monitorID, since).Scan(&pct); err != nil {
		return 0, fmt.Errorf("pingLogRepo.UptimePercentage: %w", err)
	}
	return pct, nil
}

func (r *pingLogRepo) AvgLatencyUs(ctx context.Context, monitorID string, since time.Time) (uint64, error) {
	const q = `
		SELECT COALESCE(AVG(total_us)::bigint, 0)
		FROM ping_logs WHERE monitor_id = $1 AND timestamp >= $2 AND success = TRUE`
	var avg uint64
	if err := r.pool.QueryRow(ctx, q, monitorID, since).Scan(&avg); err != nil {
		return 0, fmt.Errorf("pingLogRepo.AvgLatencyUs: %w", err)
	}
	return avg, nil
}

// ── Monitor (add FindByJobID) ──────────────────────────────────────────────

// FindByJobID resolves a job_id (stored as a Redis nonce key) to a monitor.
// The result processor calls this to populate monitor_id on ProbeResult packets.
// job_id format: "<monitor_uuid>:<region>:<unix_ts>" (set by scheduler.go).
func (r *monitorRepo) FindByJobID(ctx context.Context, jobID string) (*Monitor, error) {
	// Extract monitor UUID from job_id prefix
	// job_id = "<monitor_id>:<region>:<ts>"
	for i, c := range jobID {
		if c == ':' {
			monitorID := jobID[:i]
			return r.findByID(ctx, monitorID)
		}
	}
	return nil, fmt.Errorf("monitorRepo.FindByJobID: malformed job_id %q", jobID)
}

func (r *monitorRepo) findByID(ctx context.Context, id string) (*Monitor, error) {
	const q = `
		SELECT id, owner_id, target_url, check_interval_seconds,
		       credit_balance_checks, total_spent_tokens, is_active, created_at
		FROM monitors WHERE id = $1 AND deleted_at IS NULL`
	m := &Monitor{}
	err := r.pool.QueryRow(ctx, q, id).
		Scan(&m.ID, &m.OwnerID, &m.TargetURL, &m.CheckIntervalSeconds,
			&m.CreditBalanceChecks, &m.TotalSpentTokens, &m.IsActive, &m.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("monitorRepo.findByID: %w", err)
	}
	return m, nil
}

// ── SolanaSync ─────────────────────────────────────────────────────────────

type solanaSyncRepo struct{ pool *pgxpool.Pool }

func (r *solanaSyncRepo) RecordSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO solana_sync_events (runner_pubkey, tx_signature, amount_raw) VALUES ($1, $2, $3)`,
		runnerPubkey, txSignature, amountRaw)
	return err
}

func (r *solanaSyncRepo) ExistsBySignature(ctx context.Context, txSignature string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM solana_sync_events WHERE tx_signature = $1)`,
		txSignature).Scan(&exists)
	return exists, err
}
