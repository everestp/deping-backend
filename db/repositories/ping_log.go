package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pingLogRepo struct {
	pool *pgxpool.Pool
}

// NewPingLogRepository instantiates the explicit concrete repository implementation.
func NewPingLogRepository(pool *pgxpool.Pool) PingLogRepository {
	return &pingLogRepo{pool: pool}
}

// BulkInsert uses pgx.CopyFrom — handles tens of thousands of rows/second efficiently.
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
			l.GeoRegion, l.Timestamp, l.Latitude, l.Longitude,l.TimestampMs,
		}
	}

	_, err := r.pool.CopyFrom(ctx,
		pgx.Identifier{"ping_logs"},
		[]string{
			"monitor_id", "runner_pubkey",
			"dns_us", "tcp_us", "tls_us", "ttfb_us", "total_us",
			"latency_ms", "status_code", "success", "error_kind",
			"geo_region", "timestamp", "latitude", "longitude","timestamp_ms",
		},
		pgx.CopyFromRows(rows),
	)

	if err != nil {
		return fmt.Errorf("pingLogRepo.BulkInsert: %w", err)
	}
	return nil
}

// FindByMonitor fetches chronological history logs for a single target monitor up to the requested limit.
func (r *pingLogRepo) FindByMonitor(ctx context.Context, monitorID string, limit int) ([]*PingLog, error) {
	const q = `
        SELECT id, monitor_id, runner_pubkey,
               dns_us, tcp_us, tls_us, ttfb_us, total_us,
               latency_ms, status_code, success, error_kind,
               geo_region, timestamp, latitude, longitude
        FROM ping_logs
        WHERE monitor_id = $1
        ORDER BY timestamp DESC
        LIMIT $2`

	rows, err := r.pool.Query(ctx, q, monitorID, limit)
	if err != nil {
		return nil, fmt.Errorf("pingLogRepo.FindByMonitor: %w", err)
	}
	defer rows.Close()

	var result []*PingLog
	for rows.Next() {
		l := &PingLog{}
		if err := rows.Scan(
			&l.ID, &l.MonitorID, &l.RunnerPubkey,
			&l.DnsUs, &l.TcpUs, &l.TlsUs, &l.TtfbUs, &l.TotalUs,
			&l.LatencyMs, &l.StatusCode, &l.Success, &l.ErrorKind,
			&l.GeoRegion, &l.Timestamp, &l.Latitude, &l.Longitude,
		); err != nil {
			return nil, fmt.Errorf("pingLogRepo.FindByMonitor scan error: %w", err)
		}
		result = append(result, l)
	}
	return result, rows.Err()
}

// UptimePercentage calculates the dynamic sliding window successful run ratio.
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

// AvgLatencyUs calculates the mathematical mean network trip runtime matching successful connection metrics.
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
