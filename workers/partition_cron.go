package workers

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// StartPartitionCron ensures ping_logs weekly partitions always exist 4 weeks ahead.
// Runs immediately on startup, then every Monday at 00:05 UTC.
// Prevents the "insert fails because partition doesn't exist" production incident.
func StartPartitionCron(ctx context.Context, pool *pgxpool.Pool) {
	go func() {
		// Run immediately on startup to cover any missed weeks
		if err := ensurePartitions(ctx, pool, 4); err != nil {
			log.Printf("[partition-cron] startup run error: %v", err)
		}

		for {
			next := nextMonday()
			log.Printf("[partition-cron] next run at %s", next.Format(time.RFC3339))

			select {
			case <-ctx.Done():
				log.Println("[partition-cron] shutting down")
				return
			case <-time.After(time.Until(next)):
				if err := ensurePartitions(ctx, pool, 4); err != nil {
					log.Printf("[partition-cron] error: %v", err)
				}
			}
		}
	}()
}

// ensurePartitions creates the next `weeksAhead` weekly partitions if they don't exist.
func ensurePartitions(ctx context.Context, pool *pgxpool.Pool, weeksAhead int) error {
	now := time.Now().UTC()
	// Find the Monday of the current week
	weekday := int(now.Weekday())
	if weekday == 0 {
		weekday = 7 // treat Sunday as 7
	}
	monday := now.AddDate(0, 0, -(weekday - 1)).Truncate(24 * time.Hour)

	for i := 0; i < weeksAhead; i++ {
		from := monday.AddDate(0, 0, i*7)
		to := from.AddDate(0, 0, 7)

		year, week := from.ISOWeek()
		tableName := fmt.Sprintf("ping_logs_%d_w%02d", year, week)

		createSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s
			PARTITION OF ping_logs
			FOR VALUES FROM ('%s') TO ('%s')`,
			tableName,
			from.Format("2006-01-02"),
			to.Format("2006-01-02"),
		)

		if _, err := pool.Exec(ctx, createSQL); err != nil {
			return fmt.Errorf("create partition %s: %w", tableName, err)
		}

		// Create index on the partition — critical for read performance
		indexSQL := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_%s_monitor_ts ON %s (monitor_id, timestamp DESC)`,
			tableName, tableName,
		)
		if _, err := pool.Exec(ctx, indexSQL); err != nil {
			return fmt.Errorf("create index on %s: %w", tableName, err)
		}

		log.Printf("[partition-cron] ensured partition %s (%s → %s)",
			tableName, from.Format("2006-01-02"), to.Format("2006-01-02"))
	}
	return nil
}

// nextMonday returns the next Monday at 00:05 UTC.
func nextMonday() time.Time {
	now := time.Now().UTC()
	weekday := int(now.Weekday())
	if weekday == 0 {
		weekday = 7
	}
	daysUntilMonday := (8 - weekday) % 7
	if daysUntilMonday == 0 {
		daysUntilMonday = 7
	}
	next := now.AddDate(0, 0, daysUntilMonday).Truncate(24 * time.Hour).Add(5 * time.Minute)
	return next
}
