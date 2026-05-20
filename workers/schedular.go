package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// JobPayload is what the Rust miner receives from job_queue.
type JobPayload struct {
	JobID     string `json:"job_id"`      // nonce — validated on result submission
	MonitorID string `json:"monitor_id"`
	TargetURL string `json:"target_url"`
	Region    string `json:"region"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"` // Unix — nonce invalid after this
}

// StartScheduler runs the Redis-backed scheduler in the background.
// It caches the active monitor list with a 30s TTL to avoid hammering Postgres.
func StartScheduler(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool, rabbitCh *amqp.Channel) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		log.Println("[scheduler] started")

		for {
			select {
			case <-ctx.Done():
				log.Println("[scheduler] shutting down")
				return
			case <-ticker.C:
				if err := scheduleTick(ctx, rdb, pool, rabbitCh); err != nil {
					log.Printf("[scheduler] tick error: %v", err)
				}
			}
		}
	}()
}

func scheduleTick(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool, rabbitCh *amqp.Channel) error {
	monitors, err := cachedActiveMonitors(ctx, rdb, pool)
	if err != nil {
		return fmt.Errorf("get active monitors: %w", err)
	}

	now := time.Now().Unix()

	for _, m := range monitors {
		// Redis ZSET stores next_due timestamp scored by Unix time.
		// Pop monitors whose score (next_due) <= now.
		key := fmt.Sprintf("sched:monitor:%s", m.ID)
		score, err := rdb.ZScore(ctx, "scheduler:due", key).Result()
		if err != nil || int64(score) > now {
			continue
		}

		// Dispatch one job per active region. Lock prevents duplicate dispatch.
		for _, region := range []string{"us-east", "eu-west", "ap-south"} {
			lockKey := fmt.Sprintf("lock:monitor:%s:%s", m.ID, region)
			set, err := rdb.SetNX(ctx, lockKey, 1, time.Duration(m.IntervalSeconds)*time.Second).Result()
			if err != nil || !set {
				continue // already dispatched this region this interval
			}

			jobID := fmt.Sprintf("%s:%s:%d", m.ID, region, now)
			// Store nonce with 2x interval TTL so Rust miner has time to respond
			nonceKey := fmt.Sprintf("nonce:%s", jobID)
			rdb.Set(ctx, nonceKey, 1, time.Duration(m.IntervalSeconds*2)*time.Second)

			payload := JobPayload{
				JobID:     jobID,
				MonitorID: m.ID,
				TargetURL: m.TargetURL,
				Region:    region,
				IssuedAt:  now,
				ExpiresAt: now + int64(m.IntervalSeconds*2),
			}
			body, _ := json.Marshal(payload)

			_ = rabbitCh.PublishWithContext(ctx, "", "job_queue", false, false,
				amqp.Publishing{
					ContentType:  "application/json",
					Body:         body,
					DeliveryMode: amqp.Persistent,
				},
			)
		}

		// Re-schedule: update ZSET score to next_due
		nextDue := float64(now + int64(m.IntervalSeconds))
		rdb.ZAdd(ctx, "scheduler:due", redis.Z{Score: nextDue, Member: key})
	}
	return nil
}

type monitorCacheEntry struct {
	ID              string `json:"id"`
	TargetURL       string `json:"target_url"`
	IntervalSeconds int    `json:"interval_seconds"`
}

// cachedActiveMonitors reads the monitor list from Redis (30s TTL).
// On miss it queries Postgres and repopulates the cache.
func cachedActiveMonitors(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool) ([]*monitorCacheEntry, error) {
	const cacheKey = "cache:active_monitors"

	raw, err := rdb.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var cached []*monitorCacheEntry
		if jsonErr := json.Unmarshal(raw, &cached); jsonErr == nil {
			return cached, nil
		}
	}

	// Cache miss — query DB
	rows, err := pool.Query(ctx,
		`SELECT id, target_url, check_interval_seconds
		 FROM monitors
		 WHERE is_active = TRUE AND deleted_at IS NULL AND credit_balance_checks > 0`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var monitors []*monitorCacheEntry
	for rows.Next() {
		m := &monitorCacheEntry{}
		if err := rows.Scan(&m.ID, &m.TargetURL, &m.IntervalSeconds); err != nil {
			return nil, err
		}
		monitors = append(monitors, m)
	}

	if body, err := json.Marshal(monitors); err == nil {
		rdb.Set(ctx, cacheKey, body, 30*time.Second)
	}
	return monitors, rows.Err()
}
