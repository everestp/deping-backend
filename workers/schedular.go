package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/services"
)

// JobPayload is what the Rust miner receives from job_queue.
type JobPayload struct {
	JobID        string `json:"job_id"`        // nonce — validated on result submission
	MonitorID    string `json:"monitor_id"`
	TargetURL    string `json:"target_url"`
	RunnerPubkey string `json:"runner_pubkey"` // Added: Explicitly targets a specific streaming node
	IssuedAt     int64  `json:"issued_at"`
	ExpiresAt    int64  `json:"expires_at"`    // Unix — nonce invalid after this
}

// StartScheduler runs the Redis-backed scheduler in the background.
// We pass the active MemoryRegistry and SmartScheduler instances into it.
func StartScheduler(
	ctx context.Context,
	rdb *redis.Client,
	pool *pgxpool.Pool,
	rabbitCh *amqp.Channel,
	reg *services.MemoryRegistry,
	sched *services.SmartScheduler,
) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		log.Println("[scheduler] started with dynamic DePIN balancing")

		for {
			select {
			case <-ctx.Done():
				log.Println("[scheduler] shutting down")
				return
			case <-ticker.C:
				if err := scheduleTick(ctx, rdb, pool, rabbitCh, reg, sched); err != nil {
					log.Printf("[scheduler] tick error: %v", err)
				}
			}
		}
	}()
}

func scheduleTick(
	ctx context.Context,
	rdb *redis.Client,
	pool *pgxpool.Pool,
	rabbitCh *amqp.Channel,
	reg *services.MemoryRegistry,
	sched *services.SmartScheduler,
) error {
	now := time.Now().Unix()

	// 1. Get due members (using your ZSET logic)
	dueMembers, err := rdb.ZRangeByScore(ctx, "scheduler:due", &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now), Offset: 0, Count: 100,
	}).Result()
	if err != nil || len(dueMembers) == 0 {
		return nil
	}

	// 2. Map members to Monitor IDs
    monitorIDs := make([]string, 0, len(dueMembers))
    for _, m := range dueMembers {
        // Change "monitor:" to "sched:monitor:" to match your error logs
        id := strings.TrimPrefix(m, "sched:monitor:")
        monitorIDs = append(monitorIDs, id)
    }

	// 3. Batch fetch monitor details
	repo := repositories.NewStorage(pool) // Ensure you have access to this
	monitors, err := repo.Monitors.FindMany(ctx, monitorIDs)
	if err != nil {
		return err
	}

	// 4. Handle "Ghost" monitors: If a monitor was in ZSET but not in DB, remove it
	foundIDs := make(map[string]bool)
	for _, m := range monitors {
		foundIDs[m.ID] = true
	}
for _, m := range dueMembers {
        // Change "monitor:" to "sched:monitor:" here too
        id := strings.TrimPrefix(m, "sched:monitor:")
        if !foundIDs[id] {
            rdb.ZRem(ctx, "scheduler:due", m)
        }
    }

	// 5. Batch Dispatch
	assignments := sched.MatchBatch(monitors)
	for pubkey, m := range assignments {
		// Calculate next run time
		nextDue := float64(now + int64(m.CheckIntervalSeconds))

		// Update ZSET immediately to prevent duplicate scheduling if this tick is slow
		rdb.ZAdd(ctx, "scheduler:due", redis.Z{
    Score: nextDue,
    Member: "sched:monitor:" + m.ID, // Match the prefix!
})

		// 6. Build and publish payload
		jobID := fmt.Sprintf("%s:%s:%d", m.ID, pubkey, now)
		rdb.Set(ctx, "nonce:"+jobID, "1", 15*time.Minute)

		payload := JobPayload{
			JobID: jobID, MonitorID: m.ID, TargetURL: m.TargetURL,
			RunnerPubkey: pubkey, IssuedAt: now,
			ExpiresAt: now + int64(m.CheckIntervalSeconds*2),
		}
		body, _ := json.Marshal(payload)

		_ = rabbitCh.PublishWithContext(ctx, "", "job_queue", false, false, amqp.Publishing{
			ContentType: "application/json", Body: body, DeliveryMode: amqp.Persistent,
		})
	}

	return nil
}
type monitorCacheEntry struct {
	ID              string `json:"id"`
	TargetURL       string `json:"target_url"`
	IntervalSeconds int    `json:"interval_seconds"`
}

// func cachedActiveMonitors(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool) ([]*monitorCacheEntry, error) {
// 	const cacheKey = "cache:active_monitors"

// 	raw, err := rdb.Get(ctx, cacheKey).Bytes()
// 	if err == nil {
// 		var cached []*monitorCacheEntry
// 		if jsonErr := json.Unmarshal(raw, &cached); jsonErr == nil {
// 			return cached, nil
// 		}
// 	}

// 	rows, err := pool.Query(ctx,
// 		`SELECT id, target_url, check_interval_seconds
//          FROM monitors
//          WHERE is_active = TRUE AND deleted_at IS NULL AND credit_balance_checks > 0`)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	var monitors []*monitorCacheEntry
// 	for rows.Next() {
// 		m := &monitorCacheEntry{}
// 		if err := rows.Scan(&m.ID, &m.TargetURL, &m.IntervalSeconds); err != nil {
// 			return nil, err
// 		}
// 		monitors = append(monitors, m)
// 	}

// 	if body, err := json.Marshal(monitors); err == nil {
// 		rdb.Set(ctx, cacheKey, body, 30*time.Second)
// 	}
// 	return monitors, rows.Err()
// }
