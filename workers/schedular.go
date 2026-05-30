package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/everestp/depin-backend/db/repositories"
	
	"github.com/everestp/depin-backend/services"
)

// JobPayload is what the Rust miner receives from job_queue.
type JobPayload struct {
    JobID        string `json:"job_id"`
    MonitorID    string `json:"monitor_id"`
    TargetURL    string `json:"target_url"`
    RunnerPubkey string `json:"runner_pubkey"`
    TaskNonce    string `json:"task_nonce"` // 👈 REQUIRED: The dynamic proof-of-work secret
    IssuedAt     int64  `json:"issued_at"`
    ExpiresAt    int64  `json:"expires_at"`
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

    // 1. Get due members
    dueMembers, err := rdb.ZRangeByScore(ctx, "scheduler:due", &redis.ZRangeBy{
        Min: "-inf", Max: fmt.Sprintf("%d", now), Offset: 0, Count: 100,
    }).Result()
    if err != nil || len(dueMembers) == 0 {
        return nil
    }

    // 2. Identify Monitor IDs
    monitorIDs := make([]string, 0, len(dueMembers))
    for _, m := range dueMembers {
        monitorIDs = append(monitorIDs, strings.TrimPrefix(m, "sched:monitor:"))
    }

    // 3. Fetch monitors
    repo := repositories.NewStorage(pool)
    monitors, err := repo.Monitors.FindMany(ctx, monitorIDs)
    if err != nil {
        return err
    }

    // 4. Cleanup Ghost Monitors
    foundMap := make(map[string]any) // Map for quick access
    for _, m := range monitors {
        foundMap[m.ID] = m
    }

    for _, mKey := range dueMembers {
        id := strings.TrimPrefix(mKey, "sched:monitor:")
        if _, exists := foundMap[id]; !exists {
            rdb.ZRem(ctx, "scheduler:due", mKey)
        }
    }

    // 5. Batch Dispatch
    assignments := sched.MatchBatch(monitors)

    // Track which monitors were actually processed so we can reschedule them
    processedIDs := make(map[string]bool)

    for pubkey, m := range assignments {
        processedIDs[m.ID] = true

        // Calculate next run time based on the interval
        nextDue := float64(now + int64(m.CheckIntervalSeconds))

        // Update ZSET: This sets the new time for the next check
        rdb.ZAdd(ctx, "scheduler:due", redis.Z{
            Score:  nextDue,
            Member: "sched:monitor:" + m.ID,
        })

        // 6. Nonce and Publish
        taskNonce := uuid.New().String()
        jobID := fmt.Sprintf("%s:%s:%d", m.ID, pubkey, now)

        rdb.Set(ctx, "task_nonce:"+taskNonce, "1", 15*time.Minute)

        payload := JobPayload{
            JobID:        jobID,
            MonitorID:    m.ID,
            TargetURL:    m.TargetURL,
            RunnerPubkey: pubkey,
            TaskNonce:    taskNonce,
            IssuedAt:     now,
            ExpiresAt:    now + int64(m.CheckIntervalSeconds*2),
        }

        body, _ := json.Marshal(payload)
        rabbitCh.PublishWithContext(ctx, "", "job_queue", false, false, amqp.Publishing{
            ContentType: "application/json", Body: body, DeliveryMode: amqp.Persistent,
        })
    }

    // 7. Critical: Reschedule monitors that were "Due" but not assigned in this batch
    // This prevents monitors from getting stuck if the scheduler skips them
    for _, m := range monitors {
        if !processedIDs[m.ID] {
            rdb.ZAdd(ctx, "scheduler:due", redis.Z{
                Score:  float64(now + int64(m.CheckIntervalSeconds)),
                Member: "sched:monitor:" + m.ID,
            })
        }
    }

    return nil
}























// type monitorCacheEntry struct {
// 	ID              string `json:"id"`
// 	TargetURL       string `json:"target_url"`
// 	IntervalSeconds int    `json:"interval_seconds"`
// }

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
