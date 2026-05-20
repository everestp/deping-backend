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
	// 1. Fetch monitor entries due for updates from Redis/Postgres cache
	cachedMonitors, err := cachedActiveMonitors(ctx, rdb, pool)
	if err != nil {
		return fmt.Errorf("get active monitors: %w", err)
	}

	now := time.Now().Unix()
	var dueMonitors []*repositories.Monitor

	// Filter monitors that are currently due according to their ZSET timer score
	for _, cm := range cachedMonitors {
		key := fmt.Sprintf("sched:monitor:%s", cm.ID)
		score, err := rdb.ZScore(ctx, "scheduler:due", key).Result()
		if err != nil || int64(score) > now {
			continue
		}

		// Map cache entries into domain-layer entity structures expected by the MatchBatch engine
		dueMonitors = append(dueMonitors, &repositories.Monitor{
			ID:                   cm.ID,
			TargetURL:            cm.TargetURL,
			CheckIntervalSeconds: cm.IntervalSeconds,
		})
	}

	if len(dueMonitors) == 0 {
		return nil
	}

	// 2. Pass due jobs to your advanced 50km spatial round-robin matcher engine
	// This respects the anti-DDOS filter (max 5 matching domains per batch)
	assignments := sched.MatchBatch(dueMonitors)

	// 3. Process matches and dispatch individual targeted payloads
	for pubkey, m := range assignments {
		// Use node public key as a lock namespace to avoid hammering an identical instance concurrently
		lockKey := fmt.Sprintf("lock:monitor:%s:%s", m.ID, pubkey)
		set, err := rdb.SetNX(ctx, lockKey, 1, time.Duration(m.CheckIntervalSeconds)*time.Second).Result()
		if err != nil || !set {
			continue // Already dispatched to this specific runner within this interval window
		}

		// Build safe tracking jobID format embedding the runner token identity
		jobID := fmt.Sprintf("%s:%s:%d", m.ID, pubkey, now)
		nonceKey := fmt.Sprintf("nonce:%s", jobID)
		rdb.Set(ctx, nonceKey, 1, time.Duration(m.CheckIntervalSeconds*2)*time.Second)

		payload := JobPayload{
			JobID:        jobID,
			MonitorID:    m.ID,
			TargetURL:    m.TargetURL,
			RunnerPubkey: pubkey,
			IssuedAt:     now,
			ExpiresAt:    now + int64(m.CheckIntervalSeconds*2),
		}
		body, _ := json.Marshal(payload)

		// 4. Publish to RabbitMQ. Your consumer can routing-key filter jobs
		// or pass them directly over specific active client SSE/WebSocket streams.
		_ = rabbitCh.PublishWithContext(ctx, "", "job_queue", false, false,
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
			},
		)

		// 5. Update ZSET scheduler queue marker time block for this monitor profile
		key := fmt.Sprintf("sched:monitor:%s", m.ID)
		nextDue := float64(now + int64(m.CheckIntervalSeconds))
		rdb.ZAdd(ctx, "scheduler:due", redis.Z{Score: nextDue, Member: key})
	}

	return nil
}

type monitorCacheEntry struct {
	ID              string `json:"id"`
	TargetURL       string `json:"target_url"`
	IntervalSeconds int    `json:"interval_seconds"`
}

func cachedActiveMonitors(ctx context.Context, rdb *redis.Client, pool *pgxpool.Pool) ([]*monitorCacheEntry, error) {
	const cacheKey = "cache:active_monitors"

	raw, err := rdb.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var cached []*monitorCacheEntry
		if jsonErr := json.Unmarshal(raw, &cached); jsonErr == nil {
			return cached, nil
		}
	}

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
