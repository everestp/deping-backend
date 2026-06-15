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
	TaskNonce    string `json:"task_nonce"`
	IssuedAt     int64  `json:"issued_at"`
	ExpiresAt    int64  `json:"expires_at"`
}

// schedulerAMQP wraps the RabbitMQ connection and owns its own channel.
// The channel is reopened on publish failure so a silent broker-side close
// (idle timeout, heartbeat miss) never permanently kills the scheduler.
type schedulerAMQP struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newSchedulerAMQP(conn *amqp.Connection) (*schedulerAMQP, error) {
	s := &schedulerAMQP{conn: conn}
	if err := s.reopen(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *schedulerAMQP) reopen() error {
	if s.ch != nil {
		_ = s.ch.Close() // best-effort; may already be dead
	}
	ch, err := s.conn.Channel()
	if err != nil {
		return fmt.Errorf("schedulerAMQP: open channel: %w", err)
	}
	// Prefetch 1 — not needed for publishing but keeps parity with consumers
	s.ch = ch
	return nil
}

// publish publishes to queue, reopening the channel once on failure.
func (s *schedulerAMQP) publish(ctx context.Context, queue string, body []byte) error {
	err := s.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
	if err == nil {
		return nil
	}

	// Channel is probably dead — reopen and retry once
	log.Printf("[scheduler-amqp] publish failed (%v), reopening channel and retrying", err)
	if rerr := s.reopen(); rerr != nil {
		return fmt.Errorf("scheduler: channel reopen failed: %w", rerr)
	}
	return s.ch.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

// StartScheduler runs the Redis-backed scheduler in the background.
func StartScheduler(
	ctx context.Context,
	rdb *redis.Client,
	pool *pgxpool.Pool,
	rabbitConn *amqp.Connection,
	reg *services.MemoryRegistry,
	sched *services.SmartScheduler,
) {
	// The scheduler owns its own AMQP channel — never shares with gRPC server.
	// This prevents the common "channel closed after 2-3 min idle" silent failure.
	amqpPub, err := newSchedulerAMQP(rabbitConn)
	if err != nil {
		log.Fatalf("[scheduler] CRITICAL: cannot open AMQP channel: %v", err)
	}

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
				if err := scheduleTick(ctx, rdb, pool, amqpPub, reg, sched); err != nil {
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
	amqpPub *schedulerAMQP,
	reg *services.MemoryRegistry,
	sched *services.SmartScheduler,
) error {
	now := time.Now().Unix()

	// 1. Fetch due monitors from sorted set
	dueMembers, err := rdb.ZRangeByScore(ctx, "scheduler:due", &redis.ZRangeBy{
		Min: "-inf", Max: fmt.Sprintf("%d", now), Offset: 0, Count: 100,
	}).Result()
	if err != nil || len(dueMembers) == 0 {
		return nil
	}

	// 2. Collect monitor IDs
	monitorIDs := make([]string, 0, len(dueMembers))
	for _, m := range dueMembers {
		monitorIDs = append(monitorIDs, strings.TrimPrefix(m, "sched:monitor:"))
	}

	// 3. Fetch monitor records
	repo := repositories.NewStorage(pool)
	monitors, err := repo.Monitors.FindMany(ctx, monitorIDs)
	if err != nil {
		return err
	}

	// 4. Remove ghost monitors (deleted from DB but still in ZSET)
	foundMap := make(map[string]struct{}, len(monitors))
	for _, m := range monitors {
		foundMap[m.ID] = struct{}{}
	}
	for _, mKey := range dueMembers {
		id := strings.TrimPrefix(mKey, "sched:monitor:")
		if _, exists := foundMap[id]; !exists {
			rdb.ZRem(ctx, "scheduler:due", mKey)
		}
	}

	if len(monitors) == 0 {
		return nil
	}

	// 5. Match monitors → nodes
	assignments := sched.MatchBatch(monitors)

	processedMonitorIDs := make(map[string]bool, len(assignments))

	for pubkey, m := range assignments {
		processedMonitorIDs[m.ID] = true

		// Reschedule in ZSET immediately so it won't fire again until next interval
		nextDue := float64(now + int64(m.CheckIntervalSeconds))
		rdb.ZAdd(ctx, "scheduler:due", redis.Z{
			Score:  nextDue,
			Member: "sched:monitor:" + m.ID,
		})

		// Build and publish job
		taskNonce := uuid.New().String()
		jobID := fmt.Sprintf("%s:%s:%d", m.ID, pubkey, now)

		// Store nonce with TTL = 2x interval so the miner always has time to respond
		nonceTTL := time.Duration(m.CheckIntervalSeconds*2) * time.Second
		if nonceTTL < 15*time.Minute {
			nonceTTL = 15 * time.Minute
		}
		rdb.Set(ctx, "task_nonce:"+taskNonce, "1", nonceTTL)

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
		if err := amqpPub.publish(ctx, "job_queue", body); err != nil {
			log.Printf("[scheduler] AMQP publish failed for monitor %s: %v", m.ID, err)
			// Don't reschedule to now — let it fire at nextDue naturally
		} else {
			log.Printf("[scheduler] dispatched job=%s monitor=%s runner=%s", jobID, m.ID, pubkey)
		}
	}

	// 6. Reschedule monitors that were due but got no node assignment this tick
	// (no online nodes, or node cap hit). Advance by a short backoff so they
	// retry next tick rather than thundering-herd every second.
	const noNodeBackoffSec = 5
	for _, m := range monitors {
		if !processedMonitorIDs[m.ID] {
			rdb.ZAdd(ctx, "scheduler:due", redis.Z{
				Score:  float64(now + noNodeBackoffSec),
				Member: "sched:monitor:" + m.ID,
			})
		}
	}

	return nil
}