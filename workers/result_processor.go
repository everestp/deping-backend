package workers

import (
	"context"
	"encoding/json"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/everestp/depin-backend/services"
)

// StartResultProcessor consumes processing_queue.
//
// Each message is a JSON-encoded services.ResultPacket, published by:
//   - grpc/server.go  — after validating a ProbeResult from the Rust miner
//   - controllers/ping.go — after receiving a REST /api/v1/results submission
//
// For each packet it:
//  1. Resolves missing monitor_id from job_id (gRPC path)
//  2. Bulk-inserts PingLog rows via pgx.CopyFrom (phase latencies preserved)
//  3. Calls AccumulateAndMaybeSync — atomic off-chain reward + threshold trigger
func StartResultProcessor(
	ctx context.Context,
	pool *pgxpool.Pool,
	rabbitCh *amqp.Channel,
	pingLogSvc services.PingLogService,
	rewardSvc services.RewardService,
) {
	go func() {
		msgs, err := rabbitCh.Consume("processing_queue", "result-processor", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("[result-processor] consume: %v", err)
		}
		log.Println("[result-processor] started")

		for {
			select {
			case <-ctx.Done():
				log.Println("[result-processor] shutting down")
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				if err := handleResultMessage(ctx, msg, pingLogSvc, rewardSvc); err != nil {
					log.Printf("[result-processor] error: %v", err)
					_ = msg.Nack(false, true) // requeue on transient failure
				} else {
					_ = msg.Ack(false)
				}
			}
		}
	}()
}

func handleResultMessage(
	ctx context.Context,
	msg amqp.Delivery,
	pingLogSvc services.PingLogService,
	rewardSvc services.RewardService,
) error {
	var packet services.ResultPacket
	if err := json.Unmarshal(msg.Body, &packet); err != nil {
		// Malformed — dead-letter, never requeue
		_ = msg.Nack(false, false)
		return nil
	}
	if packet.RunnerPubkey == "" || len(packet.Results) == 0 {
		_ = msg.Nack(false, false)
		return nil
	}

	// ProcessPacket: resolves monitor_id, bulk-inserts, returns reward delta
	totalDelta, err := pingLogSvc.ProcessPacket(ctx, &packet)
	if err != nil {
		return err
	}

	if totalDelta <= 0 {
		return nil // all results were invalid/unknown jobs — no reward
	}

	// Atomic off-chain accumulation + solana_sync_queue trigger if threshold crossed
	return rewardSvc.AccumulateAndMaybeSync(ctx, packet.RunnerPubkey, totalDelta)
}
