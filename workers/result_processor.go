package workers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/everestp/depin-backend/services"
)

// StartResultProcessor boots up an isolated, resilient worker consumer loop
func StartResultProcessor(
	ctx context.Context,
	pool *pgxpool.Pool,
	rabbitConn *amqp.Connection,
	pingLogSvc services.PingLogService, // Using your official interface patterns
	rewardSvc services.RewardService,   // Using your official interface patterns
) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				log.Println("[worker-processor] Context canceled. Stopping result processor background worker.")
				return
			default:
				log.Println("[worker-processor] Spawning fresh results pipeline worker channel stream...")

				ch, err := rabbitConn.Channel()
				if err != nil {
					log.Printf("[worker-processor] Failed to allocate background worker channel. Retrying in 5s: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}

				// Enforce fair dispatch parameters (prefetch count of 10 matches workload sizing)
				if err := ch.Qos(10, 0, false); err != nil {
					log.Printf("[worker-processor] Failed to set QoS parameters: %v", err)
				}

				msgs, err := ch.Consume(
					"processing_queue",
					"result-processor-worker",
					false, // Manual Ack (Crucial to ensure data lands safely before deletion)
					false,
					false,
					false,
					nil,
				)
				if err != nil {
					log.Printf("[worker-processor] Failed to register queue listener. Retrying in 5s: %v", err)
					_ = ch.Close()
					time.Sleep(5 * time.Second)
					continue
				}

				// Run internal processing loop until channel context drops out
				err = runConsumeLoop(ctx, msgs, pingLogSvc, rewardSvc)
				if err != nil {
					log.Printf("[worker-processor] Stream connection severed: %v. Re-establishing...", err)
				}

				_ = ch.Close()
			}
		}
	}()
}

// runConsumeLoop reads incoming packets, processes batches, and synchronizes settlements
func runConsumeLoop(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	pingLogSvc services.PingLogService,
	rewardSvc services.RewardService,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			// 1. Decode the combined ResultPacket structural layout
			var packet services.ResultPacket
			if err := json.Unmarshal(msg.Body, &packet); err != nil {
				log.Printf("[worker-processor] ❌ Failed to parse ResultPacket JSON: %v. Dropping corrupt message.", err)
				_ = msg.Nack(false, false) // Drop without re-queueing corrupt syntax data
				continue
			}

			log.Printf(
				"[SETTLEMENT] Processing packet from Node: %s containing %d probe records.",
				packet.RunnerPubkey,
				len(packet.Results),
			)

			// 2. Route directly to your business logic layer
			// ProcessPacket automatically inserts logs, resolves missing IDs, and subtracts monitor credit balances!
			rewardDelta, err := pingLogSvc.ProcessPacket(ctx, &packet)
			if err != nil {
				log.Printf("[worker-processor] 🚨 Failed to complete ProcessPacket business flow: %v. Re-queueing work batch.", err)
				_ = msg.Nack(false, true) // Re-queue to preserve database integrity
				continue
			}

			// 3. Coordinate token settlement tracking if a reward delta was accumulated
			if rewardDelta > 0 {
				err = rewardSvc.AccumulateAndMaybeSync(ctx, packet.RunnerPubkey, rewardDelta)
				if err != nil {
					log.Printf("[worker-processor] ⚠️ Reward accumulation processing issue for %s: %v", packet.RunnerPubkey, err)
					// We do not reject/re-queue here because the data has already successfully saved to your database inside ProcessPacket.
				} else {
					log.Printf("[SETTLEMENT] Successfully registered +%.4f token delta for Node: %s", rewardDelta, packet.RunnerPubkey)
				}
			}

			// 4. Explicit Acknowledgment - Flush packet from queue
			_ = msg.Ack(false)
		}
	}
}
