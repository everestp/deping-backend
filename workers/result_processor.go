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

func StartResultProcessor(
	ctx context.Context,
	pool *pgxpool.Pool,
	rabbitConn *amqp.Connection,
	pingLogSvc services.PingLogService,
	rewardSvc services.RewardService,
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

				if err := ch.Qos(10, 0, false); err != nil {
					log.Printf("[worker-processor] Failed to set QoS parameters: %v", err)
				}

				msgs, err := ch.Consume(
					"processing_queue",
					"result-processor-worker",
					false, // Manual Ack
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

				err = runConsumeLoop(ctx, msgs, pingLogSvc, rewardSvc)

				if err != nil {
					log.Printf("[worker-processor] Stream connection severed: %v. Re-establishing...", err)
				}

				_ = ch.Close()
			}
		}
	}()
}

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

			// 🎯 FIXED: Mapping directly to services.ResultPacket now extracts the nested arrays cleanly
			var packet services.ResultPacket
			if err := json.Unmarshal(msg.Body, &packet); err != nil {
				log.Printf("[worker-processor] ❌ Failed to parse ResultPacket JSON: %v. Dropping corrupt message.", err)
				_ = msg.Nack(false, false)
				continue
			}

			log.Printf(
				"[SETTLEMENT] Processing packet from Node: %s containing %d probe records.",
				packet.RunnerPubkey,
				len(packet.Results),
			)
// Replace your log line with this:
log.Printf("The data of the packet: %+v", packet)	// Route payload context straight out into database settlement channels
			rewardDelta, err := pingLogSvc.ProcessPacket(ctx, &packet)
			if err != nil {
				log.Printf("[worker-processor] 🚨 Failed to complete ProcessPacket business flow: %v. Re-queueing work batch.", err)
				_ = msg.Nack(false, true)
				continue
			}

			if rewardDelta > 0 {
				err = rewardSvc.AccumulateAndMaybeSync(ctx, packet.RunnerPubkey, rewardDelta)
				if err != nil {
					log.Printf("[worker-processor] ⚠️ Reward accumulation processing issue for %s: %v", packet.RunnerPubkey, err)
				} else {
					log.Printf("[SETTLEMENT] Successfully registered +%.4f token delta for Node: %s", rewardDelta, packet.RunnerPubkey)
				}
			}

			_ = msg.Ack(false)
		}
	}
}

