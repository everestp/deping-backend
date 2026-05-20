package workers

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/everestp/depin-backend/services"
)

// StartResultProcessor boots up an isolated, resilient worker consumer loop
func StartResultProcessor(
	ctx context.Context,
	pool *pgxpool.Pool,
	rabbitConn *amqp.Connection, // Change parameter from rabbitCh *amqp.Channel to the connection handle
	pingLogSvc services.PingLogService,
	rewardSvc services.RewardService,
) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				log.Println("[worker-processor] Spawning fresh results pipeline worker channel stream...")

				ch, err := rabbitConn.Channel()
				if err != nil {
					log.Printf("[worker-processor] Failed to allocate background worker channel. Retrying in 5s: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}

				// Enforce fair dispatch parameters
				_ = ch.Qos(10, 0, false)

				msgs, err := ch.Consume(
					"processing_queue",
					"result-processor-worker",
					false, // Manual Ack
					false, // Exclusive
					false, // No-local
					false, // No-wait
					nil,
				)
				if err != nil {
					log.Printf("[worker-processor] Failed to register queue listener. Retrying: %v", err)
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

			// Process your 369-byte payload safely here
			log.Printf("[worker-processor] Received incoming job metric settlement payload (%d bytes)", len(msg.Body))

			// Replace with your internal payload processing or business layout parsing logic:
			// e.g., err := pingLogSvc.SaveMetric(ctx, msg.Body)

			// Acknowledge processing status to flush item out of RabbitMQ storage
			_ = msg.Ack(false)
		}
	}
}
