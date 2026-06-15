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
				log.Println("[worker-processor] stopped")
				return
			default:
			}

			ch, err := rabbitConn.Channel()
			if err != nil {
				log.Printf("[worker-processor] channel error: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}

			if err := ch.Qos(10, 0, false); err != nil {
				log.Printf("[worker-processor] qos error: %v", err)
			}

			msgs, err := ch.Consume(
				"processing_queue",
				"result-processor-worker",
				false,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Printf("[worker-processor] consume error: %v", err)
				_ = ch.Close()
				time.Sleep(5 * time.Second)
				continue
			}

			_ = runConsumeLoop(ctx, msgs, pingLogSvc)
			_ = ch.Close()
		}
	}()
}

func runConsumeLoop(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	pingLogSvc services.PingLogService,
) error {

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg, ok := <-msgs:
			if !ok {
				return amqp.ErrClosed
			}

			var packet services.ResultPacket
			if err := json.Unmarshal(msg.Body, &packet); err != nil {
				log.Printf("[worker] bad packet: %v", err)
				_ = msg.Nack(false, false)
				continue
			}

			log.Printf("[SETTLEMENT] runner=%s records=%d",
				packet.RunnerPubkey,
				len(packet.Results),
			)

			log.Printf("[PACKET] %+v", packet)

			// ONLY DB settlement + logging happens here
			_, err := pingLogSvc.ProcessPacket(ctx, &packet)
			if err != nil {
				log.Printf("[worker] ProcessPacket failed: %v", err)
				_ = msg.Nack(false, true)
				continue
			}
	log.Printf("Deos we reach here                        =======================================================================================")
			// ACK only after success
			_ = msg.Ack(false)
		}
	}
}