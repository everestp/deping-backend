package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/dto"
)

// SolanaSyncPayload is what the Solana sync worker reads off solana_sync_queue.
type SolanaSyncPayload struct {
	RunnerPubkey string  `json:"runner_pubkey"`
	AmountTokens float64 `json:"amount_tokens"`
}

type RewardService interface {
	AccumulateAndMaybeSync(ctx context.Context, pubkey string, delta float64) error
	GetStatus(ctx context.Context, pubkey string) (*dto.RewardStatusResponse, error)
}

type rewardService struct {
	store    *repositories.Storage
	rabbitCh *amqp.Channel
	cfg      *env.Config
}

func NewRewardService(store *repositories.Storage, rabbitCh *amqp.Channel, cfg *env.Config) RewardService {
	return &rewardService{store: store, rabbitCh: rabbitCh, cfg: cfg}
}

func (s *rewardService) AccumulateAndMaybeSync(ctx context.Context, pubkey string, delta float64) error {
    // 1. Atomic DB increment and "DidSync" check
    // Ensure repository returns DidSync = true ONLY if:
    // (old_balance + delta) >= s.cfg.RewardThreshold AND PendingSolanaSync was false.
    res, err := s.store.Runners.AccumulateReward(ctx, pubkey, delta, s.cfg.RewardThreshold)
    if err != nil {
        return fmt.Errorf("accumulate reward: %w", err)
    }

    if !res.DidSync {
        return nil
    }

    // 2. Prepare payload
    payload := SolanaSyncPayload{
        RunnerPubkey: pubkey,
        AmountTokens: res.NewBalance, // Send actual balance reached
    }

    body, err := json.Marshal(payload)
    if err != nil {
        return fmt.Errorf("marshal sync payload: %w", err)
    }

    // 3. Publish to queue
    err = s.rabbitCh.PublishWithContext(ctx, "", "solana_sync_queue", false, false,
        amqp.Publishing{
            ContentType:  "application/json",
            Body:         body,
            DeliveryMode: amqp.Persistent,
            // 💡 Add message ID or Timestamp to help with Idempotency
            MessageId:    fmt.Sprintf("%s-%d", pubkey, time.Now().Unix()),
        },
    )

    if err != nil {
        // If publishing fails, we ensure the system knows it's pending
        _ = s.store.Runners.SetPendingSync(ctx, pubkey, true)
        return fmt.Errorf("publish sync job: %w", err)
    }

    return nil
}

func (s *rewardService) GetStatus(ctx context.Context, pubkey string) (*dto.RewardStatusResponse, error) {
	node, err := s.store.Runners.FindByPubkey(ctx, pubkey)
	if err != nil {
		return nil, fmt.Errorf("runner not found: %w", err)
	}
	return &dto.RewardStatusResponse{
		RunnerPubkey:              node.OwnerPubkey,
		OffchainAccumulatedTokens: node.OffchainAccumulatedTokens,
		TotalEarnedAllTime:        node.TotalEarnedTokensAllTime,
		PendingSync:               node.PendingSolanaSync,
	}, nil
}
