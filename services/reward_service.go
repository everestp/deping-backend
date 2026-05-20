package services

import (
	"context"
	"encoding/json"
	"fmt"

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

// AccumulateAndMaybeSync calls the atomic DB function, then — if threshold was
// crossed — enqueues a solana_sync_queue message. DB is mutated BEFORE any RPC.
func (s *rewardService) AccumulateAndMaybeSync(ctx context.Context, pubkey string, delta float64) error {
	res, err := s.store.Runners.AccumulateReward(ctx, pubkey, delta, s.cfg.RewardThreshold)
	if err != nil {
		return fmt.Errorf("accumulate reward: %w", err)
	}

	if !res.DidSync {
		return nil
	}

	// Threshold crossed — push sync job. Overflow remainder is already preserved in DB.
	payload := SolanaSyncPayload{
		RunnerPubkey: pubkey,
		AmountTokens: s.cfg.RewardThreshold,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal sync payload: %w", err)
	}

	err = s.rabbitCh.PublishWithContext(ctx, "", "solana_sync_queue", false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		// DB already decremented — mark pending_sync so retry worker can re-queue.
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
