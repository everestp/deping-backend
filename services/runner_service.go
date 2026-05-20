package services

import (
	"context"
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/dto"
)

type RunnerService interface {
	Register(ctx context.Context, email string, req *dto.RegisterRunnerRequest) (*dto.RunnerResponse, error)
	GetByPubkey(ctx context.Context, pubkey string) (*dto.RunnerResponse, error)
	Heartbeat(ctx context.Context, pubkey string) error
}

type runnerService struct {
	store    *repositories.Storage
	rdb      *redis.Client
	rabbitCh *amqp.Channel
	cfg      *env.Config
}

func NewRunnerService(store *repositories.Storage, rdb *redis.Client, rabbitCh *amqp.Channel, cfg *env.Config) RunnerService {
	return &runnerService{store: store, rdb: rdb, rabbitCh: rabbitCh, cfg: cfg}
}

func (s *runnerService) Register(ctx context.Context, email string, req *dto.RegisterRunnerRequest) (*dto.RunnerResponse, error) {
	if req.OwnerPubkey == "" || req.Region == "" {
		return nil, errors.New("owner_pubkey and region are required")
	}

	node, err := s.store.Runners.Register(ctx, email, req.OwnerPubkey, req.Region, req.Latitude, req.Longitude)
	if err != nil {
		return nil, fmt.Errorf("register runner: %w", err)
	}
	return toRunnerResponse(node), nil
}

func (s *runnerService) GetByPubkey(ctx context.Context, pubkey string) (*dto.RunnerResponse, error) {
	node, err := s.store.Runners.FindByPubkey(ctx, pubkey)
	if err != nil {
		return nil, fmt.Errorf("runner not found: %w", err)
	}
	return toRunnerResponse(node), nil
}

func (s *runnerService) Heartbeat(ctx context.Context, pubkey string) error {
	return s.store.Runners.UpdateHeartbeat(ctx, pubkey)
}

func toRunnerResponse(n *repositories.RunnerNode) *dto.RunnerResponse {
	return &dto.RunnerResponse{
		ID:                        n.ID,
		OwnerPubkey:               n.OwnerPubkey,
		Region:                    n.Region,
		OffchainAccumulatedTokens: n.OffchainAccumulatedTokens,
		TotalEarnedTokensAllTime:  n.TotalEarnedTokensAllTime,
		PendingSolanaSync:         n.PendingSolanaSync,
	}
}
