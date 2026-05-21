package services

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
	store       *repositories.Storage
	rdb         *redis.Client
	rabbitCh    *amqp.Channel
	cfg         *env.Config
	memRegistry *MemoryRegistry // Direct link to our real-time in-memory tracking pool
}

// NewRunnerService matches the exact signature called by your app's main orchestration wireframe.
func NewRunnerService(
	store *repositories.Storage,
	rdb *redis.Client,
	rabbitCh *amqp.Channel,
	cfg *env.Config,
	memRegistry *MemoryRegistry,
) RunnerService {
	return &runnerService{
		store:       store,
		rdb:         rdb,
		rabbitCh:    rabbitCh,
		cfg:         cfg,
		memRegistry: memRegistry,
	}
}

func (s *runnerService) Register(ctx context.Context, email string, req *dto.RegisterRunnerRequest) (*dto.RunnerResponse, error) {
	if req.OwnerPubkey == "" || req.Region == "" {
		return nil, errors.New("owner_pubkey and region are required")
	}

	// 💡 Convert string inputs to float64 to safely satisfy s.store.Runners.Register
	lat, _ := strconv.ParseFloat(req.Latitude, 64)
	lng, _ := strconv.ParseFloat(req.Longitude, 64)

	node, err := s.store.Runners.Register(ctx, email, req.OwnerPubkey, req.Region, lat, lng)
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
	// 1. Persist the database-backed timestamp update
	err := s.store.Runners.UpdateHeartbeat(ctx, pubkey)
	if err != nil {
		return fmt.Errorf("update database heartbeat: %w", err)
	}

	// 2. Fetch node properties to extract coordinates and owner mappings
	node, err := s.store.Runners.FindByPubkey(ctx, pubkey)
	if err != nil {
		return fmt.Errorf("resolve runner for memory allocation: %w", err)
	}

	// 3. Track state changes instantly inside our fast memory topology
	s.memRegistry.TrackHeartbeat(node.OwnerPubkey, node.OwnerEmail, node.Latitude, node.Longitude)

	return nil
}

func toRunnerResponse(n *repositories.RunnerNode) *dto.RunnerResponse {
	return &dto.RunnerResponse{
		ID:                        n.ID,
		OwnerPubkey:               n.OwnerPubkey,
		Region:                    n.Region,
		// 💡 Convert your float64 properties smoothly back into readable strings for the DTO layer
		Latitude:                  strconv.FormatFloat(n.Latitude, 'f', -1, 64),
		Longitude:                 strconv.FormatFloat(n.Longitude, 'f', -1, 64),
		OffchainAccumulatedTokens: n.OffchainAccumulatedTokens,
		TotalEarnedTokensAllTime:  n.TotalEarnedTokensAllTime,
		PendingSolanaSync:         n.PendingSolanaSync,
	}
}
