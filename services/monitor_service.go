package services

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/dto"
)

type MonitorService interface {
	Create(ctx context.Context, ownerID int, req *dto.CreateMonitorRequest) (*dto.MonitorResponse, error)
	ListByOwner(ctx context.Context, ownerID int) ([]*dto.MonitorResponse, error)
	Stats(ctx context.Context, monitorID string, ownerID int) (*dto.MonitorStatsResponse, error)
	Pause(ctx context.Context, monitorID string, ownerID int) error
	Resume(ctx context.Context, monitorID string, ownerID int) error
	Delete(ctx context.Context, monitorID string, ownerID int) error
	GetActiveMonitors(ctx context.Context) ([]*repositories.Monitor, error)
}

type monitorService struct {
	store   *repositories.Storage
	rdb     *redis.Client
	rabbitCh *amqp.Channel
	cfg     *env.Config
}

func NewMonitorService(store *repositories.Storage, rdb *redis.Client, rabbitCh *amqp.Channel, cfg *env.Config) MonitorService {
	return &monitorService{store: store, rdb: rdb, rabbitCh: rabbitCh, cfg: cfg}
}

func (s *monitorService) Create(ctx context.Context, ownerID int, req *dto.CreateMonitorRequest) (*dto.MonitorResponse, error) {
    if req.TargetURL == "" {
        return nil, errors.New("target_url is required")
    }
    interval := req.IntervalSeconds
    if interval < 30 {
        interval = 60
    }

    // 1. Create in DB
    m, err := s.store.Monitors.Create(ctx, ownerID, req.TargetURL, interval)
    if err != nil {
        return nil, fmt.Errorf("create monitor: %w", err)
    }

    // 2. Invalidate cache (as you are doing)
    s.rdb.Del(ctx, "cache:active_monitors")

    // 3. ARM THE SCHEDULER: Immediately add to the ZSET
    // Score = 0 means "run this immediately on the next tick"
    err = s.rdb.ZAdd(ctx, "scheduler:due", redis.Z{
        Score:  0,
        Member: "sched:monitor:" + m.ID,
    }).Err()

    if err != nil {
        // Log the error but perhaps don't fail the request,
        // as the bootstrap logic will eventually pick it up on next restart
        log.Printf("[error] failed to arm scheduler for monitor %s: %v", m.ID, err)
    }

    return toMonitorResponse(m), nil
}
func (s *monitorService) ListByOwner(ctx context.Context, ownerID int) ([]*dto.MonitorResponse, error) {
	monitors, err := s.store.Monitors.FindByOwner(ctx, ownerID)
	if err != nil {
		return nil, err
	}

	resp := make([]*dto.MonitorResponse, len(monitors))
	for i, m := range monitors {
		resp[i] = toMonitorResponse(m)
	}
	return resp, nil
}

func (s *monitorService) Stats(ctx context.Context, monitorID string, ownerID int) (*dto.MonitorStatsResponse, error) {
	now := time.Now()
	pct24h, err := s.store.PingLogs.UptimePercentage(ctx, monitorID, now.Add(-24*time.Hour))
	if err != nil {
		return nil, err
	}
	pct7d, err := s.store.PingLogs.UptimePercentage(ctx, monitorID, now.Add(-7*24*time.Hour))
	if err != nil {
		return nil, err
	}
	monitor, err := s.store.Monitors.FindByMonitorID(ctx,monitorID)
	if err != nil {
		return nil, err
	}
	recent, err := s.store.PingLogs.FindByMonitor(ctx, monitorID, 7)
	if err != nil {
		return nil, err
	}

	return &dto.MonitorStatsResponse{
		MonitorID:    monitorID,
		CheckInterval: monitor.CheckIntervalSeconds,
		UptimePct24h: pct24h,
		UptimePct7d:  pct7d,
		RecentPings:  recent,
	}, nil
}

func (s *monitorService) Pause(ctx context.Context, monitorID string, ownerID int) error {
	if err := s.store.Monitors.UpdateActive(ctx, monitorID, false); err != nil {
		return err
	}
	s.rdb.Del(ctx, "cache:active_monitors")
	return nil
}

func (s *monitorService) Resume(ctx context.Context, monitorID string, ownerID int) error {
	if err := s.store.Monitors.UpdateActive(ctx, monitorID, true); err != nil {
		return err
	}
	s.rdb.Del(ctx, "cache:active_monitors")
	return nil
}

func (s *monitorService) Delete(ctx context.Context, monitorID string, ownerID int) error {
	if err := s.store.Monitors.Delete(ctx, monitorID, ownerID); err != nil {
		return err
	}
	s.rdb.Del(ctx, "cache:active_monitors")
	return nil
}

func (s *monitorService) GetActiveMonitors(ctx context.Context) ([]*repositories.Monitor, error) {
	return s.store.Monitors.FindActive(ctx)
}

func toMonitorResponse(m *repositories.Monitor) *dto.MonitorResponse {
	return &dto.MonitorResponse{
		ID:                   m.ID,
		TargetURL:            m.TargetURL,
		IntervalSeconds:      m.CheckIntervalSeconds,
		CreditBalanceChecks:  m.CreditBalanceChecks,
		TotalSpentTokens:     m.TotalSpentTokens,
		IsActive:             m.IsActive,
	}
}
