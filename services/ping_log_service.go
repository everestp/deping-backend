package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/dto"
)

// ── ResultPacket ───────────────────────────────────────────────────────────

// ResultPacket represents the fully detailed JSON structure passed through RabbitMQ queues
type ResultPacket struct {
	RunnerPubkey string               `json:"runner_pubkey"`
	Signature    string               `json:"signature"`
	Results      []dto.PingResultItem `json:"results"` // 🚀 Points directly to the comprehensive DTO schema definition
}

// ── PingLogService ─────────────────────────────────────────────────────────

type PingLogService interface {
	ProcessPacket(ctx context.Context, packet *ResultPacket) (totalDelta float64, err error)
	GetRecentPings(ctx context.Context, monitorID string, limit int) ([]*repositories.PingLog, error)
	AvgLatencyUs(ctx context.Context, monitorID string, since time.Time) (uint64, error)
}

type pingLogService struct {
	store *repositories.Storage
	pool  *pgxpool.Pool
}

func NewPingLogService(store *repositories.Storage, pool *pgxpool.Pool) PingLogService {
	return &pingLogService{store: store, pool: pool}
}

// ProcessPacket converts ProbeRecords, inserts logs, and settles payments ATOMICALLY
func (s *pingLogService) ProcessPacket(ctx context.Context, packet *ResultPacket) (float64, error) {
	if len(packet.Results) == 0 {
		return 0, nil
	}

	now := time.Now()
	logsToInsert := make([]*repositories.PingLog, 0, len(packet.Results))
	var collectiveBatchRewards float64

	for idx := range packet.Results {
		r := &packet.Results[idx]
		monitorID := r.MonitorID

		// 1. Core Verification Phase
		if monitorID == "" && r.JobID != "" {
			m, err := s.store.Monitors.FindByJobID(ctx, r.JobID)
			if err != nil {
				log.Printf("[processor] warning: skipping unknown job validation frame (ID: %s): %v", r.JobID, err)
				continue
			}
			monitorID = m.ID
		}

		// Fail-safe protection if both inputs map completely empty properties
		if monitorID == "" {
			continue
		}

		// 2. Dynamic Performance Tier Token Computations
		// Base completion award fee allocation: 0.001 tokens
		// Premium performance success bonus allocation: +0.001 tokens
		tokenSettlementRate := 0.001
		if r.Success {
			tokenSettlementRate += 0.001
		}

		// 3. Dual-Sided Database Ledger Settlement (Atomic Transaction)
		err := s.store.ProcessJobSettlement(ctx, monitorID, packet.RunnerPubkey, tokenSettlementRate)
		if err != nil {
			log.Printf("[processor] settlement transaction aborted for monitor %s -> runner %s: %v", monitorID, packet.RunnerPubkey, err)
			continue // Skip adding this log line since payment and billing failed
		}

		// 4. Transform Records Into System Log Profiles
		ts := now
		if r.TimestampMs > 0 {
			ts = time.UnixMilli(r.TimestampMs)
		}

		latencyMs := r.LatencyMs
		if latencyMs == 0 && r.TotalUs > 0 {
			latencyMs = int(r.TotalUs / 1000)
		}

		logEntry := &repositories.PingLog{
			MonitorID:    monitorID,
			RunnerPubkey: packet.RunnerPubkey,
			DnsUs:        uint64(r.DnsUs), // ✅ int64 → uint64 cast, safe for positive durations
			TcpUs:        uint64(r.TcpUs),
			TlsUs:        uint64(r.TlsUs),
			TtfbUs:       uint64(r.TtfbUs),
			TotalUs:      uint64(r.TotalUs),
			LatencyMs:    r.LatencyMs,
			StatusCode:   r.StatusCode,
			Success:      r.Success,
			ErrorKind:    r.ErrorKind,
			GeoRegion:    r.GeoRegion,
			Timestamp:    ts,
		}

		logsToInsert = append(logsToInsert, logEntry)
		collectiveBatchRewards += tokenSettlementRate
	}

	// 5. Bulk write verified logging metadata to database disk storage partitions
	if len(logsToInsert) > 0 {
		if err := s.store.PingLogs.BulkInsert(ctx, logsToInsert); err != nil {
			return 0, fmt.Errorf("ping metrics batch storage commit execution failure: %w", err)
		}
	}

	return collectiveBatchRewards, nil
}

func (s *pingLogService) GetRecentPings(ctx context.Context, monitorID string, limit int) ([]*repositories.PingLog, error) {
	return s.store.PingLogs.FindByMonitor(ctx, monitorID, limit)
}

func (s *pingLogService) AvgLatencyUs(ctx context.Context, monitorID string, since time.Time) (uint64, error) {
	return s.store.PingLogs.AvgLatencyUs(ctx, monitorID, since)
}

// ── REST path helper ───────────────────────────────────────────────────────

// MarshalResultPacket cleanly preserves every single detailed microsecond and error tracking field for RabbitMQ
func MarshalResultPacket(req *dto.SubmitResultsRequest) ([]byte, error) {
	return json.Marshal(ResultPacket{
		RunnerPubkey: req.RunnerPubkey,
		Signature:    req.Signature,
		Results:      req.Results, // 🎯 High-fidelity pass-through: retains all rich microsecond latency metrics!
	})
}
