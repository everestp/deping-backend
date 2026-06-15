package services

import (
	"context"
	"encoding/json"
	
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/dto"
)

// ================================================================================================================
//                                              DATA TRANSFERS / SCHEMA
// ================================================================================================================

// ResultPacket represents the fully detailed JSON structure passed through RabbitMQ queues.
type ResultPacket struct {
	NodeId        string              `json:"node_pubkey"`
	RunnerPubkey string               `json:"runner_pubkey"`
	Signature    string               `json:"signature"`
	Results      []dto.PingResultItem `json:"results"` // Points directly to the comprehensive DTO schema definition
}

// ================================================================================================================
//                                              SERVICE CONTRACTS
// ================================================================================================================

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

// ================================================================================================================
//                                            CORE LOGIC PROCESSING
// ================================================================================================================

// ProcessPacket converts ProbeRecords, inserts logs, and settles payments ATOMICALLY.
func (s *pingLogService) ProcessPacket(ctx context.Context, packet *ResultPacket) (float64, error) {
	// if len(packet.Results) == 0 {
	// 	return 0, nil
	// }

	now := time.Now()
	logsToInsert := make([]*repositories.PingLog, 0, len(packet.Results))

	for i := range packet.Results {
		r := &packet.Results[i]

		if r.MonitorID == "" {
			continue
		}

		// 1. Call settlement (SQL handles reward)
		resp, err := s.store.ProcessJobSettlement(
			ctx,
			r.MonitorID,
			packet.RunnerPubkey,
			0,
		)

		if err != nil {
			continue
		}

		// 🔥 IMPORTANT: LOG PAYOUT INFO (this is what you asked)
		if resp.Created {
			log.Printf(
				"[PAYOUT] owner=%s monitor=%s runner=%s amount=%.6f reward_delta=%.6f triggered_by_monitor=%s",
				resp.Owner,
				r.MonitorID,
				packet.RunnerPubkey,
				resp.Amount,
				resp.RewardDelta,
				r.MonitorID,
			)
		}

		// 2. Build ping log
		ts := now
		if r.TimestampMs > 0 {
			ts = time.UnixMilli(r.TimestampMs)
		}

		logsToInsert = append(logsToInsert, &repositories.PingLog{
			MonitorID:    r.MonitorID,
			RunnerPubkey: packet.RunnerPubkey,
			DnsUs:        uint64(r.DnsUs),
			TcpUs:        uint64(r.TcpUs),
			TlsUs:        uint64(r.TlsUs),
			TtfbUs:       uint64(r.TtfbUs),
			TotalUs:      uint64(r.TotalUs),
			StatusCode:   r.StatusCode,
			Success:      r.Success,
			Timestamp:    ts,
			TimestampMs: int(r.TimestampMs),
		})
	}

	// 3. bulk insert logs
	if len(logsToInsert) > 0 {
		if err := s.store.PingLogs.BulkInsert(ctx, logsToInsert); err != nil {
			return 0, err
		}
	}

	return 0, nil
}
func (s *pingLogService) GetRecentPings(ctx context.Context, monitorID string, limit int) ([]*repositories.PingLog, error) {
	return s.store.PingLogs.FindByMonitor(ctx, monitorID, limit)
}

func (s *pingLogService) AvgLatencyUs(ctx context.Context, monitorID string, since time.Time) (uint64, error) {
	return s.store.PingLogs.AvgLatencyUs(ctx, monitorID, since)
}

// ================================================================================================================
//                                              SERIALIZATION HELPERS
// ================================================================================================================

// MarshalResultPacket cleanly preserves every single detailed microsecond and error tracking field for RabbitMQ.
func MarshalResultPacket(req *dto.SubmitResultsRequest) ([]byte, error) {
	return json.Marshal(ResultPacket{
		RunnerPubkey: req.RunnerPubkey,
		Signature:    req.Signature,
		Results:      req.Results, // High-fidelity pass-through: retains all rich microsecond latency metrics!
	})
}
