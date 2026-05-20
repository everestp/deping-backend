package services

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/dto"
)

// ── ResultPacket ───────────────────────────────────────────────────────────
// Published to processing_queue by both:
//   - grpc/server.go (gRPC path — from ProbeResult)
//   - controllers/ping.go (REST path — from SubmitResultsRequest)
// The result processor deserialises this and calls BulkInsert + AccumulateReward.

type ResultPacket struct {
	RunnerPubkey string        `json:"runner_pubkey"`
	Results      []ProbeRecord `json:"results"`
}

// ProbeRecord is the normalised internal representation of one probe.
// It covers both the gRPC ProbeResult fields (microsecond latencies, phase breakdown)
// and the REST fallback (latency_ms only). Fields unused by one path are zero-valued.
type ProbeRecord struct {
	JobID      string `json:"job_id"`
	BatchID    string `json:"batch_id"`
	MonitorID  string `json:"monitor_id"`  // may be empty; resolved by processor from job_id
	TargetURL  string `json:"target_url"`
	Success    bool   `json:"success"`
	StatusCode int    `json:"status_code"`

	// Phase latencies (microseconds) — from ProbeResult via gRPC
	DnsUs   uint64 `json:"dns_us"`
	TcpUs   uint64 `json:"tcp_us"`
	TlsUs   uint64 `json:"tls_us"`
	TtfbUs  uint64 `json:"ttfb_us"`
	TotalUs uint64 `json:"total_us"`

	// Derived millisecond latency — total_us/1000; or latency_ms from REST path
	LatencyMs int `json:"latency_ms"`

	ErrorKind   string `json:"error_kind"`
	GeoRegion   string `json:"geo_region"`
	TimestampMs uint64 `json:"timestamp_ms"`
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

// ProcessPacket converts ProbeRecords into PingLog rows and bulk-inserts them.
// Resolves monitor_id from job_id prefix when missing.
// Returns total reward delta for the batch.
func (s *pingLogService) ProcessPacket(ctx context.Context, packet *ResultPacket) (float64, error) {
	if len(packet.Results) == 0 {
		return 0, nil
	}

	now := time.Now()
	logs := make([]*repositories.PingLog, 0, len(packet.Results))
	var totalDelta float64

	for _, r := range packet.Results {
		monitorID := r.MonitorID
		// Resolve monitor_id from job_id when not populated (gRPC path)
		if monitorID == "" && r.JobID != "" {
			m, err := s.store.Monitors.FindByJobID(ctx, r.JobID)
			if err != nil {
				// Unknown job — skip, don't reward
				continue
			}
			monitorID = m.ID
			// Deduct one credit from the monitor's balance
			_ = s.store.Monitors.DeductCredit(ctx, monitorID)
		}

		ts := now
		if r.TimestampMs > 0 {
			ts = time.UnixMilli(int64(r.TimestampMs))
		}

		latencyMs := r.LatencyMs
		if latencyMs == 0 && r.TotalUs > 0 {
			latencyMs = int(r.TotalUs / 1000)
		}

		log := &repositories.PingLog{
			MonitorID:    monitorID,
			RunnerPubkey: packet.RunnerPubkey,
			DnsUs:        r.DnsUs,
			TcpUs:        r.TcpUs,
			TlsUs:        r.TlsUs,
			TtfbUs:       r.TtfbUs,
			TotalUs:      r.TotalUs,
			LatencyMs:    latencyMs,
			StatusCode:   r.StatusCode,
			Success:      r.Success,
			ErrorKind:    r.ErrorKind,
			GeoRegion:    r.GeoRegion,
			Timestamp:    ts,
		}
		logs = append(logs, log)

		// Reward: 0.001 base for completing the probe, +0.001 bonus for success
		totalDelta += 0.001
		if r.Success {
			totalDelta += 0.001
		}
	}

	if err := s.store.PingLogs.BulkInsert(ctx, logs); err != nil {
		return 0, fmt.Errorf("bulk insert: %w", err)
	}
	return totalDelta, nil
}

func (s *pingLogService) GetRecentPings(ctx context.Context, monitorID string, limit int) ([]*repositories.PingLog, error) {
	return s.store.PingLogs.FindByMonitor(ctx, monitorID, limit)
}

func (s *pingLogService) AvgLatencyUs(ctx context.Context, monitorID string, since time.Time) (uint64, error) {
	return s.store.PingLogs.AvgLatencyUs(ctx, monitorID, since)
}

// ── REST path helper ───────────────────────────────────────────────────────

// MarshalResultPacket converts the REST SubmitResultsRequest → ResultPacket JSON
// for publishing to processing_queue. The REST path doesn't carry phase latencies,
// so only LatencyMs and StatusCode are populated.
func MarshalResultPacket(req *dto.SubmitResultsRequest) ([]byte, error) {
	records := make([]ProbeRecord, len(req.Results))
	for i, r := range req.Results {
		records[i] = ProbeRecord{
			JobID:      r.JobID,
			MonitorID:  r.MonitorID,
			StatusCode: r.StatusCode,
			LatencyMs:  r.LatencyMs,
			GeoRegion:  r.GeoRegion,
			Success:    r.StatusCode >= 200 && r.StatusCode < 400,
		}
	}
	return json.Marshal(ResultPacket{RunnerPubkey: req.RunnerPubkey, Results: records})
}
