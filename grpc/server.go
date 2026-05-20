package grpc

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/everestp/depin-backend/anticheat"
	"github.com/everestp/depin-backend/grpc/pb"
	"github.com/everestp/depin-backend/services"
)

// NewServer wires the gRPC server and registers MonitorServiceServer,
// which implements the JobStream RPC defined in monitor.proto.
func NewServer(
	runnerSvc services.RunnerService,
	monitorSvc services.MonitorService,
	validator *anticheat.Validator,
	rabbitCh *amqp.Channel,
) *grpc.Server {
	srv := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     5 * time.Minute,
			MaxConnectionAge:      30 * time.Minute,
			MaxConnectionAgeGrace: 5 * time.Second,
			Time:                  10 * time.Second,
			Timeout:               3 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)

	impl := &MonitorServiceServer{
		runnerSvc:  runnerSvc,
		monitorSvc: monitorSvc,
		validator:  validator,
		rabbitCh:   rabbitCh,
		miners:     make(map[string]*connectedMiner),
	}
	pb.RegisterMonitorServiceServer(srv, impl)
	return srv
}

// ── Internal miner state ───────────────────────────────────────────────────

type connectedMiner struct {
	nodeID string
	region string
	sendCh chan *pb.ServerMessage
}

// ── MonitorServiceServer ───────────────────────────────────────────────────

// MonitorServiceServer implements pb.MonitorServiceServer (generated from monitor.proto).
// It handles the JobStream bidirectional RPC:
//
//	Miner → Server:  MinerRegister (first), Ping, ProbeResult
//	Server → Miner:  Pong (reply to Ping), JobBatch
type MonitorServiceServer struct {
	pb.UnimplementedMonitorServiceServer

	runnerSvc  services.RunnerService
	monitorSvc services.MonitorService
	validator  *anticheat.Validator
	rabbitCh   *amqp.Channel

	mu     sync.RWMutex
	miners map[string]*connectedMiner // node_id → state
}

// JobStream is the single bidirectional RPC from monitor.proto.
//
// Protocol (matches Rust CLI tonic client):
//
//	Step 1: miner sends MinerRegister as the first message
//	Step 2: server validates node_id, responds with nothing — jobs arrive as JobBatch
//	Step 3: miner sends Ping periodically; server replies with Pong inline
//	Step 4: miner sends ProbeResult for each completed job; server validates + queues
//	Step 5: server pushes JobBatch whenever the scheduler dispatches work
func (s *MonitorServiceServer) JobStream(stream pb.MonitorService_JobStreamServer) error {
	ctx := stream.Context()

	// ── Step 1: expect MinerRegister as the first message ─────────────────
	firstMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.Internal, "recv first message: %v", err)
	}

	reg, ok := firstMsg.Payload.(*pb.MinerMessage_Register)
	if !ok {
		return status.Error(codes.InvalidArgument, "first message must be MinerRegister")
	}

	nodeID := reg.Register.NodeId
	region := reg.Register.Region
	version := reg.Register.Version

	if nodeID == "" || region == "" {
		return status.Error(codes.InvalidArgument, "node_id and region are required")
	}

	// ── Step 2: validate node is registered in runner_nodes ───────────────
	// node_id is the ed25519 public key hex — maps to owner_pubkey in DB
	if _, err := s.runnerSvc.GetByPubkey(ctx, nodeID); err != nil {
		return status.Errorf(codes.NotFound, "node %q not registered: %v", nodeID, err)
	}

	log.Printf("[grpc] miner registered: node_id=%s region=%s version=%s", nodeID, region, version)

	// ── Step 3: register miner outbound channel ────────────────────────────
	sendCh := make(chan *pb.ServerMessage, 64)
	s.mu.Lock()
	s.miners[nodeID] = &connectedMiner{nodeID: nodeID, region: region, sendCh: sendCh}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.miners, nodeID)
		s.mu.Unlock()
		_ = s.runnerSvc.Heartbeat(ctx, nodeID) // final heartbeat timestamp on disconnect
		log.Printf("[grpc] miner disconnected: node_id=%s", nodeID)
	}()

	// ── Recv goroutine — Ping + ProbeResult from miner ────────────────────
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					recvErrCh <- nil
				} else {
					recvErrCh <- err
				}
				return
			}
			s.handleMinerMessage(ctx, nodeID, msg, stream)
		}
	}()

	// ── Main loop: push JobBatch down, handle lifecycle ────────────────────
	for {
		select {
		case <-ctx.Done():
			return nil

		case err := <-recvErrCh:
			if err != nil {
				log.Printf("[grpc] recv error node_id=%s: %v", nodeID, err)
			}
			return err

		case msg, ok := <-sendCh:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				log.Printf("[grpc] send error node_id=%s: %v", nodeID, err)
				return err
			}
		}
	}
}

// handleMinerMessage dispatches each incoming MinerMessage type.
func (s *MonitorServiceServer) handleMinerMessage(
	ctx context.Context,
	nodeID string,
	msg *pb.MinerMessage,
	stream pb.MonitorService_JobStreamServer,
) {
	switch p := msg.Payload.(type) {

	case *pb.MinerMessage_Ping:
		// Reply with Pong immediately — same goroutine is safe because
		// stream.Send is called from the recv goroutine only here.
		// The main loop handles all other sends via sendCh.
		pong := &pb.ServerMessage{
			Payload: &pb.ServerMessage_Pong{
				Pong: &pb.Pong{
					TimestampMs:  p.Ping.TimestampMs, // echo back for RTT calculation
					ServerTimeMs: uint64(time.Now().UnixMilli()),
				},
			},
		}
		if err := stream.Send(pong); err != nil {
			log.Printf("[grpc] pong send error node_id=%s: %v", nodeID, err)
		}
		// Update liveness timestamp
		if err := s.runnerSvc.Heartbeat(ctx, nodeID); err != nil {
			log.Printf("[grpc] heartbeat error node_id=%s: %v", nodeID, err)
		}

	case *pb.MinerMessage_Result:
		s.handleProbeResult(ctx, nodeID, p.Result)

	default:
		log.Printf("[grpc] unknown payload type from node_id=%s", nodeID)
	}
}

// handleProbeResult runs all anti-cheat checks then enqueues to processing_queue.
func (s *MonitorServiceServer) handleProbeResult(ctx context.Context, nodeID string, r *pb.ProbeResult) {
	// ── Anti-cheat: nonce validation (atomic Redis GET+DEL) ───────────────
	if err := s.validator.ValidateJobID(ctx, r.JobId); err != nil {
		log.Printf("[grpc] rejected result node_id=%s job_id=%s: %v", nodeID, r.JobId, err)
		return
	}

	// ── Anti-cheat: per-runner rate limiting ──────────────────────────────
	if err := s.validator.CheckRateLimit(ctx, nodeID, 600); err != nil {
		log.Printf("[grpc] rate limit node_id=%s: %v", nodeID, err)
		return
	}

	// ── Anti-cheat: fake latency detection (total_us converted to ms) ─────
	latencyMs := int(r.TotalUs / 1000)
	if err := s.validator.DetectFakeLatency(ctx, nodeID, latencyMs); err != nil {
		log.Printf("[grpc] fake latency node_id=%s job_id=%s: %v", nodeID, r.JobId, err)
		return
	}

	// ── Enqueue to processing_queue for bulk DB insert + reward calc ──────
	packet := probeResultToPacket(nodeID, r)
	body, err := json.Marshal(packet)
	if err != nil {
		log.Printf("[grpc] marshal probe result error: %v", err)
		return
	}

	if err := s.rabbitCh.PublishWithContext(ctx, "", "processing_queue", false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	); err != nil {
		log.Printf("[grpc] publish probe result error node_id=%s: %v", nodeID, err)
	}
}

// ── Job dispatch ───────────────────────────────────────────────────────────

// PushJobBatch sends a JobBatch to a connected miner by node_id. Non-blocking.
func (s *MonitorServiceServer) PushJobBatch(nodeID string, batch *pb.JobBatch) {
	s.mu.RLock()
	m, ok := s.miners[nodeID]
	s.mu.RUnlock()
	if !ok {
		return
	}
	msg := &pb.ServerMessage{
		Payload: &pb.ServerMessage_JobBatch{JobBatch: batch},
	}
	select {
	case m.sendCh <- msg:
	default:
		log.Printf("[grpc] node_id=%s send channel full — dropping batch %s", nodeID, batch.BatchId)
	}
}

// PushJobBatchToRegion sends a batch to the first connected miner in the given region.
// Replace with proper load balancing (round-robin within region) for production.
func (s *MonitorServiceServer) PushJobBatchToRegion(region string, batch *pb.JobBatch) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, m := range s.miners {
		if m.region == region {
			select {
			case m.sendCh <- &pb.ServerMessage{
				Payload: &pb.ServerMessage_JobBatch{JobBatch: batch},
			}:
				return true
			default:
				// channel full — try next miner
			}
		}
	}
	return false
}

// ConnectedCount returns the number of currently streaming miners.
func (s *MonitorServiceServer) ConnectedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.miners)
}

// ConnectedByRegion returns the count of miners per region (for metrics).
func (s *MonitorServiceServer) ConnectedByRegion() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int)
	for _, m := range s.miners {
		out[m.region]++
	}
	return out
}

// ConsumeJobQueue bridges RabbitMQ job_queue → connected miners via the stream.
// Reads jobPayloadRaw JSON from the queue and dispatches to the right region.
func (s *MonitorServiceServer) ConsumeJobQueue(ctx context.Context) {
	msgs, err := s.rabbitCh.Consume("job_queue", "grpc-dispatcher", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[grpc] consume job_queue: %v", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				batch := amqpBodyToJobBatch(msg.Body)
				if batch == nil {
					_ = msg.Nack(false, false)
					continue
				}

				// Try the target region; fall back to any miner if none available
				dispatched := s.PushJobBatchToRegion(batch.BatchId, batch) // BatchId reused as region key
				if !dispatched {
					// batch.BatchId is a UUID — we need the region from the raw payload
					var raw jobPayloadRaw
					if err := json.Unmarshal(msg.Body, &raw); err == nil {
						dispatched = s.PushJobBatchToRegion(raw.Region, batch)
					}
				}
				if !dispatched {
					_ = msg.Nack(false, true) // no miners online — requeue
				} else {
					_ = msg.Ack(false)
				}
			}
		}
	}()
}

// ── Internal conversion helpers ────────────────────────────────────────────

// jobPayloadRaw matches the JSON published by workers/scheduler.go.
type jobPayloadRaw struct {
	JobID     string `json:"job_id"`
	MonitorID string `json:"monitor_id"`
	TargetURL string `json:"target_url"`
	Region    string `json:"region"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
}

// amqpBodyToJobBatch converts a scheduler JSON payload → pb.JobBatch.
// The scheduler publishes individual jobs; we wrap each in a single-job batch.
// BatchId is set to the target region so PushJobBatchToRegion can route it.
func amqpBodyToJobBatch(body []byte) *pb.JobBatch {
	var raw jobPayloadRaw
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	// timeout_ms: default 10s if not set by scheduler
	return &pb.JobBatch{
		BatchId: raw.Region, // used for region routing in ConsumeJobQueue
		Jobs: []*pb.Job{
			{
				JobId:     raw.JobID,
				TargetUrl: raw.TargetURL,
				TimeoutMs: 10_000,
			},
		},
	}
}

// probeResultPacket is what gets published to processing_queue.
// It is consumed by workers/result_processor.go and mapped to services.ResultPacket.
type probeResultPacket struct {
	RunnerPubkey string `json:"runner_pubkey"` // = node_id from ProbeResult
	JobID        string `json:"job_id"`
	BatchID      string `json:"batch_id"`
	MonitorID    string `json:"monitor_id"`    // resolved from job_id by result processor
	TargetURL    string `json:"target_url"`
	Success      bool   `json:"success"`
	StatusCode   uint32 `json:"status_code"`
	// Phase latencies — microseconds, matching ProbeResult exactly
	DnsUs   uint64 `json:"dns_us"`
	TcpUs   uint64 `json:"tcp_us"`
	TlsUs   uint64 `json:"tls_us"`
	TtfbUs  uint64 `json:"ttfb_us"`
	TotalUs uint64 `json:"total_us"`
	// Derived — total_us / 1000, floored to ms for DB storage
	LatencyMs  int    `json:"latency_ms"`
	ErrorKind  string `json:"error_kind"`
	ErrorMsg   string `json:"error_msg"`
	TimestampMs uint64 `json:"timestamp_ms"`
}

func probeResultToPacket(nodeID string, r *pb.ProbeResult) *probeResultPacket {
	return &probeResultPacket{
		RunnerPubkey: nodeID,
		JobID:        r.JobId,
		BatchID:      r.BatchId,
		MonitorID:    "", // resolved by result_processor from job_id → monitor_id Redis lookup
		TargetURL:    r.TargetUrl,
		Success:      r.Success,
		StatusCode:   r.StatusCode,
		DnsUs:        r.DnsUs,
		TcpUs:        r.TcpUs,
		TlsUs:        r.TlsUs,
		TtfbUs:       r.TtfbUs,
		TotalUs:      r.TotalUs,
		LatencyMs:    int(r.TotalUs / 1000),
		ErrorKind:    r.ErrorKind,
		ErrorMsg:     r.ErrorMsg,
		TimestampMs:  r.TimestampMs,
	}
}
