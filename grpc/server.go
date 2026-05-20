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

// NewServer wires the gRPC server and registers MonitorServiceServer.
func NewServer(
	runnerSvc services.RunnerService,
	monitorSvc services.MonitorService,
	validator *anticheat.Validator,
	rabbitConn *amqp.Connection, // <-- CHANGED: Pass the Connection instance instead of a static Channel
) *grpc.Server {

	// Open an initial channel to configure topology
	initCh, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("[grpc-init] CRITICAL: Failed to open initial RabbitMQ channel: %v", err)
	}

	log.Println("[grpc-init] Ensuring RabbitMQ topologies exist...")
	queues := []string{"job_queue", "processing_queue"}
	for _, qName := range queues {
		_, err := initCh.QueueDeclare(
			qName, // Queue name
			true,  // Durable
			false, // Delete when unused
			false, // Exclusive
			false, // No-wait
			nil,   // Arguments
		)
		if err != nil {
			log.Fatalf("[grpc-init] CRITICAL: Failed to declare queue %s: %v", qName, err)
		}
	}

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
		rabbitConn: rabbitConn, // Save the connection for future recovery
		rabbitCh:   initCh,     // Store the initial working channel
		miners:     make(map[string]*connectedMiner),
	}
	pb.RegisterMonitorServiceServer(srv, impl)

	log.Println("[grpc-init] Spawning async AMQP broker stream bridge pipeline...")
	impl.ConsumeJobQueue(context.Background())

	return srv
}

// ── Internal miner state ───────────────────────────────────────────────────

type connectedMiner struct {
	nodeID string
	region string
	sendCh chan *pb.ServerMessage
}

// ── MonitorServiceServer ───────────────────────────────────────────────────

type MonitorServiceServer struct {
	pb.UnimplementedMonitorServiceServer

	runnerSvc  services.RunnerService
	monitorSvc services.MonitorService
	validator  *anticheat.Validator

	rabbitConn *amqp.Connection // <-- ADDED: Safe parent connection handle
	rabbitCh   *amqp.Channel    // <-- Active thread channel (can become stale)

	mu     sync.RWMutex
	miners map[string]*connectedMiner
}

// ── FIX: Dynamic Hot-Swap Channel Recovery System ──────────────────────────
// This function guarantees that if RabbitMQ drops a channel, we silently
// drop-in replace it instantly without crashing or missing packets.
func (s *MonitorServiceServer) GetHealthyChannel() (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If channel is missing or has been explicitly closed/invalidated by RabbitMQ
	if s.rabbitCh == nil || s.rabbitCh.IsClosed() {
		log.Println("[grpc-amqp] Stale or closed channel detected. Initializing a clean hot-swap channel...")

		freshCh, err := s.rabbitConn.Channel()
		if err != nil {
			return nil, err
		}

		s.rabbitCh = freshCh
	}
	return s.rabbitCh, nil
}

func (s *MonitorServiceServer) JobStream(stream pb.MonitorService_JobStreamServer) error {
	ctx := stream.Context()

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

	if _, err := s.runnerSvc.GetByPubkey(ctx, nodeID); err != nil {
		return status.Errorf(codes.NotFound, "node %q not registered: %v", nodeID, err)
	}

	log.Printf("[grpc] miner registered: node_id=%s region=%s version=%s", nodeID, region, version)

	sendCh := make(chan *pb.ServerMessage, 64)
	s.mu.Lock()
	s.miners[nodeID] = &connectedMiner{nodeID: nodeID, region: region, sendCh: sendCh}
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.miners, nodeID)
		s.mu.Unlock()
		_ = s.runnerSvc.Heartbeat(ctx, nodeID)
		log.Printf("[grpc] miner disconnected: node_id=%s", nodeID)
	}()

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

func (s *MonitorServiceServer) handleMinerMessage(
	ctx context.Context,
	nodeID string,
	msg *pb.MinerMessage,
	stream pb.MonitorService_JobStreamServer,
) {
	switch p := msg.Payload.(type) {

	case *pb.MinerMessage_Ping:
		pong := &pb.ServerMessage{
			Payload: &pb.ServerMessage_Pong{
				Pong: &pb.Pong{
					TimestampMs:  p.Ping.TimestampMs,
					ServerTimeMs: uint64(time.Now().UnixMilli()),
				},
			},
		}
		if err := stream.Send(pong); err != nil {
			log.Printf("[grpc] pong send error node_id=%s: %v", nodeID, err)
		}
		if err := s.runnerSvc.Heartbeat(ctx, nodeID); err != nil {
			log.Printf("[grpc] heartbeat error node_id=%s: %v", nodeID, err)
		}

	case *pb.MinerMessage_Result:
		s.handleProbeResult(ctx, nodeID, p.Result)

	default:
		log.Printf("[grpc] unknown payload type from node_id=%s", nodeID)
	}
}

func (s *MonitorServiceServer) handleProbeResult(ctx context.Context, nodeID string, r *pb.ProbeResult) {
	if err := s.validator.ValidateJobID(ctx, r.JobId); err != nil {
		log.Printf("[grpc] rejected result node_id=%s job_id=%s: %v", nodeID, r.JobId, err)
		return
	}

	if err := s.validator.CheckRateLimit(ctx, nodeID, 600); err != nil {
		log.Printf("[grpc] rate limit node_id=%s: %v", nodeID, err)
		return
	}

	latencyMs := int(r.TotalUs / 1000)
	if err := s.validator.DetectFakeLatency(ctx, nodeID, latencyMs); err != nil {
		log.Printf("[grpc] fake latency node_id=%s job_id=%s: %v", nodeID, r.JobId, err)
		return
	}

	packet := probeResultToPacket(nodeID, r)
	body, err := json.Marshal(packet)
	if err != nil {
		log.Printf("[grpc] marshal probe result error: %v", err)
		return
	}

	// ── FIX: Fetch dynamic healthy channel instead of using stale static property ──
	ch, err := s.GetHealthyChannel()
	if err != nil {
		log.Printf("[grpc] CRITICAL: Cannot publish result. Failed to acquire healthy channel: %v", err)
		return
	}

	if err := ch.PublishWithContext(ctx, "", "processing_queue", false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	); err != nil {
		log.Printf("[grpc] publish probe result error node_id=%s: %v", nodeID, err)
		// Force closure so next invocation cycles into fresh setup
		_ = ch.Close()
	}
}

// ── Job dispatch ───────────────────────────────────────────────────────────

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

func (s *MonitorServiceServer) PushJobBatchToRegion(region string, batch *pb.JobBatch) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, m := range s.miners {
		if m.region == region {
			count++
			select {
			case m.sendCh <- &pb.ServerMessage{
				Payload: &pb.ServerMessage_JobBatch{JobBatch: batch},
			}:
				log.Printf("[grpc-bridge] Route success: Dispatched job batch %s down stream to node_id=%s [%s]", batch.Jobs[0].JobId, m.nodeID, region)
				return true
			default:
				log.Printf("[grpc-bridge] Target node send channel full, skipping down pool context: %s", m.nodeID)
			}
		}
	}

	log.Printf("[grpc-bridge] Route missing: No available miners streaming online inside region %q (Total active regional connections checked: %d)", region, count)
	return false
}

// ConsumeJobQueue bridges RabbitMQ job_queue → connected miners via the stream.
func (s *MonitorServiceServer) ConsumeJobQueue(ctx context.Context) {
	ch, err := s.GetHealthyChannel()
	if err != nil {
		log.Fatalf("[grpc-bridge] Failed setup on initialization queue fetch: %v", err)
	}

	_ = ch.Qos(1, 0, false)
	msgs, err := ch.Consume("job_queue", "grpc-dispatcher", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("[grpc-bridge] CRITICAL: Consumer pipeline channel failed to register: %v", err)
	}

	go func() {
		log.Println("[grpc-bridge] AMQP consumer background loop listening for task footprints...")
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					log.Println("[grpc-bridge] AMQP channel severed. Re-binding consumer loop...")
					time.Sleep(2 * time.Second)
					go s.ConsumeJobQueue(ctx) // Hot re-bind the listener loop safely
					return
				}

				batch := amqpBodyToJobBatch(msg.Body)
				if batch == nil {
					_ = msg.Ack(false)
					continue
				}

				dispatched := s.PushJobBatchToRegion(batch.BatchId, batch)
				if !dispatched {
					var raw jobPayloadRaw
					if err := json.Unmarshal(msg.Body, &raw); err == nil {
						dispatched = s.PushJobBatchToRegion(raw.Region, batch)
					}
				}

				if !dispatched {
					_ = msg.Ack(false)
					go func(payload []byte) {
						time.Sleep(3 * time.Second)
						currentCh, err := s.GetHealthyChannel()
						if err != nil {
							return
						}
						_ = currentCh.PublishWithContext(context.Background(), "", "job_queue", false, false,
							amqp.Publishing{
								ContentType:  "application/json",
								Body:         payload,
								DeliveryMode: amqp.Persistent,
							},
						)
					}(msg.Body)
				} else {
					_ = msg.Ack(false)
				}
			}
		}
	}()
}

// ── Internal conversion helpers ────────────────────────────────────────────

type jobPayloadRaw struct {
	JobID     string `json:"job_id"`
	MonitorID string `json:"monitor_id"`
	TargetURL string `json:"target_url"`
	Region    string `json:"region"`
	IssuedAt  int64  `json:"issued_at"`
	ExpiresAt int64  `json:"expires_at"`
}

func amqpBodyToJobBatch(body []byte) *pb.JobBatch {
	var raw jobPayloadRaw
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil
	}
	return &pb.JobBatch{
		BatchId: raw.Region,
		Jobs: []*pb.Job{
			{
				JobId:     raw.JobID,
				TargetUrl: raw.TargetURL,
				TimeoutMs: 10_000,
			},
		},
	}
}

type probeResultPacket struct {
	RunnerPubkey string `json:"runner_pubkey"`
	JobID        string `json:"job_id"`
	BatchID      string `json:"batch_id"`
	MonitorID    string `json:"monitor_id"`
	TargetURL    string `json:"target_url"`
	Success      bool   `json:"success"`
	StatusCode   uint32 `json:"status_code"`
	DnsUs        uint64 `json:"dns_us"`
	TcpUs        uint64 `json:"tcp_us"`
	TlsUs        uint64 `json:"tls_us"`
	TtfbUs       uint64 `json:"ttfb_us"`
	TotalUs      uint64 `json:"total_us"`
	LatencyMs    int    `json:"latency_ms"`
	ErrorKind    string `json:"error_kind"`
	ErrorMsg     string `json:"error_msg"`
	TimestampMs  uint64 `json:"timestamp_ms"`
}

func probeResultToPacket(nodeID string, r *pb.ProbeResult) *probeResultPacket {
	return &probeResultPacket{
		RunnerPubkey: nodeID,
		JobID:        r.JobId,
		BatchID:      r.BatchId,
		MonitorID:    "",
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
