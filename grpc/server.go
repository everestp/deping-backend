package grpc

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"math"
	"strconv"
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
	rabbitConn *amqp.Connection,
) *grpc.Server {

	initCh, err := rabbitConn.Channel()
	if err != nil {
		log.Fatalf("[grpc-init] CRITICAL: Failed to open initial RabbitMQ channel: %v", err)
	}

	log.Println("[grpc-init] Ensuring RabbitMQ topologies exist...")
	queues := []string{"job_queue", "processing_queue"}
	for _, qName := range queues {
		_, err := initCh.QueueDeclare(
			qName, true, false, false, false, nil,
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
		rabbitConn: rabbitConn,
		rabbitCh:   initCh,
		miners:     make(map[string]*connectedMiner),
	}
	pb.RegisterMonitorServiceServer(srv, impl)

	log.Println("[grpc-init] Spawning async AMQP broker stream bridge pipeline...")
	impl.ConsumeJobQueue(context.Background())

	return srv
}

// ── Internal miner state ───────────────────────────────────────────────────

type connectedMiner struct {
	nodeID    string
	latitude  float64
	longitude float64
	sendCh    chan *pb.ServerMessage
}

// ── MonitorServiceServer ───────────────────────────────────────────────────

type MonitorServiceServer struct {
	pb.UnimplementedMonitorServiceServer

	runnerSvc  services.RunnerService
	monitorSvc services.MonitorService
	validator  *anticheat.Validator

	rabbitConn *amqp.Connection
	rabbitCh   *amqp.Channel

	mu     sync.RWMutex
	miners map[string]*connectedMiner
}

func (s *MonitorServiceServer) GetHealthyChannel() (*amqp.Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
	version := reg.Register.Version

	if nodeID == "" {
		return status.Error(codes.InvalidArgument, "node_id is required")
	}

	// 🛡️ DYNAMIC COORD SOURCE: Query the DB model layer by nodeID instead of using client-side overrides
	dbRunner, err := s.runnerSvc.GetByPubkey(ctx, nodeID)
	if err != nil {
		return status.Errorf(codes.NotFound, "node %q not registered in database records: %v", nodeID, err)
	}

	// Safely parse string fields from database into geolocation float structures
	dbLat, _ := strconv.ParseFloat(dbRunner.Latitude, 64)
	dbLng, _ := strconv.ParseFloat(dbRunner.Longitude, 64)

	log.Printf("[grpc] miner registered via database coordinates: node_id=%s database-location=(%.4f, %.4f) version=%s", nodeID, dbLat, dbLng, version)

	sendCh := make(chan *pb.ServerMessage, 64)
	s.mu.Lock()
	s.miners[nodeID] = &connectedMiner{nodeID: nodeID, latitude: dbLat, longitude: dbLng, sendCh: sendCh}
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
		log.Printf("[grpc" + "] marshal probe result error: %v", err)
		return
	}

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
		_ = ch.Close()
	}
}

// ── Job Dispatching Realignment ───────────────────────────────────────────

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

// ConsumeJobQueue unmarshals the targeted job and routes it directly to the assigned miner.
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
		log.Println("[grpc-bridge] AMQP consumer loop listening for targeted tasks...")
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					log.Println("[grpc-bridge] AMQP channel severed. Re-binding consumer loop...")
					time.Sleep(2 * time.Second)
					go s.ConsumeJobQueue(ctx)
					return
				}

				var raw jobPayloadRaw
				if err := json.Unmarshal(msg.Body, &raw); err != nil {
					_ = msg.Ack(false)
					continue
				}

				batch := &pb.JobBatch{
					BatchId: raw.JobID,
					Jobs: []*pb.Job{
						{
							JobId:     raw.JobID,
							TargetUrl: raw.TargetURL,
							TimeoutMs: 10000,
						},
					},
				}

				// 🎯 THE RESOLUTION FIX: Match miners using target node public key directly
				s.mu.RLock()
				targetMiner, isConnected := s.miners[raw.RunnerPubkey]
				s.mu.RUnlock()

				dispatched := false
				if isConnected {
					select {
					case targetMiner.sendCh <- &pb.ServerMessage{
						Payload: &pb.ServerMessage_JobBatch{JobBatch: batch},
					}:
						log.Printf("[grpc-bridge] Route success: Dispatched job %s straight to target node_id=%s", batch.Jobs[0].JobId, raw.RunnerPubkey)
						dispatched = true
					default:
						log.Printf("[grpc-bridge] Target node %s channel full — dropping batch context", raw.RunnerPubkey)
					}
				}

				if !dispatched {
					log.Printf("[grpc-bridge] Route missing: Target node_id=%s is offline. Re-queuing job %s...", raw.RunnerPubkey, raw.JobID)
					_ = msg.Ack(false)

					// Re-enqueue back to RabbitMQ with a 3-second delay fallback window
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

// ── Geospatial Calculation Helpers ─────────────────────────────────────────

func calculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
	const earthRadiusKm = 6371.0

	dLat := (lat2 - lat1) * math.Pi / 180.0
	dLon := (lon2 - lon1) * math.Pi / 180.0

	l1 := lat1 * math.Pi / 180.0
	l2 := lat2 * math.Pi / 180.0

	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(l1)*math.Cos(l2)
	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

	return earthRadiusKm * c
}

// ── Internal conversion helpers ────────────────────────────────────────────

type jobPayloadRaw struct {
	JobID        string `json:"job_id"`
	MonitorID    string `json:"monitor_id"`
	TargetURL    string `json:"target_url"`
	RunnerPubkey string `json:"runner_pubkey"` // Maps to workers.go structural assignments
	IssuedAt     int64  `json:"issued_at"`
	ExpiresAt    int64  `json:"expires_at"`
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
