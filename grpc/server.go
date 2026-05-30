package grpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

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
	"github.com/everestp/depin-backend/dto"
	"github.com/everestp/depin-backend/grpc/pb"
	"github.com/everestp/depin-backend/services"
)

// NewServer wires the gRPC server and registers MonitorServiceServer.
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
    // Added "monitor_updates" for the fanout exchange mentioned in your probe results logic
    queues := []string{"job_queue", "processing_queue"}
    for _, qName := range queues {
        _, err := initCh.QueueDeclare(
            qName, true, false, false, false, nil,
        )
        if err != nil {
            log.Fatalf("[grpc-init] CRITICAL: Failed to declare queue %s: %v", qName, err)
        }
    }

    // Ensure exchange exists for your probe results
    if err := initCh.ExchangeDeclare("monitor_updates", "fanout", true, false, false, false, nil); err != nil {
        log.Fatalf("[grpc-init] CRITICAL: Failed to declare exchange: %v", err)
    }

    srv := grpc.NewServer(
        grpc.KeepaliveParams(keepalive.ServerParameters{
            MaxConnectionIdle:     5 * time.Minute,
            MaxConnectionAge:      30 * time.Minute,
            MaxConnectionAgeGrace: 5 * time.Second,
            Time:                  10 * time.Second,
            Timeout:               5 * time.Second,
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

// connectedMiner maintains the active session state for a registered miner.
type connectedMiner struct {
	nodeID    string
	latitude  float64
	longitude float64
	region    string
	sendCh    chan *pb.ServerMessage // Channel for pushing jobs to the specific miner
}

// MonitorServiceServer holds the dependencies for the gRPC service.
type MonitorServiceServer struct {
	pb.UnimplementedMonitorServiceServer

	runnerSvc  services.RunnerService
	monitorSvc services.MonitorService
	validator  *anticheat.Validator

	rabbitConn *amqp.Connection
	rabbitCh   *amqp.Channel

	mu     sync.RWMutex
	miners map[string]*connectedMiner // Thread-safe registry of connected nodes
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

    // 1. REGISTER
    firstMsg, err := stream.Recv()
    if err != nil {
        return err
    }
    reg := firstMsg.GetRegister()
    if reg == nil {
        return status.Error(codes.InvalidArgument, "must register first")
    }

    // 2. HANDSHAKE
    authNonce := make([]byte, 16)
    rand.Read(authNonce)
    nonceHex := hex.EncodeToString(authNonce)

    err = stream.Send(&pb.ServerMessage{
        Payload: &pb.ServerMessage_AuthChallenge{
            AuthChallenge: &pb.AuthChallenge{Nonce: nonceHex},
        },
    })
    if err != nil {
        return err
    }

    authMsg, err := stream.Recv()
    if err != nil {
        return err
    }

    resp := authMsg.GetAuthResponse()
    if resp == nil {
        return status.Error(codes.Unauthenticated, "expected auth response")
    }

    if err := s.validator.VerifyIdentity(reg.NodeId, nonceHex, hex.EncodeToString(resp.Signature)); err != nil {
        log.Printf("[grpc] auth failure: node_id=%s, error=%v", reg.NodeId, err)
        return status.Error(codes.Unauthenticated, "handshake failed")
    }

    // 3. MINER SESSION ESTABLISHED
    dbRunner, err := s.runnerSvc.GetByPubkey(ctx, reg.NodeId)
    if err != nil {
        return status.Error(codes.Internal, "internal server error")
    }
    if dbRunner == nil {
        return status.Error(codes.NotFound, "node not registered in system")
    }

    sendCh := make(chan *pb.ServerMessage, 64)
    s.mu.Lock()
    s.miners[reg.NodeId] = &connectedMiner{
        nodeID:    reg.NodeId,
        latitude:  dbRunner.Latitude,
        longitude: dbRunner.Longitude,
        region:    dbRunner.Region,
        sendCh:    sendCh,
    }
    s.mu.Unlock()

    // Cleanup session on exit
    defer func() {
        s.mu.Lock()
        delete(s.miners, reg.NodeId)
        s.mu.Unlock()
    }()

    // 4. BRIDGE: Push jobs from RabbitMQ/Channel to the gRPC Stream
    // We use a separate context for the bridge goroutine to ensure it cleans up
    bridgeCtx, cancel := context.WithCancel(ctx)
    defer cancel()

    errChan := make(chan error, 1)
    go func() {
        for {
            select {
            case msg := <-sendCh:
                if err := stream.Send(msg); err != nil {
                    errChan <- err
                    return
                }
            case <-bridgeCtx.Done():
                return
            }
        }
    }()

    // 5. RECEIVE LOOP: Handle messages from the Miner (Ping, Result, etc.)
    for {
        select {
        case err := <-errChan:
            return err
        default:
            msg, err := stream.Recv()
            if err == io.EOF {
                return nil
            }
            if err != nil {
                return err
            }
            s.handleMinerMessage(ctx, reg.NodeId, msg, stream)
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

    case *pb.MinerMessage_AuthResponse:
        // Optional: Handle late-stage auth responses if needed
        log.Printf("[grpc] received unexpected auth response from node_id=%s", nodeID)

    default:
        log.Printf("[grpc] unknown or unsupported payload type from node_id=%s", nodeID)
    }
}
func (s *MonitorServiceServer) handleProbeResult(ctx context.Context, nodeID string, r *pb.ProbeResult) {
    // 🛡️ LAYER 1: DEBUGGING NONCE
    // Log the incoming nonce and job details to verify what the miner is sending
    log.Printf("[grpc-debug] Processing result from node=%s, job=%s, nonce=%s", nodeID, r.JobId, r.TaskNonce)

    // 🛡️ LAYER 2: VALIDATE TASK NONCE (Replay Protection)
    // IMPORTANT: We use context.Background() here to prevent stream cancellation
    // from aborting the Redis lookup prematurely.
    if err := s.validator.ValidateTaskNonce(context.Background(), r.TaskNonce); err != nil {
        log.Printf("[grpc] SECURITY ALERT: rejected result - invalid/expired nonce node_id=%s job_id=%s nonce=%s err=%v",
            nodeID, r.JobId, r.TaskNonce, err)
        return
    }

//  🛡️ LAYER 2: VERIFY PAYLOAD SIGNATURE
// Construct the exact string used in Rust: job_id + task_nonce
signableData := fmt.Sprintf("%s%s", r.JobId, r.TaskNonce)

// Pass r.Signature as []byte directly (no hex.EncodeToString)
if err := s.validator.VerifySignature(nodeID, signableData, r.Signature); err != nil {
    log.Printf("[grpc] SECURITY ALERT: invalid payload signature node_id=%s job_id=%s error=%v", nodeID, r.JobId, err)
    return
}

    // Existing integrity checks
    if err := s.validator.CheckRateLimit(context.Background(), nodeID, 600); err != nil {
        log.Printf("[grpc] rate limit exceeded node_id=%s: %v", nodeID, err)
        return
    }

    latencyMs := int(r.TotalUs / 1000)
    if err := s.validator.DetectFakeLatency(context.Background(), nodeID, latencyMs); err != nil {
        log.Printf("[grpc] fake latency detection node_id=%s: %v", nodeID, err)
        return
    }

    // Miner Location Data
    s.mu.RLock()
    miner, ok := s.miners[nodeID]
    s.mu.RUnlock()

    region, lat, lng := "Unknown", 0.0, 0.0
    if ok {
        region, lat, lng = miner.region, miner.latitude, miner.longitude
    }
	parts := strings.Split(r.JobId, ":")
monitorID := ""
if len(parts) >= 1 {
    monitorID = parts[0]
}

    // Map Protobuf to DTO
    item := dto.PingResultItem{
        JobID:       r.JobId,
		MonitorID: monitorID,
        BatchID:     r.BatchId,
        NodeID:      nodeID,
        TargetURL:   r.TargetUrl,
        Success:     r.Success,
        StatusCode:  int(r.StatusCode),
        DnsUs:       int64(r.DnsUs),
        TcpUs:       int64(r.TcpUs),
        TlsUs:       int64(r.TlsUs),
        TtfbUs:      int64(r.TtfbUs),
        TotalUs:     int64(r.TotalUs),
        LatencyMs:   latencyMs,
        ErrorKind:   r.ErrorKind,
        ErrorMsg:    r.ErrorMsg,
        TimestampMs: int64(r.TimestampMs),
        GeoRegion:   region,
        Latitude:    lat,
        Longitude:   lng,
    }

    packet := services.ResultPacket{
        RunnerPubkey: nodeID,
        Results:      []dto.PingResultItem{item},
    }

    body, err := json.Marshal(packet)
    if err != nil {
        log.Printf("[grpc] marshal error node_id=%s: %v", nodeID, err)
        return
    }

    // 🛡️ LAYER 3: PUBLISH TO AMQP
    ch, err := s.GetHealthyChannel()
    if err != nil {
        log.Printf("[grpc] CRITICAL: RabbitMQ channel unavailable: %v", err)
        return
    }

    if err := ch.PublishWithContext(context.Background(), "monitor_updates", "", false, false, amqp.Publishing{
        ContentType:  "application/json",
        Body:         body,
        DeliveryMode: amqp.Persistent,
    }); err != nil {
        log.Printf("[grpc] AMQP publish error node_id=%s: %v", nodeID, err)
        _ = ch.Close()
    } else {
        log.Printf("[grpc] Successfully published result for job %s", r.JobId)
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
                    time.Sleep(2 * time.Second)
                    go s.ConsumeJobQueue(ctx)
                    return
                }

                var raw jobPayloadRaw
                if err := json.Unmarshal(msg.Body, &raw); err != nil {
                    log.Printf("[grpc-bridge] JSON unmarshal error: %v | Body: %s", err, string(msg.Body))
                    _ = msg.Ack(false)
                    continue
                }

                // 🔍 DEBUG: Verify if the nonce is in the RabbitMQ message
                if raw.TaskNonce == "" {
                    log.Printf("[grpc-bridge] WARNING: Task %s has EMPTY TaskNonce! Check upstream producer.", raw.JobID)
                    _ = msg.Ack(false) // Dropping corrupt task
                    continue
                }

                log.Printf("[grpc-bridge] Verified task %s | Nonce: %s | Target: %s",
                    raw.JobID, raw.TaskNonce, raw.RunnerPubkey)

                batch := &pb.JobBatch{
                    BatchId: raw.JobID,
                    Jobs: []*pb.Job{
                        {
                            JobId:     raw.JobID,
                            TargetUrl: raw.TargetURL,
                            TimeoutMs: 10000,
                            TaskNonce: raw.TaskNonce,
                        },
                    },
                }

                s.mu.RLock()
                targetMiner, isConnected := s.miners[raw.RunnerPubkey]
                s.mu.RUnlock()

                if isConnected {
                    select {
                    case targetMiner.sendCh <- &pb.ServerMessage{
                        Payload: &pb.ServerMessage_JobBatch{JobBatch: batch},
                    }:
                        log.Printf("[grpc-bridge] Successfully pushed job %s to gRPC stream", raw.JobID)
                        _ = msg.Ack(false)
                    default:
                        log.Printf("[grpc-bridge] Miner channel full, nacking job %s", raw.JobID)
                        _ = msg.Nack(false, true)
                    }
                } else {
                    log.Printf("[grpc-bridge] Miner %s disconnected, re-queuing job %s", raw.RunnerPubkey, raw.JobID)
                    _ = msg.Nack(false, true)
                }
            }
        }
    }()
}
// ── Geospatial Calculation Helpers ─────────────────────────────────────────

// func calculateDistance(lat1, lon1, lat2, lon2 float64) float64 {
// 	const earthRadiusKm = 6371.0

// 	dLat := (lat2 - lat1) * math.Pi / 180.0
// 	dLon := (lon2 - lon1) * math.Pi / 180.0

// 	l1 := lat1 * math.Pi / 180.0
// 	l2 := lat2 * math.Pi / 180.0

// 	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
// 		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(l1)*math.Cos(l2)
// 	c := 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))

// 	return earthRadiusKm * c
// }

// ── Internal conversion helpers ────────────────────────────────────────────

// jobPayloadRaw represents the job structure dispatched via RabbitMQ.
type jobPayloadRaw struct {
    JobID        string `json:"job_id"`
    MonitorID    string `json:"monitor_id"`
    TargetURL    string `json:"target_url"`
    RunnerPubkey string `json:"runner_pubkey"`
    IssuedAt     int64  `json:"issued_at"`
    ExpiresAt    int64  `json:"expires_at"`

    // 🛡️ SECURITY ADDITION: Unique nonce for the miner to sign
    TaskNonce    string `json:"task_nonce"`
}
