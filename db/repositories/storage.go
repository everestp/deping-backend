package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/dto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	pool       *pgxpool.Pool
	Users      UserRepository
	Monitors   MonitorRepository
	Runners    RunnerRepository
	PingLogs   PingLogRepository
	SolanaSync SolanaSyncRepository
}

func NewStorage(pool *pgxpool.Pool) *Storage {
	s := &Storage{pool: pool}
	s.Users = &userRepo{pool: pool}
	s.Monitors = &monitorRepo{pool: pool}
	s.Runners = &runnerRepo{pool: pool}
	s.PingLogs = &pingLogRepo{pool: pool}
	s.SolanaSync = &solanaSyncRepo{pool: pool}
	return s
}
type ProcessJobSettlementResponse struct {
	Created     bool
	Amount      float64
	RewardDelta float64
	Owner       string
}
func (s *Storage) ProcessJobSettlement(
    ctx context.Context,
    monitorID string,
    runnerPubkey string,
    tokenCost float64,
) (*ProcessJobSettlementResponse, error) {

    tx, err := s.pool.Begin(ctx)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback(ctx)

    // 1. Deduct 1 credit from the USER who owns the monitor
    const deductQ = `
        UPDATE users 
        SET credit_balance = credit_balance - 1
        WHERE id = (SELECT owner_id FROM monitors WHERE id = $1)
          AND credit_balance > 0
        RETURNING credit_balance;
    `

    var newBalance int64
    err = tx.QueryRow(ctx, deductQ, monitorID).Scan(&newBalance)
    if err != nil {
        // If no rows updated, either monitor doesn't exist OR user has 0 credits
        return nil, fmt.Errorf("insufficient credits or monitor not found")
    }

    // 2. Update usage metrics on the monitor
    const updateMonitorQ = `
        UPDATE monitors 
        SET total_spent_tokens = total_spent_tokens + $1
        WHERE id = $2
    `
    _, err = tx.Exec(ctx, updateMonitorQ, tokenCost, monitorID)
    if err != nil {
        return nil, err
    }

    // 3. Reward Engine
    const q = `
        SELECT created, amount, reward_delta, node_pubkey
        FROM create_payout_event_if_threshold($1, $2)
    `
    var res ProcessJobSettlementResponse
    err = tx.QueryRow(ctx, q, runnerPubkey, env.Load().RewardThreshold).Scan(
        &res.Created, &res.Amount, &res.RewardDelta, &res.Owner,
    )
    if err != nil {
        return nil, err
    }

    if err := tx.Commit(ctx); err != nil {
        return nil, err
    }

    return &res, nil
}
// ── Domain Models ──

type User struct {
    ID            int       `json:"id"`
    Email         string    `json:"email"`
    PasswordHash  string    `json:"-"`
    WalletPubkey  string    `json:"wallet_pubkey"`
    CreditBalance int64     `json:"credit_balance"` // Added
    CreatedAt     time.Time `json:"created_at"`
}

type Monitor struct {
    ID                   string    `json:"id"`
    OwnerID              int       `json:"owner_id"`
    TargetURL            string    `json:"target_url"`
    CheckIntervalSeconds int       `json:"check_interval_seconds"`
    TotalSpentTokens     float64   `json:"total_spent_tokens"`
    IsActive             bool      `json:"is_active"`
    CreatedAt            time.Time `json:"created_at"`
}

type RunnerNode struct {
	ID                        int        `json:"id"`
	OwnerEmail                string     `json:"owner_email"`
	OwnerPubkey               string     `json:"owner_pubkey"`
	NodePubkey                *string    `json:"node_pubkey"`

	Region                    string     `json:"region"`
	Latitude                  float64    `json:"latitude"`
	Longitude                 float64    `json:"longitude"`

	OffchainAccumulatedTokens float64    `json:"offchain_accumulated_tokens"`
	TotalEarnedTokensAllTime  float64    `json:"total_earned_tokens_all_time"`

	PendingSolanaSync         bool       `json:"pending_solana_sync"`
	LastSeenTimestamp         time.Time  `json:"last_seen_timestamp"`

	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
	DeletedAt                 *time.Time `json:"deleted_at"`

	IsValidator               bool       `json:"is_validator"`
	StakedAmount              float64    `json:"staked_amount"`
	UnstakeRequestAt          *time.Time `json:"unstake_request_at"`
}

type PingLog struct {
	ID           int64     `json:"id"`
	MonitorID    string    `json:"monitor_id"`
	RunnerPubkey string    `json:"runner_pubkey"`
	DnsUs        uint64    `json:"dns_us"`
	TcpUs        uint64    `json:"tcp_us"`
	TlsUs        uint64    `json:"tls_us"`
	TtfbUs       uint64    `json:"ttfb_us"`
	TotalUs      uint64    `json:"total_us"`
	LatencyMs    float64   `json:"latency_ms"`
	StatusCode   int       `json:"status_code"`
	Success      bool      `json:"success"`
	ErrorKind    string    `json:"error_kind"`
	GeoRegion    string    `json:"geo_region"`
	Latitude     float64   `json:"latitude"`
	Longitude    float64   `json:"longitude"`
	Timestamp    time.Time `json:"timestamp"`
	TimestampMs   int       `json:"timestamp_ms"`
}

type SolanaSyncEvent struct {
	ID           int       `json:"id"`
	RunnerPubkey string    `json:"runner_pubkey"`
	TxSignature  string    `json:"tx_signature"`
	AmountRaw    int64     `json:"amount_raw"`
	ConfirmedAt  time.Time `json:"confirmed_at"`
}

type AccumulateResult struct {
	NewBalance float64 `json:"new_balance"`
	DidSync    bool    `json:"did_sync"`
}

// ── Interfaces ──

type UserRepository interface {
	Create(ctx context.Context, email, passwordHash, walletPubkey string) (*User, error)
	FindByEmail(ctx context.Context, email string) (*User, error)
	FindByWallet(ctx context.Context, walletPubkey string) (*User, error)
}

type MonitorRepository interface {
	Create(ctx context.Context, ownerID int, targetURL string, intervalSeconds int) (*Monitor, error)
	FindByOwner(ctx context.Context, ownerID int) ([]*Monitor, error)
	FindActive(ctx context.Context) ([]*Monitor, error)
	FindMany(ctx context.Context, ids []string) ([]*Monitor, error)
	FindByMonitorID(ctx context.Context, monitorId string) (*Monitor, error)
	UpdateActive(ctx context.Context, id string, isActive bool) error
	DeductCredit(ctx context.Context, id string, tokenCost float64) error
	Delete(ctx context.Context, id string, ownerID int) error
}

type RunnerRepository interface {
	Register(ctx context.Context, ownerEmail, ownerPubkey, region string, lat, lng float64) (*RunnerNode, error)
	FindByPubkey(ctx context.Context, pubkey string) (*RunnerNode, error)
	FindByNodePDA(ctx context.Context, pubkey string) (*RunnerNode, error)
	FindByNodePubKey(ctx context.Context, pubkey string) (*RunnerNode, error)
	FindByEmailAndPubkey(ctx context.Context, email, pubkey string) ([]*RunnerNode, error)
	UpdateHeartbeat(ctx context.Context, pubkey string) error
	AccumulateReward(ctx context.Context, pubkey string, delta, threshold float64) (*AccumulateResult, error)
	SetPendingSync(ctx context.Context, pubkey string, pending bool) error
}

type PingLogRepository interface {
	BulkInsert(ctx context.Context, logs []*PingLog) error
	FindByMonitor(ctx context.Context, monitorID string, limit int) ([]*PingLog, error)
	UptimePercentage(ctx context.Context, monitorID string, since time.Time) (float64, error)
	AvgLatencyUs(ctx context.Context, monitorID string, since time.Time) (uint64, error)
}

type SolanaSyncRepository interface {
	// ─────────────────────────────
	// QUEUE OPERATIONS (worker core)
	// ─────────────────────────────

	FetchPending(ctx context.Context, limit int) ([]dto.SolanaSyncEvent, error)

	MarkProcessing(ctx context.Context, id string) error

	MarkDone(ctx context.Context, id string, txSignature string) error

	MarkPendingAgain(ctx context.Context, id string) error

	// MarkFailed(ctx context.Context, id string, reason string) error



	// ─────────────────────────────
	// LEGACY / OPTIONAL (you had these, kept for safety)
	// ─────────────────────────────

	RecordSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error

	ExistsBySignature(ctx context.Context, txSignature string) (bool, error)

	// Final atomic settlement after successful Solana confirmation
	FinalizeSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error
}
