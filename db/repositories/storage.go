package repositories

import (
	"context"
	"fmt"
	"time"


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

func (s *Storage) ProcessJobSettlement(ctx context.Context, monitorID string, runnerPubkey string, tokenCost float64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start settlement tx: %w", err)
	}
	defer tx.Rollback(ctx)

	const monitorQ = `
        UPDATE monitors
        SET credit_balance_checks = credit_balance_checks - 1,
            total_spent_tokens = total_spent_tokens + $1
        WHERE id = $2 AND credit_balance_checks > 0 AND is_active = TRUE AND deleted_at IS NULL`

	cmd, err := tx.Exec(ctx, monitorQ, tokenCost, monitorID)
	if err != nil {
		return fmt.Errorf("settlement monitor update failed: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return fmt.Errorf("settlement rejected: monitor %s is inactive or out of credits", monitorID)
	}

	const runnerQ = `
    SELECT COALESCE(new_balance, 0.0), COALESCE(did_sync, false) 
    FROM accumulate_runner_reward($1, $2, 10.0000)`

var newBalance float64
var didSync bool

err = tx.QueryRow(ctx, runnerQ, runnerPubkey, tokenCost).Scan(&newBalance, &didSync)
	if err != nil {
		return fmt.Errorf("settlement runner accumulation failed: %w", err)
	}

	return tx.Commit(ctx)
}

// ── Domain Models ──

type User struct {
	ID           int       `json:"id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	WalletPubkey string    `json:"wallet_pubkey"`
	CreatedAt    time.Time `json:"created_at"`
}

type Monitor struct {
	ID                   string    `json:"id"`
	OwnerID              int       `json:"owner_id"`
	TargetURL            string    `json:"target_url"`
	CheckIntervalSeconds int       `json:"check_interval_seconds"`
	CreditBalanceChecks  int64     `json:"credit_balance_checks"`
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
    RecordSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error
    ExistsBySignature(ctx context.Context, txSignature string) (bool, error)
    // FinalizeSync handles the atomic log-and-debit transaction
    FinalizeSync(ctx context.Context, runnerPubkey, txSignature string, amountRaw int64) error
}
