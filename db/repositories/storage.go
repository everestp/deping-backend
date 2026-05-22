package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage is the single dependency injected into every service.
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

// ── Atomic Multi-Table Settlement Transaction ──────────────────────────────

// ProcessJobSettlement processes a safe transaction ensuring both sides update together.
// It deducts credits from the monitor and pays out rewards to the runner node atomically.
func (s *Storage) ProcessJobSettlement(ctx context.Context, monitorID string, runnerPubkey string, tokenCost float64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start settlement tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// 1. Deduct dynamic credit fee from monitor profile
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

	// 2. Accumulate reward to the runner node using your Postgres procedure
	const runnerQ = `SELECT new_balance, did_sync FROM accumulate_runner_reward($1, $2, 1000.0000)`
	var newBalance float64
	var didSync bool

	err = tx.QueryRow(ctx, runnerQ, runnerPubkey, tokenCost).Scan(&newBalance, &didSync)
	if err != nil {
		return fmt.Errorf("settlement runner accumulation failed: %w", err)
	}

	return tx.Commit(ctx)
}

// ── Domain models ──────────────────────────────────────────────────────────

type User struct {
	ID           int
	Email        string
	PasswordHash string
	WalletPubkey string
	CreatedAt    time.Time
}

type Monitor struct {
	ID                   string
	OwnerID              int
	TargetURL            string
	CheckIntervalSeconds int
	CreditBalanceChecks  int64
	TotalSpentTokens     float64
	IsActive             bool
	CreatedAt            time.Time
}

type RunnerNode struct {
	ID                        int
	OwnerEmail                string
	OwnerPubkey               string
	Region                    string
	Latitude                  float64
	Longitude                 float64
	OffchainAccumulatedTokens float64
	TotalEarnedTokensAllTime  float64
	PendingSolanaSync         bool
	LastSeenTimestamp         time.Time
}

type PingLog struct {
	ID           int64
	MonitorID    string
	RunnerPubkey string
	DnsUs        uint64
	TcpUs        uint64
	TlsUs        uint64
	TtfbUs       uint64
	TotalUs      uint64
	LatencyMs    int
	StatusCode   int
	Success      bool
	ErrorKind    string
	GeoRegion    string
	Timestamp    time.Time
}

type SolanaSyncEvent struct {
	ID           int
	RunnerPubkey string
	TxSignature  string
	AmountRaw    int64
	ConfirmedAt  time.Time
}

type AccumulateResult struct {
	NewBalance float64
	DidSync    bool
}

// ── Repository interfaces ──────────────────────────────────────────────────

type UserRepository interface {
	Create(ctx context.Context, email, passwordHash, walletPubkey string) (*User, error)
	FindByEmail(ctx context.Context, email string) (*User, error)
	FindByWallet(ctx context.Context, walletPubkey string) (*User, error)
}

type MonitorRepository interface {
    Create(ctx context.Context, ownerID int, targetURL string, intervalSeconds int) (*Monitor, error)
    FindByOwner(ctx context.Context, ownerID int) ([]*Monitor, error)
    FindActive(ctx context.Context) ([]*Monitor, error)
    // ADDED: Efficient batch lookup for the scheduler
    FindMany(ctx context.Context, ids []string) ([]*Monitor, error)
    FindByJobID(ctx context.Context, jobID string) (*Monitor, error)
    UpdateActive(ctx context.Context, id string, isActive bool) error
    DeductCredit(ctx context.Context, id string, tokenCost float64) error
    Delete(ctx context.Context, id string, ownerID int) error
}

type RunnerRepository interface {
	Register(ctx context.Context, ownerEmail, ownerPubkey, region string, lat, lng float64) (*RunnerNode, error)
	FindByPubkey(ctx context.Context, pubkey string) (*RunnerNode, error)
	// ADDED: For frontend lookup binding matching JWT email identity + Phantom pubkey
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
}
