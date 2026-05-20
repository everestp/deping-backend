package repositories

import (
	"context"
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

// PingLog stores one ProbeResult row. Phase latencies are stored in microseconds
// (matching the proto) so no precision is lost. LatencyMs is a derived column
// (total_us / 1000) kept for backwards-compatible queries.
type PingLog struct {
	ID           int64
	MonitorID    string
	RunnerPubkey string  // = node_id from ProbeResult
	// Phase latencies (microseconds) — directly from ProbeResult
	DnsUs   uint64
	TcpUs   uint64
	TlsUs   uint64
	TtfbUs  uint64
	TotalUs uint64
	// Derived millisecond latency — total_us / 1000
	LatencyMs  int
	StatusCode int
	Success    bool
	ErrorKind  string
	GeoRegion  string
	Timestamp  time.Time
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
	FindByJobID(ctx context.Context, jobID string) (*Monitor, error)
	UpdateActive(ctx context.Context, id string, isActive bool) error
	DeductCredit(ctx context.Context, id string) error
	Delete(ctx context.Context, id string, ownerID int) error
}

type RunnerRepository interface {
	Register(ctx context.Context, ownerEmail, ownerPubkey, region string, lat, lng float64) (*RunnerNode, error)
	FindByPubkey(ctx context.Context, pubkey string) (*RunnerNode, error)
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
