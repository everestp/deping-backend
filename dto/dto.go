package dto

import "time"

// ── Auth ───────────────────────────────────────────────────────────────────

type RegisterRequest struct {
	Email        string `json:"email"`
	Password     string `json:"password"`
	WalletPubkey string `json:"wallet_pubkey"`
}

type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type AuthResponse struct {
	Token string   `json:"token"`
	User  UserInfo `json:"user"`
}

type UserInfo struct {
	ID           int    `json:"id"`
	Email        string `json:"email"`
	WalletPubkey string `json:"wallet_pubkey"`
}

// ── Monitor ────────────────────────────────────────────────────────────────
type CreateMonitorRequest struct {
    TargetURL       string `json:"target_url"`
    IntervalSeconds int    `json:"interval_seconds"`
}

type MonitorResponse struct {
    ID                  string  `json:"id"`
    TargetURL           string  `json:"target_url"`
    IntervalSeconds     int     `json:"interval_seconds"`
    CreditBalanceChecks int64   `json:"credit_balance_checks"`
    TotalSpentTokens    float64 `json:"total_spent_tokens"`
    IsActive            bool    `json:"is_active"`
}

type MonitorStatsResponse struct {
	MonitorID    string  `json:"monitor_id"`
	CheckInterval int     `json:"check_interval"`
    UptimePct24h float64 `json:"uptime_pct_24h"`
    UptimePct7d  float64 `json:"uptime_pct_7d"`
    RecentPings  any     `json:"recent_pings"`
}

// 🚀 ADD THIS TO RESOLVE THE THREE UNDEFINED ERRORS:
type DashboardOverviewResponse struct {
    TotalMonitors      int     `json:"total_monitors"`
    ActiveMonitors     int     `json:"active_monitors"`
    GlobalAvgLatencyMs float64 `json:"global_avg_latency_ms"`
    TotalSpentTokens   float64 `json:"total_spent_tokens"`
    WalletConnected    bool    `json:"wallet_connected"`
    RunnerNodesCount   int     `json:"runner_nodes_count"`
}
// ── Runner ─────────────────────────────────────────────────────────────────

type RegisterRunnerRequest struct {
	OwnerPubkey string `json:"owner_pubkey"`
	Region      string `json:"region"`
	Latitude    string `json:"latitude"`
	Longitude   string `json:"longitude"`
}

type RunnerResponse struct {
	ID                        int     `json:"id"`
	OwnerPubkey               string  `json:"owner_pubkey"`
	NodePubkey                string `json:"node_pubkey"`
	Region                    string  `json:"region"`
	Latitude                  float64  `json:"latitude"`
	Longitude                 float64  `json:"longitude"`
	OffchainAccumulatedTokens float64 `json:"offchain_accumulated_tokens"`
	TotalEarnedTokensAllTime  float64 `json:"total_earned_tokens_all_time"`
	PendingSolanaSync         bool    `json:"pending_solana_sync"`
}
// ── Ping result (from Rust miner) ─────────────────────────────────────────



// PingResultItem represents the data structure of a single probe outcome.
type PingResultItem struct {
    JobID       string `json:"job_id"`
    BatchID     string `json:"batch_id"`
    NodeID      string `json:"node_id"`
    TargetURL   string `json:"target_url"`
    Success     bool   `json:"success"`
    StatusCode  int    `json:"status_code"`

    // ── Phase latencies in microseconds (us) ───────────────────────────────────
    DnsUs       int64  `json:"dns_us"`
    TcpUs       int64  `json:"tcp_us"`
    TlsUs       int64  `json:"tls_us"`
    TtfbUs      int64  `json:"ttfb_us"`
    TotalUs     int64  `json:"total_us"`
    LatencyMs   int    `json:"latency_ms"`

    // 🛡️ SECURITY ADDITION: Nonce for integrity verification
    TaskNonce   string `json:"task_nonce"`

    // ── Error envelope ────────────────────────────────────────────────────────
    ErrorKind   string `json:"error_kind"`
    ErrorMsg    string `json:"error_msg"`

    // ── Metadata ──────────────────────────────────────────────────────────────
    MonitorID   string `json:"monitor_id"`
    GeoRegion   string `json:"geo_region"`
    TimestampMs int64  `json:"timestamp_ms"`
    Latitude    float64 `json:"latitude"`
    Longitude   float64 `json:"longitude"`
}

type SubmitResultsRequest struct {
	RunnerPubkey string           `json:"runner_pubkey"`
	Signature    string           `json:"signature"` // Ed25519 signature tracking payload data validation
	Results      []PingResultItem `json:"results"`
}
// ── Reward ─────────────────────────────────────────────────────────────────

type RewardStatusResponse struct {
	RunnerPubkey              string  `json:"runner_pubkey"`
	OffchainAccumulatedTokens float64 `json:"offchain_accumulated_tokens"`
	TotalEarnedAllTime        float64 `json:"total_earned_all_time"`
	PendingSync               bool    `json:"pending_sync"`
}

// ── Generic ───────────────────────────────────────────────────────────────

type ErrorResponse struct {
	Error string `json:"error"`
}

type MessageResponse struct {
	Message string `json:"message"`
}

type RunnerDashboardResponse struct {
	HasData                  bool        `json:"has_data"`
	Nodes                    []NodeItem  `json:"nodes"`
}

type NodeItem struct {
	ID                        string    `json:"id"`
	OwnerPubkey               string    `json:"owner_pubkey"`
	Region                    string    `json:"region"`
	Latitude                  float64   `json:"latitude"`
	Longitude                 float64   `json:"longitude"`
	OffchainAccumulatedTokens float64   `json:"offchain_accumulated_tokens"`
	TotalEarnedTokensAllTime  float64   `json:"total_earned_tokens_all_time"`
	PendingSolanaSync         bool      `json:"pending_solana_sync"`
	LastSeenTimestamp         time.Time `json:"last_seen_timestamp"`
	IsOnline                  bool      `json:"is_online"` // Derived from Redis heartbeat state
}


