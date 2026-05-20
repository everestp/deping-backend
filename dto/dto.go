package dto

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
	ID                   string  `json:"id"`
	TargetURL            string  `json:"target_url"`
	IntervalSeconds      int     `json:"interval_seconds"`
	CreditBalanceChecks  int64   `json:"credit_balance_checks"`
	TotalSpentTokens     float64 `json:"total_spent_tokens"`
	IsActive             bool    `json:"is_active"`
}

type MonitorStatsResponse struct {
	MonitorID        string  `json:"monitor_id"`
	UptimePct24h     float64 `json:"uptime_pct_24h"`
	UptimePct7d      float64 `json:"uptime_pct_7d"`
	RecentPings      any     `json:"recent_pings"`
}

// ── Runner ─────────────────────────────────────────────────────────────────

type RegisterRunnerRequest struct {
	OwnerPubkey string  `json:"owner_pubkey"`
	Region      string  `json:"region"`
	Latitude    float64 `json:"latitude"`
	Longitude   float64 `json:"longitude"`
}

type RunnerResponse struct {
	ID                        int     `json:"id"`
	OwnerPubkey               string  `json:"owner_pubkey"`
	Region                    string  `json:"region"`
	OffchainAccumulatedTokens float64 `json:"offchain_accumulated_tokens"`
	TotalEarnedTokensAllTime  float64 `json:"total_earned_tokens_all_time"`
	PendingSolanaSync         bool    `json:"pending_solana_sync"`
}

// ── Ping result (from Rust miner) ─────────────────────────────────────────

type PingResultItem struct {
	MonitorID  string `json:"monitor_id"`
	LatencyMs  int    `json:"latency_ms"`
	StatusCode int    `json:"status_code"`
	GeoRegion  string `json:"geo_region"`
	JobID      string `json:"job_id"` // nonce for anti-cheat validation
}

type SubmitResultsRequest struct {
	RunnerPubkey string           `json:"runner_pubkey"`
	Signature    string           `json:"signature"` // ed25519 sig of payload
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
