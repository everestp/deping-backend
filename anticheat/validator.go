package anticheat

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// validateAndConsumeNonce atomically GETs and DELetes the nonce in one round-trip.
// Returns 1 if the nonce existed and was consumed, 0 if missing/expired.
// Prevents any replay of the same job_id regardless of timing.
var validateAndConsumeNonce = redis.NewScript(`
	local val = redis.call("GET", KEYS[1])
	if val == false then
		return 0
	end
	redis.call("DEL", KEYS[1])
	return 1
`)

// Validator holds all anti-cheat checks. Inject one instance via app.New()
// and pass it to both the gRPC server and the REST result handler.
type Validator struct {
	rdb *redis.Client
}

func NewValidator(rdb *redis.Client) *Validator {
	return &Validator{rdb: rdb}
}

// ValidateJobID atomically validates and consumes the nonce for a job_id.
//
// job_id format (set by workers/scheduler.go):
//
//	"<monitor_uuid>:<region>:<unix_ts_seconds>"
//
// The nonce is stored under "nonce:<job_id>" with TTL = 2 × check_interval.
// A result submitted after TTL expiry or a second submission of the same job_id
// both return ErrInvalidNonce and are rejected — no reward is issued.
func (v *Validator) ValidateJobID(ctx context.Context, jobID string) error {
	if jobID == "" {
		return ErrInvalidNonce
	}
	nonceKey := fmt.Sprintf("nonce:%s", jobID)
	result, err := validateAndConsumeNonce.Run(ctx, v.rdb, []string{nonceKey}).Int()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("nonce lua script: %w", err)
	}
	if result == 0 {
		return ErrInvalidNonce
	}
	return nil
}

// CheckRateLimit enforces per-node submission rate limiting.
// node_id is the ed25519 public key hex from ProbeResult / MinerRegister.
// maxPerMinute should be >= (number of active monitors × regions) for the node.
func (v *Validator) CheckRateLimit(ctx context.Context, nodeID string, maxPerMinute int) error {
	key := fmt.Sprintf("ratelimit:node:%s", nodeID)
	pipe := v.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 60)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("rate limit pipeline: %w", err)
	}
	if incr.Val() > int64(maxPerMinute) {
		return ErrRateLimitExceeded
	}
	return nil
}

// CheckIPAbuse detects same-IP multi-node Sybil attacks.
// Uses a Redis SSET to track distinct node_ids per IP over a 24h window.
// More than maxNodes distinct nodes from one IP triggers the error.
func (v *Validator) CheckIPAbuse(ctx context.Context, ip, nodeID string, maxNodes int) error {
	key := fmt.Sprintf("ipabuse:%s", ip)
	v.rdb.SAdd(ctx, key, nodeID)
	v.rdb.Expire(ctx, key, 24*60*60)
	count, err := v.rdb.SCard(ctx, key).Result()
	if err != nil {
		return nil // non-fatal — log externally
	}
	if int(count) > maxNodes {
		return ErrIPAbuse
	}
	return nil
}

// DetectFakeLatency flags suspiciously uniform total_us values.
// ProbeResult carries total_us in microseconds; we bucket to nearest millisecond
// for streak detection (real variance means < 10 identical ms values in 5 min).
func (v *Validator) DetectFakeLatency(ctx context.Context, nodeID string, latencyMs int) error {
	// Ignore implausibly fast or zero values (connection failures)
	if latencyMs <= 0 {
		return nil
	}
	key := fmt.Sprintf("latency:streak:%s:%d", nodeID, latencyMs)
	count, err := v.rdb.Incr(ctx, key).Result()
	if err != nil {
		return nil
	}
	v.rdb.Expire(ctx, key, 300) // 5-minute window
	if count > 10 {
		return ErrSuspiciousLatency
	}
	return nil
}

// DetectImpossibleUptime flags a node reporting 100% success across all probes
// in a short window — statistically impossible for a residential connection.
func (v *Validator) DetectImpossibleUptime(ctx context.Context, nodeID string) error {
	key := fmt.Sprintf("uptime:streak:%s", nodeID)
	count, err := v.rdb.Incr(ctx, key).Result()
	if err != nil {
		return nil
	}
	v.rdb.Expire(ctx, key, 300) // 5-minute window
	if count > 50 { // 50 consecutive successes without any failure
		return ErrImpossibleUptime
	}
	return nil
}

// ResetUptimeStreak resets the uptime streak counter when a failure is seen.
// Call this from handleProbeResult when ProbeResult.success == false.
func (v *Validator) ResetUptimeStreak(ctx context.Context, nodeID string) {
	v.rdb.Del(ctx, fmt.Sprintf("uptime:streak:%s", nodeID))
}

// ── Sentinel errors ────────────────────────────────────────────────────────

var (
	ErrInvalidNonce      = fmt.Errorf("anti-cheat: invalid or expired job_id nonce")
	ErrRateLimitExceeded = fmt.Errorf("anti-cheat: submission rate limit exceeded")
	ErrIPAbuse           = fmt.Errorf("anti-cheat: too many nodes from same IP")
	ErrSuspiciousLatency = fmt.Errorf("anti-cheat: suspiciously uniform latency pattern")
	ErrImpossibleUptime  = fmt.Errorf("anti-cheat: impossible uptime pattern detected")
)
