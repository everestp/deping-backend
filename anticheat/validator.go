package anticheat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// validateAndConsumeNonce atomically GETs and DELetes the nonce in one round-trip.
var validateAndConsumeNonce = redis.NewScript(`
    local val = redis.call("GET", KEYS[1])
    if val == false then
        return 0
    end
    redis.call("DEL", KEYS[1])
    return 1
`)

type Validator struct {
	rdb *redis.Client
}

func NewValidator(rdb *redis.Client) *Validator {
	return &Validator{rdb: rdb}
}

// ValidateJobID atomically validates and consumes the nonce for a job_id.
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
func (v *Validator) CheckRateLimit(ctx context.Context, nodeID string, maxPerMinute int) error {
	key := fmt.Sprintf("ratelimit:node:%s", nodeID)

	pipe := v.rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	// Only set TTL on initial key creation to enforce an absolute 60s window
	pipe.ExpireNX(ctx, key, 60*time.Second)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("rate limit pipeline: %w", err)
	}
	if incr.Val() > int64(maxPerMinute) {
		return ErrRateLimitExceeded
	}
	return nil
}

// CheckIPAbuse detects same-IP multi-node Sybil attacks.
func (v *Validator) CheckIPAbuse(ctx context.Context, ip, nodeID string, maxNodes int) error {
	key := fmt.Sprintf("ipabuse:%s", ip)
	v.rdb.SAdd(ctx, key, nodeID)
	v.rdb.Expire(ctx, key, 24*time.Hour)

	count, err := v.rdb.SCard(ctx, key).Result()
	if err != nil {
		return nil
	}
	if int(count) > maxNodes {
		return ErrIPAbuse
	}
	return nil
}

// DetectFakeLatency flags suspiciously uniform total_us values.
func (v *Validator) DetectFakeLatency(ctx context.Context, nodeID string, latencyMs int) error {
	if latencyMs <= 0 {
		return nil
	}
	key := fmt.Sprintf("latency:streak:%s:%d", nodeID, latencyMs)
	count, err := v.rdb.Incr(ctx, key).Result()
	if err != nil {
		return nil
	}
	v.rdb.Expire(ctx, key, 5*time.Minute)
	if count > 10 {
		return ErrSuspiciousLatency
	}
	return nil
}

// DetectImpossibleUptime flags a node reporting 100% success across all probes
func (v *Validator) DetectImpossibleUptime(ctx context.Context, nodeID string) error {
	key := fmt.Sprintf("uptime:streak:%s", nodeID)
	count, err := v.rdb.Incr(ctx, key).Result()
	if err != nil {
		return nil
	}
	v.rdb.Expire(ctx, key, 5*time.Minute)
	if count > 50 {
		return ErrImpossibleUptime
	}
	return nil
}

func (v *Validator) ResetUptimeStreak(ctx context.Context, nodeID string) {
	v.rdb.Del(ctx, fmt.Sprintf("uptime:streak:%s", nodeID))
}

var (
	ErrInvalidNonce      = fmt.Errorf("anti-cheat: invalid or expired job_id nonce")
	ErrRateLimitExceeded = fmt.Errorf("anti-cheat: submission rate limit exceeded")
	ErrIPAbuse           = fmt.Errorf("anti-cheat: too many nodes from same IP")
	ErrSuspiciousLatency = fmt.Errorf("anti-cheat: suspiciously uniform latency pattern")
	ErrImpossibleUptime  = fmt.Errorf("anti-cheat: impossible uptime pattern detected")
)
