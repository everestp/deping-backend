package anticheat

import (
    "context"
    "crypto/ed25519"
    "encoding/hex"
    "errors"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
)

// Lua script to ensure atomic check-and-delete (prevents race conditions)
var validateAndConsumeNonce = redis.NewScript(`
    local val = redis.call("GET", KEYS[1])
    if val == false then return 0 end
    redis.call("DEL", KEYS[1])
    return 1
`)

type Validator struct {
    rdb *redis.Client
}

func NewValidator(rdb *redis.Client) *Validator {
    return &Validator{rdb: rdb}
}

// CheckNonceExists is a helper to verify if a nonce exists in Redis (for debugging)
func (v *Validator) CheckNonceExists(ctx context.Context, nonce string) (bool, error) {
    key := fmt.Sprintf("task_nonce:%s", nonce)
    val, err := v.rdb.Exists(ctx, key).Result()
    return val > 0, err
}

func (v *Validator) ValidateTaskNonce(ctx context.Context, nonce string) error {
    if nonce == "" {
        return ErrInvalidNonce
    }

    nonceKey := fmt.Sprintf("task_nonce:%s", nonce)

    // Execute the Lua script
    result, err := validateAndConsumeNonce.Run(ctx, v.rdb, []string{nonceKey}).Int()
    if err != nil {
        return fmt.Errorf("redis error: %w", err)
    }

    if result == 0 {
        return ErrInvalidNonce
    }
    return nil
}

// VerifyIdentity validates Ed25519 signatures
func (v *Validator) VerifyIdentity(pubKeyHex, message, sigHex string) error {
    pubKeyBytes, err := hex.DecodeString(pubKeyHex)
    if err != nil { return errors.New("invalid public key hex") }

    sigBytes, err := hex.DecodeString(sigHex)
    if err != nil { return errors.New("invalid signature hex") }

    if len(pubKeyBytes) != ed25519.PublicKeySize {
        return fmt.Errorf("pubkey: expected %d bytes, got %d", ed25519.PublicKeySize, len(pubKeyBytes))
    }
    if len(sigBytes) != ed25519.SignatureSize {
        return fmt.Errorf("sig: expected %d bytes, got %d", ed25519.SignatureSize, len(sigBytes))
    }

    if !ed25519.Verify(pubKeyBytes, []byte(message), sigBytes) {
        return errors.New("cryptographic signature mismatch")
    }
    return nil
}

func (v *Validator) VerifySignature(pubKeyHex string, message string, sigBytes []byte) error {
    // 1. Decode the hex-encoded Public Key
    pubKeyBytes, err := hex.DecodeString(pubKeyHex)
    if err != nil {
        return errors.New("invalid public key hex")
    }

    // 2. Validate Key Size
    if len(pubKeyBytes) != ed25519.PublicKeySize {
        return fmt.Errorf("pubkey: expected %d bytes, got %d", ed25519.PublicKeySize, len(pubKeyBytes))
    }

    // 3. Validate Signature Size (Standard Ed25519 signature is 64 bytes)
    if len(sigBytes) != ed25519.SignatureSize {
        return fmt.Errorf("sig: expected %d bytes, got %d", ed25519.SignatureSize, len(sigBytes))
    }

    // 4. Verify
    if !ed25519.Verify(pubKeyBytes, []byte(message), sigBytes) {
        return errors.New("cryptographic signature mismatch")
    }

    return nil
}

func (v *Validator) CheckRateLimit(ctx context.Context, nodeID string, maxPerMinute int) error {
    key := fmt.Sprintf("ratelimit:node:%s", nodeID)
    pipe := v.rdb.Pipeline()
    incr := pipe.Incr(ctx, key)
    pipe.ExpireNX(ctx, key, 60*time.Second)
    _, err := pipe.Exec(ctx)
    if err != nil { return err }
    if incr.Val() > int64(maxPerMinute) { return ErrRateLimitExceeded }
    return nil
}

func (v *Validator) DetectFakeLatency(ctx context.Context, nodeID string, latencyMs int) error {
    key := fmt.Sprintf("latency:streak:%s:%d", nodeID, latencyMs)
    count, _ := v.rdb.Incr(ctx, key).Result()
    v.rdb.Expire(ctx, key, 5*time.Minute)
    if count > 10 { return ErrSuspiciousLatency }
    return nil
}

var (
    ErrInvalidNonce      = fmt.Errorf("anti-cheat: invalid or expired task nonce")
    ErrRateLimitExceeded = fmt.Errorf("anti-cheat: submission rate limit exceeded")
    ErrSuspiciousLatency = fmt.Errorf("anti-cheat: suspiciously uniform latency pattern")
)
