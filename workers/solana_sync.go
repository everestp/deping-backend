package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/services"
)

// StartSolanaSync consumes solana_sync_queue.
// For each payload it:
//  1. Checks idempotency table — skips if tx already confirmed (prevents double-pay on RPC retry)
//  2. Submits the add_reward Anchor instruction via Solana JSON-RPC
//  3. Records the confirmed tx signature in solana_sync_events
//  4. Clears pending_solana_sync flag on the runner
func StartSolanaSync(ctx context.Context, pool *pgxpool.Pool, rabbitCh *amqp.Channel, cfg *env.Config) {
	store := repositories.NewStorage(pool)

	go func() {
		msgs, err := rabbitCh.Consume("solana_sync_queue", "", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("[solana-sync] consume error: %v", err)
		}

		log.Println("[solana-sync] started")

		for {
			select {
			case <-ctx.Done():
				log.Println("[solana-sync] shutting down")
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				if err := processSyncMessage(ctx, msg, store, cfg); err != nil {
					log.Printf("[solana-sync] error: %v", err)
					_ = msg.Nack(false, true) // requeue for retry
				} else {
					_ = msg.Ack(false)
				}
			}
		}
	}()
}

func processSyncMessage(ctx context.Context, msg amqp.Delivery, store *repositories.Storage, cfg *env.Config) error {
	var payload services.SolanaSyncPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		_ = msg.Nack(false, false) // malformed — dead-letter
		return nil
	}

	// Convert float tokens → raw u64 (6 decimals for SPL token)
	const decimals = 1_000_000
	amountRaw := int64(payload.AmountTokens * decimals)

	// Submit to Solana via JSON-RPC (replace with your actual Anchor client)
	txSignature, err := submitAddReward(ctx, cfg.SolanaRPCURL, payload.RunnerPubkey, amountRaw)
	if err != nil {
		return fmt.Errorf("submit add_reward: %w", err)
	}

	// Check idempotency — skip if already recorded (protects against double Ack)
	exists, err := store.SolanaSync.ExistsBySignature(ctx, txSignature)
	if err != nil {
		return fmt.Errorf("idempotency check: %w", err)
	}
	if exists {
		log.Printf("[solana-sync] tx %s already recorded, skipping", txSignature)
		return nil
	}

	// Record confirmed sync
	if err := store.SolanaSync.RecordSync(ctx, payload.RunnerPubkey, txSignature, amountRaw); err != nil {
		return fmt.Errorf("record sync: %w", err)
	}

	// Clear pending flag on runner
	if err := store.Runners.SetPendingSync(ctx, payload.RunnerPubkey, false); err != nil {
		log.Printf("[solana-sync] warn: clear pending_sync for %s: %v", payload.RunnerPubkey, err)
	}

	log.Printf("[solana-sync] settled %s → tx %s (%.4f tokens)", payload.RunnerPubkey, txSignature, payload.AmountTokens)
	return nil
}

// submitAddReward is a thin JSON-RPC shim.
// In production replace with go-solana or anchor-go client that loads
// the keypair from HSM/KMS — never from a flat file.
func submitAddReward(ctx context.Context, rpcURL, runnerPubkey string, amountRaw int64) (string, error) {
	// This is a placeholder structure. Wire up your real Anchor instruction here.
	body, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendTransaction",
		"params":  []any{runnerPubkey, amountRaw},
	})

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := http.NewRequestWithContext(reqCtx, http.MethodPost, rpcURL, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	resp.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	httpResp, err := client.Do(resp)
	if err != nil {
		return "", fmt.Errorf("rpc call: %w", err)
	}
	defer httpResp.Body.Close()

	var result struct {
		Result string `json:"result"` // tx signature
	}
	if err := json.NewDecoder(httpResp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode rpc response: %w", err)
	}
	if result.Result == "" {
		return "", fmt.Errorf("empty tx signature from RPC")
	}

	return result.Result, nil
}
