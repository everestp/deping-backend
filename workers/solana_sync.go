package workers

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/everestp/depin-backend/config/env"
    "github.com/everestp/depin-backend/db/repositories"
    "github.com/everestp/depin-backend/services"
    "github.com/jackc/pgx/v5/pgxpool"
    amqp "github.com/rabbitmq/amqp091-go"
)

func StartSolanaSync(ctx context.Context, pool *pgxpool.Pool, rabbitCh *amqp.Channel, cfg *env.Config) {
    store := repositories.NewStorage(pool)

    go func() {
        msgs, err := rabbitCh.Consume("solana_sync_queue", "", false, false, false, false, nil)
        if err != nil {
            log.Fatalf("[solana-sync] consume error: %v", err)
        }

        for {
            select {
            case <-ctx.Done():
                return
            case msg, ok := <-msgs:
                if !ok { return }

                if err := processSyncMessage(ctx, msg, store, cfg); err != nil {
                    log.Printf("[solana-sync] processing error: %v", err)

                    // Permanent logic errors: Ack to discard
                    if err.Error() == "empty tx signature from RPC" || err.Error() == "RPC logic error" {
                        _ = msg.Ack(false)
                    } else {
                        // Transient network errors: Nack and requeue with delay
                        time.Sleep(5 * time.Second)
                        _ = msg.Nack(false, true)
                    }
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
        return fmt.Errorf("malformed json: %w", err)
    }

    // 🎯 Threshold logic: Ignore rewards < 10
    if payload.AmountTokens < 10.0 {
        log.Printf("[solana-sync] Skipping node %s: reward %.4f < 10.0 threshold", payload.RunnerPubkey, payload.AmountTokens)
        return nil
    }

    const decimals = 1_000_000
    amountRaw := int64(payload.AmountTokens * decimals)

    // Call RPC
    txSignature, err := submitAddReward(ctx, cfg.SolanaRPCURL, payload.RunnerPubkey, amountRaw)
    if err != nil {
        return err
    }

    // Idempotency: Check if signature already processed
    exists, err := store.SolanaSync.ExistsBySignature(ctx, txSignature)
    if err != nil {
        return fmt.Errorf("idempotency check: %w", err)
    }
    if exists {
        return nil
    }

    // Commit settlement
    if err := store.SolanaSync.RecordSync(ctx, payload.RunnerPubkey, txSignature, amountRaw); err != nil {
        return fmt.Errorf("db record error: %w", err)
    }

    _ = store.Runners.SetPendingSync(ctx, payload.RunnerPubkey, false)
    return nil
}

func submitAddReward(ctx context.Context, rpcURL, runnerPubkey string, amountRaw int64) (string, error) {
    // Note: ensure this method matches your Anchor program's expected JSON format
    body, _ := json.Marshal(map[string]any{
        "jsonrpc": "2.0",
        "id":      1,
        "method":  "sendTransaction", // Ensure this method is correct for your custom program
        "params":  []any{runnerPubkey, amountRaw},
    })

    req, _ := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(body))
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return "", err
    }
    defer resp.Body.Close()

    var res struct {
        Result string `json:"result"`
        Error  *struct{ Message string } `json:"error"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
        return "", err
    }

    if res.Error != nil {
        return "", fmt.Errorf("RPC logic error: %s", res.Error.Message)
    }
    if res.Result == "" {
        return "", fmt.Errorf("empty tx signature from RPC")
    }

    return res.Result, nil
}
