package workers

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"
	"github.com/jackc/pgx/v5/pgxpool"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// SolanaSyncPayload mirrors your core dynamic service JSON transfer targets.
type SolanaSyncPayload struct {
	RunnerPubkey string  `json:"runner_pubkey"`
	AmountTokens float64 `json:"amount_tokens"`
}

// Anchor instruction discriminator for "add_reward"
var addRewardDiscriminator = [8]byte{108, 14, 219, 10, 116, 2, 237, 204}

func ProcessSyncMessage(ctx context.Context, msg amqp.Delivery, store *repositories.Storage, rabbitCh *amqp.Channel, cfg *env.Config) error {
	var payload SolanaSyncPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		return fmt.Errorf("malformed json payload")
	}

	// 1. Extract Retry Count from RabbitMQ Headers
	retryCount := 0
	if val, ok := msg.Headers["retry_count"]; ok {
		switch v := val.(type) {
		case int32: retryCount = int(v)
		case int64: retryCount = int(v)
		}
	}

	// 2. Fetch runner record
	runner, err := store.Runners.FindByPubkey(ctx, payload.RunnerPubkey)
	if err != nil {
		return fmt.Errorf("database lookup failed: %w", err)
	}

identityKey, err := solana.PublicKeyFromBase58(*runner.NodePubkey)
	if err != nil {
		return fmt.Errorf("invalid identity key format: %w", err)
	}

	// 3. Derive PDA correctly using 32-byte SHA-256 hash of the email
	emailHash := sha256.Sum256([]byte(runner.OwnerEmail))
	programID := solana.MustPublicKeyFromBase58("EA4pKJ33F2p4oQyKNcCGMBptjSgbHQzCz2H8QgHbYAgR")

	nodeAccountPDA, _, err := solana.FindProgramAddress(
		[][]byte{
			[]byte("node"),
			identityKey.Bytes(),
			emailHash[:], // Use the 32-byte array, not the raw string
		},
		programID,
	)
	if err != nil {
		return fmt.Errorf("failed to derive PDA: %w", err)
	}

	// 4. Execution
	const decimals = 1_000_000_000
	amountRaw := uint64(payload.AmountTokens * decimals)
	rpcClient := rpc.New(cfg.SolanaRPCURL)

	txSignature, err := submitAnchorAddReward(ctx, rpcClient, cfg, programID, nodeAccountPDA, amountRaw)
	if err != nil {
		// Handle transient errors with retry logic
		if retryCount < 10 {
			_ = rabbitCh.PublishWithContext(ctx, "", "solana_sync_queue", false, false, amqp.Publishing{
				ContentType:  "application/json",
				Body:         msg.Body,
				DeliveryMode: amqp.Persistent,
				Headers:      amqp.Table{"retry_count": int32(retryCount + 1)},
			})
			_ = msg.Ack(false)
			return fmt.Errorf("retry %d: chain execution failed: %w", retryCount+1, err)
		}
		// Max retries reached: Flag for manual intervention
		_ = store.Runners.SetPendingSync(ctx, payload.RunnerPubkey, true)
		_ = msg.Ack(false)
		return fmt.Errorf("max retries reached for %s", payload.RunnerPubkey)
	}

	// 5. Atomic Finalization
	err = store.SolanaSync.FinalizeSync(ctx, payload.RunnerPubkey, txSignature, int64(amountRaw))
	if err != nil {
		return fmt.Errorf("failed to finalize database state: %w", err)
	}

	log.Printf("[SUCCESS] Settled %f tokens for %s (PDA: %s). Tx: %s", payload.AmountTokens, payload.RunnerPubkey, nodeAccountPDA, txSignature)
	_ = msg.Ack(false)
	return nil
}

func submitAnchorAddReward(
	ctx context.Context,
	client *rpc.Client,
	cfg *env.Config,
	programID solana.PublicKey,
	nodePDA solana.PublicKey,
	amountRaw uint64,
) (string, error) {
	// Extract the native structural key pairs straight from our validated environment hex string
	backendSigner, err := cfg.GetBackendPrivateKey()
	if err != nil {
		return "", fmt.Errorf("invalid backend signer key structure: %w", err)
	}

	// Allocate 16 bytes: 8 bytes Anchor discriminator + 8 bytes uint64 amount
	instructionData := make([]byte, 16)
	copy(instructionData[0:8], addRewardDiscriminator[:])

	// ✅ Fixed: Using Go's native encoding/binary library for safe, zero-allocation little-endian packing
	binary.LittleEndian.PutUint64(instructionData[8:16], amountRaw)

	instruction := solana.NewInstruction(
		programID,
		solana.AccountMetaSlice{
			solana.NewAccountMeta(nodePDA, true, false),
			solana.NewAccountMeta(backendSigner.PublicKey(), false, true),
		},
		instructionData,
	)

	recent, err := client.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return "", fmt.Errorf("failed to fetch latest blockhash: %w", err)
	}

	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recent.Value.Blockhash,
		solana.TransactionPayer(backendSigner.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("failed to assemble transaction envelope: %w", err)
	}

	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(backendSigner.PublicKey()) {
			return &backendSigner
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to generate signature: %w", err)
	}

	// Broadcast transaction to network cluster nodes
	sig, err := client.SendTransaction(ctx, tx)
	if err != nil {
		return "", fmt.Errorf("rpc broadcast execution rejected: %w", err)
	}

	// POLL/AWAIT Confirmation: Ensure transaction is finalized on-chain before returning
	// This prevents sync state inconsistencies if the RPC node drops or delays confirmation.
	ok := false
	for i := 0; i < 30; i++ {
		resp, err := client.GetSignatureStatuses(ctx, false, sig)
		if err == nil && resp != nil && len(resp.Value) > 0 && resp.Value[0] != nil {
			if resp.Value[0].ConfirmationStatus == rpc.ConfirmationStatusFinalized || resp.Value[0].ConfirmationStatus == rpc.ConfirmationStatusConfirmed {
				if resp.Value[0].Err == nil {
					ok = true
					break
				}
				return "", fmt.Errorf("transaction executed but failed on-chain: %v", resp.Value[0].Err)
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	if !ok {
		return "", fmt.Errorf("transaction %s broadcasted but confirmation timed out", sig)
	}

	return sig.String(), nil
}


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

                // Process using the function you provided
                if err := ProcessSyncMessage(ctx, msg, store, rabbitCh, cfg); err != nil {
                    log.Printf("[solana-sync] processing error: %v", err)
                }
            }
        }
    }()
}
