package workers

import (
	"context"
	"encoding/json"
    "encoding/binary"
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
				if !ok {
					return
				}

				if err := processSyncMessage(ctx, msg, store, cfg); err != nil {
					log.Printf("[solana-sync] processing error: %v", err)

					// Reject malformed data automatically to prevent infinite dead-loops
					if err.Error() == "malformed json payload" || err.Error() == "runner node has not been initialized on-chain yet" {
						_ = msg.Ack(false)
					} else {
						// Back off and requeue transient RPC infrastructure errors gracefully
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
	var payload SolanaSyncPayload
	if err := json.Unmarshal(msg.Body, &payload); err != nil {
		return fmt.Errorf("malformed json payload")
	}

	if payload.AmountTokens < 10.0 {
		log.Printf("[solana-sync] Skipping node %s: reward %.4f < 10.0 threshold", payload.RunnerPubkey, payload.AmountTokens)
		return nil
	}

	programID, err := solana.PublicKeyFromBase58("EA4pKJ33F2p4oQyKNcCGMBptjSgbHQzCz2H8QgHbYAgR")
	if err != nil {
		return fmt.Errorf("invalid program id configuration: %w", err)
	}

	runner, err := store.Runners.FindByPubkey(ctx, payload.RunnerPubkey)
	if err != nil {
		return fmt.Errorf("failed to fetch tracking context from database: %w", err)
	}
	if runner.NodePubkey == nil || *runner.NodePubkey == "" {
		return fmt.Errorf("runner node has not been initialized on-chain yet")
	}

	nodeAccountPDA, err := solana.PublicKeyFromBase58(*runner.NodePubkey)
	if err != nil {
		return fmt.Errorf("invalid saved node pda public key string: %w", err)
	}

	const decimals = 1_000_000_000
	amountRaw := uint64(payload.AmountTokens * decimals)

	rpcClient := rpc.New(cfg.SolanaRPCURL)

	// 1. Submit and explicitly AWAIT chain confirmation before database updates
	txSignature, err := submitAnchorAddReward(ctx, rpcClient, cfg, programID, nodeAccountPDA, amountRaw)
	if err != nil {
		return fmt.Errorf("onchain transaction execution failed: %w", err)
	}

	// 2. Local idempotency registry lock check
	exists, err := store.SolanaSync.ExistsBySignature(ctx, txSignature)
	if err != nil {
		return fmt.Errorf("idempotency check database timeout: %w", err)
	}
	if exists {
		log.Printf("[solana-sync] Transaction %s already processed locally. Skipping.", txSignature)
		return nil
	}

	// 3. Write accounting event log
	if err := store.SolanaSync.RecordSync(ctx, payload.RunnerPubkey, txSignature, int64(amountRaw)); err != nil {
		return fmt.Errorf("failed to log sync event: %w", err)
	}

	// 4. Release execution lock state
	_ = store.Runners.SetPendingSync(ctx, payload.RunnerPubkey, false)
	log.Printf("[SUCCESS] Settled %f tokens to runner %s. Tx: %s", payload.AmountTokens, payload.RunnerPubkey, txSignature)

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
