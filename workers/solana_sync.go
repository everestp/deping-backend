package workers

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

const decimals = 1_000_000_000

// ─────────────────────────────────────────────
// WORKER START
// ─────────────────────────────────────────────

func StartSolanaSync(ctx context.Context, pool *pgxpool.Pool, cfg *env.Config) {

	store := repositories.NewStorage(pool)
	rpcClient := rpc.New(cfg.SolanaRPCURL)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			// 1. Fetch pending events (DB queue)
			events, err := store.SolanaSync.FetchPending(ctx, 10)
			if err != nil {
				log.Println("[solana-sync] fetch error:", err)
				time.Sleep(2 * time.Second)
				continue
			}

			for _, event := range events {

				// 2. Lock event for processing
				_ = store.SolanaSync.MarkProcessing(ctx, event.ID)

				// 3. Execute Solana transaction
				sig, err := executeRewardTx(
					ctx,
					rpcClient,
					cfg,
					event.RunnerPubkey,
					uint64(event.Amount*decimals),
				)

				if err != nil {
					log.Println("[solana-sync] tx failed:", err)

					_ = store.SolanaSync.MarkPendingAgain(ctx, event.ID)
					continue
				}

				// 4. Mark success
				_ = store.SolanaSync.MarkDone(ctx, event.ID, sig)

				log.Printf("[solana-sync] success: %s -> %s", event.RunnerPubkey, sig)
			}

			time.Sleep(2 * time.Second)
		}
	}()
}

// ─────────────────────────────────────────────
// SOLANA EXECUTION
// ─────────────────────────────────────────────

func executeRewardTx(
	ctx context.Context,
	client *rpc.Client,
	cfg *env.Config,
	ownerPubkey string,
	amountRaw uint64,
) (string, error) {

	backendSigner, err := cfg.GetBackendPrivateKey()
	if err != nil {
		return "", fmt.Errorf("invalid backend signer: %w", err)
	}

	programID := solana.MustPublicKeyFromBase58(env.Load().BackendPrivateKeyHex)
	nodePubkey := solana.MustPublicKeyFromBase58(ownerPubkey)

	// optional PDA logic (keep if your program needs it)
	emailHash := sha256.Sum256([]byte(ownerPubkey))
	_, nodePDA, _ := solana.FindProgramAddress(
		[][]byte{
			[]byte("node"),
			nodePubkey.Bytes(),
			emailHash[:],
		},
		programID,
	)
fmt.Printf("This is the  nodePDA",nodePDA)
	// instruction data (Anchor)
	discriminator := [8]byte{108, 14, 219, 10, 116, 2, 237, 204}
	data := make([]byte, 16)
	copy(data[0:8], discriminator[:])
	binary.LittleEndian.PutUint64(data[8:16], amountRaw)

	instruction := solana.NewInstruction(
		programID,
		solana.AccountMetaSlice{
			solana.NewAccountMeta(programID, true, false),
			solana.NewAccountMeta(backendSigner.PublicKey(), false, true),
		},
		data,
	)

	recent, err := client.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return "", fmt.Errorf("blockhash error: %w", err)
	}

	tx, err := solana.NewTransaction(
		[]solana.Instruction{instruction},
		recent.Value.Blockhash,
		solana.TransactionPayer(backendSigner.PublicKey()),
	)
	if err != nil {
		return "", fmt.Errorf("tx build error: %w", err)
	}

	_, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
		if key.Equals(backendSigner.PublicKey()) {
			return &backendSigner
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("sign error: %w", err)
	}

	sig, err := client.SendTransaction(ctx, tx)
	if err != nil {
		return "", fmt.Errorf("send error: %w", err)
	}

	// confirm
	for i := 0; i < 30; i++ {
		st, err := client.GetSignatureStatuses(ctx, false, sig)
		if err == nil && st != nil && len(st.Value) > 0 && st.Value[0] != nil {
			if st.Value[0].ConfirmationStatus == rpc.ConfirmationStatusFinalized {
				if st.Value[0].Err != nil {
					return "", fmt.Errorf("tx failed on-chain: %v", st.Value[0].Err)
				}
				return sig.String(), nil
			}
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(1 * time.Second):
		}
	}

	return "", fmt.Errorf("tx timeout: %s", sig)
}