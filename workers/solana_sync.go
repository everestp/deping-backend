package workers

import (
	"context"
	"crypto/sha256"
	"encoding/binary"

	"log"
	"time"

	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/db/repositories"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
)

const decimals = 1_000_000_000

func StartSolanaSync(ctx context.Context, pool *pgxpool.Pool, cfg *env.Config) {
	store := repositories.NewStorage(pool)
	rpcClient := rpc.New(cfg.SolanaRPCURL)

	log.Println("[solana-sync] worker started...")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// 1. Fetch pending events
				events, err := store.SolanaSync.FetchPending(ctx, 10)
				if err != nil {
					log.Printf("[solana-sync] fetch error: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}

				if len(events) == 0 {
					// No events, just sleep and poll again
					time.Sleep(5 * time.Second)
					continue
				}

				for _, event := range events {
					log.Printf("[solana-sync] processing event ID: %d for runner: %s", event.ID, event.RunnerPubkey)

					// 2. Lock event
					if err := store.SolanaSync.MarkProcessing(ctx, event.ID); err != nil {
						log.Printf("[solana-sync] mark processing error: %v", err)
						continue
					}

					// 3. Execute
					// event.Amount is likely already a float64 from your DB
sig, err := executeRewardTx(
    ctx,
    rpcClient,
    cfg,
    event.RunnerPubkey,
    event.Amount, // Pass the raw float64 directly
)
					if err != nil {
						log.Printf("[solana-sync] tx failed for event %d: %v", event.ID, err)
						_ = store.SolanaSync.MarkPendingAgain(ctx, event.ID)
						continue
					}

					// 4. Mark success
					if err := store.SolanaSync.MarkDone(ctx, event.ID, sig); err != nil {
						log.Printf("[solana-sync] mark done error: %v", err)
					} else {
						log.Printf("[solana-sync] success: %s -> %s", event.RunnerPubkey, sig)
					}
				}
			}
		}
	}()
}

func executeRewardTx(
    ctx context.Context,
    client *rpc.Client,
    cfg *env.Config,
    ownerPubkey string,
    amountHumanReadable float64,
) (string, error) {
    backendSigner, err := cfg.GetBackendPrivateKey()
    if err != nil {
        return "", err
    }

    programID := solana.MustPublicKeyFromBase58("DVicVozhh4y38dA6iCzfPp2c4xj5Q29mJq6HgF5Eufiz")
    nodePubkey := solana.MustPublicKeyFromBase58(ownerPubkey)
    emailHash := sha256.Sum256([]byte("3verestp@gmail.com"))

    nodePDA, _, err := solana.FindProgramAddress(
        [][]byte{[]byte("node"), nodePubkey.Bytes(), emailHash[:]},
        programID,
    )
    if err != nil {
        return "", err
    }

    // 1. DYNAMIC DISCRIMINATOR GENERATION
    // Use "global:add_reward" (snake_case) or "global:addReward" (camelCase)
    // Most Rust programs with the function name `add_reward` expect "global:add_reward"
    hashString := "global:add_reward"
    hash := sha256.Sum256([]byte(hashString))
    discriminator := hash[:8]

    // 2. AMOUNT CONVERSION
    amountRaw := uint64(amountHumanReadable * float64(decimals))

    // 3. PACK DATA
    data := make([]byte, 16)
    copy(data[0:8], discriminator)
    binary.LittleEndian.PutUint64(data[8:16], amountRaw)

    // 4. INSTRUCTION
    instruction := solana.NewInstruction(
        programID,
        solana.AccountMetaSlice{
            {PublicKey: nodePDA, IsSigner: false, IsWritable: true},
            {PublicKey: backendSigner.PublicKey(), IsSigner: true, IsWritable: false},
        },
        data,
    )

    // 5. BUILD & SEND
    recent, err := client.GetLatestBlockhash(ctx, rpc.CommitmentFinalized)
    if err != nil {
        return "", err
    }

    tx, err := solana.NewTransaction(
        []solana.Instruction{instruction},
        recent.Value.Blockhash,
        solana.TransactionPayer(backendSigner.PublicKey()),
    )
    if err != nil {
        return "", err
    }

    _, err = tx.Sign(func(key solana.PublicKey) *solana.PrivateKey {
        if key.Equals(backendSigner.PublicKey()) {
            return &backendSigner
        }
        return nil
    })
    if err != nil {
        return "", err
    }

    sig, err := client.SendTransaction(ctx, tx)
    if err != nil {
        return "", err
    }

    return sig.String(), nil
}