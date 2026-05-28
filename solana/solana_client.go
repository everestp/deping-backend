package solana

import (
	"context"
	"fmt"
	"strconv"


	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// Client wraps the native solana-go RPC client infrastructure with clean methods
// to serve dashboard metrics lookup routines safely.
type Client struct {
	rpcClient *rpc.Client
}

// NewClient handles standard initialization of the native Solana SDK wrapper.
func NewClient(rpcURL string) *Client {
	return &Client{
		rpcClient: rpc.New(rpcURL),
	}
}

// GetTokenAccountBalance returns the exact raw uint64 SPL token balance
// for an active token account using native commitment layout protocols.
func (c *Client) GetTokenAccountBalance(ctx context.Context, tokenAccountPubkeyStr string) (uint64, error) {
	// Parse string into native cryptographic type slice
	accountKey, err := solana.PublicKeyFromBase58(tokenAccountPubkeyStr)
	if err != nil {
		return 0, fmt.Errorf("solana.Client.GetTokenAccountBalance - invalid pubkey string: %w", err)
	}

	// Fetch token metrics using finalized block state context rules
	out, err := c.rpcClient.GetTokenAccountBalance(
		ctx,
		accountKey,
		rpc.CommitmentFinalized,
	)
	if err != nil {
		return 0, fmt.Errorf("solana.Client.GetTokenAccountBalance RPC execution failed: %w", err)
	}

	if out == nil || out.Value == nil {
		return 0, fmt.Errorf("solana.Client.GetTokenAccountBalance returned an empty result buffer")
	}

	// Safely parse the string value payload into a primitive unit
	balance, err := strconv.ParseUint(out.Value.Amount, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse string balance to native uint64: %w", err)
	}

	return balance, nil
}

// GetSOLBalance returns the absolute native Lamports balance matching a wallet target identity.
func (c *Client) GetSOLBalance(ctx context.Context, walletPubkeyStr string) (uint64, error) {
	walletKey, err := solana.PublicKeyFromBase58(walletPubkeyStr)
	if err != nil {
		return 0, fmt.Errorf("solana.Client.GetSOLBalance - invalid pubkey string: %w", err)
	}

	out, err := c.rpcClient.GetBalance(
		ctx,
		walletKey,
		rpc.CommitmentFinalized,
	)
	if err != nil {
		return 0, fmt.Errorf("solana.Client.GetSOLBalance RPC execution failed: %w", err)
	}

	if out == nil {
		return 0, fmt.Errorf("solana.Client.GetSOLBalance returned an unreadable response body")
	}

	return out.Value, nil
}
