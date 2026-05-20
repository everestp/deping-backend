package solana

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Client is a minimal Solana JSON-RPC wrapper scoped to what the sync worker needs.
// Replace the sendTransaction body with your real Anchor instruction serialization
// (use github.com/gagliardetto/solana-go for full transaction building).
type Client struct {
	rpcURL     string
	httpClient *http.Client
}

func NewClient(rpcURL string) *Client {
	return &Client{
		rpcURL: rpcURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// AddReward submits the add_reward Anchor instruction on behalf of the Go backend.
// Returns the confirmed transaction signature.
//
// Production checklist:
//   - Load keypair from HSM/KMS — NEVER a flat file in production
//   - Use Helius / Triton / QuickNode RPC — never public mainnet endpoint
//   - Verify authority == ADMIN_WALLET before signing
//   - Use checked_add on-chain to prevent overflow (already in your Anchor program)
func (c *Client) AddReward(ctx context.Context, runnerPubkey string, amountRaw int64) (string, error) {
	// TODO: Replace with real Anchor instruction via solana-go:
	//
	//   tx, err := anchor.NewAddRewardInstruction(
	//       amountRaw,
	//       nodePDA,
	//       authorityKey,
	//   ).ValidateAndBuild()
	//   sig, err := client.SendAndConfirmTransaction(ctx, tx)
	//
	// For now we send a stub RPC call so the worker compiles and runs.
	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "sendTransaction",
		"params":  []any{runnerPubkey, amountRaw},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal rpc payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build rpc request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("rpc call failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Result string `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode rpc response: %w", err)
	}
	if result.Error != nil {
		return "", fmt.Errorf("rpc error %d: %s", result.Error.Code, result.Error.Message)
	}
	if result.Result == "" {
		return "", fmt.Errorf("empty tx signature in rpc response")
	}

	return result.Result, nil
}

// GetBalance returns the SPL token balance for a pubkey (for dashboard display).
func (c *Client) GetBalance(ctx context.Context, pubkey string) (uint64, error) {
	payload := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "getTokenAccountBalance",
		"params":  []any{pubkey},
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.rpcURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var result struct {
		Result struct {
			Value struct {
				Amount string `json:"amount"`
			} `json:"value"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	var balance uint64
	fmt.Sscanf(result.Result.Value.Amount, "%d", &balance)
	return balance, nil
}
