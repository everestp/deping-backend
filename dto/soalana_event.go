package dto

import "time"

type SolanaSyncEvent struct {
	ID           string     `json:"id"`
	RunnerPubkey string     `json:"runner_pubkey"`
	Amount       float64    `json:"amount"`
	Status       string     `json:"status"` // PENDING | PROCESSING | DONE | FAILED
	RetryCount   int        `json:"retry_count"`
	TxSignature  *string    `json:"tx_signature"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}