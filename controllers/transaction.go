package controllers

import (
    "encoding/json"
    "net/http"
    "github.com/everestp/depin-backend/solana" // Your wrapper
)

type TransactionController struct {
    solanaClient *solana.Client
}

func NewTransactionController(sc *solana.Client) *TransactionController {
    return &TransactionController{solanaClient: sc}
}

type ValidateRequest struct {
    Signature      string `json:"signature"`
    ExpectedAmount uint64 `json:"expected_amount"`
}

func (c *TransactionController) ValidateTransaction(w http.ResponseWriter, r *http.Request) {
    var req ValidateRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        http.Error(w, "Invalid input", http.StatusBadRequest)
        return
    }

    // Call the wrapper method we created previously
    isValid, err := c.solanaClient.ValidateOnChainAmount(r.Context(), req.Signature, req.ExpectedAmount)
    if err != nil {
        // Log the error internally and return a clear message to the frontend
        http.Error(w, "Validation error: "+err.Error(), http.StatusInternalServerError)
        return
    }

    // Send back the status
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "valid": isValid,
        "sig":   req.Signature,
    })
}
