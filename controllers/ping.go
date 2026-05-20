package controllers

import (
	"encoding/json"
	"net/http"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/everestp/depin-backend/anticheat"
	"github.com/everestp/depin-backend/dto"
	"github.com/everestp/depin-backend/middleware"
	"github.com/everestp/depin-backend/services"
)

// ── Runner ─────────────────────────────────────────────────────────────────

type RunnerController struct{ svc services.RunnerService }

func NewRunnerController(svc services.RunnerService) *RunnerController {
	return &RunnerController{svc: svc}
}

func (c *RunnerController) Register(w http.ResponseWriter, r *http.Request) {
	var req dto.RegisterRunnerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	email := middleware.GetUserEmail(r)
	resp, err := c.svc.Register(r.Context(), email, &req)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}
	respondJSON(w, http.StatusCreated, resp)
}

func (c *RunnerController) Me(w http.ResponseWriter, r *http.Request) {
	pubkey := r.URL.Query().Get("pubkey")
	if pubkey == "" {
		respondError(w, http.StatusBadRequest, "pubkey query param required")
		return
	}
	resp, err := c.svc.GetByPubkey(r.Context(), pubkey)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, resp)
}

func (c *RunnerController) Heartbeat(w http.ResponseWriter, r *http.Request) {
	pubkey := r.URL.Query().Get("pubkey")
	if pubkey == "" {
		respondError(w, http.StatusBadRequest, "pubkey query param required")
		return
	}
	if err := c.svc.Heartbeat(r.Context(), pubkey); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, dto.MessageResponse{Message: "heartbeat recorded"})
}

// ── Reward ─────────────────────────────────────────────────────────────────

type RewardController struct{ svc services.RewardService }

func NewRewardController(svc services.RewardService) *RewardController {
	return &RewardController{svc: svc}
}

func (c *RewardController) Status(w http.ResponseWriter, r *http.Request) {
	pubkey := r.URL.Query().Get("pubkey")
	if pubkey == "" {
		respondError(w, http.StatusBadRequest, "pubkey query param required")
		return
	}
	resp, err := c.svc.GetStatus(r.Context(), pubkey)
	if err != nil {
		respondError(w, http.StatusNotFound, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, resp)
}

// ── Ping (REST fallback for miners not using gRPC) ─────────────────────────

// PingController handles POST /api/v1/results — the REST submission path.
// The primary path is gRPC (grpc/server.go). This endpoint exists for:
//   - miners with connectivity issues preventing a persistent gRPC stream
//   - integration testing without a running gRPC server
//   - future web-based lightweight miners
//
// It runs the same anti-cheat checks as the gRPC path then enqueues
// the payload to processing_queue for identical downstream handling.
type PingController struct {
	svc       services.PingLogService
	rabbitCh  *amqp.Channel
	validator *anticheat.Validator
}

func NewPingController(svc services.PingLogService, rabbitCh *amqp.Channel, validator *anticheat.Validator) *PingController {
	return &PingController{svc: svc, rabbitCh: rabbitCh, validator: validator}
}

// SubmitResults accepts a batch from the Rust miner via HTTP POST.
// Returns 202 immediately — validation and DB writes are asynchronous.
func (c *PingController) SubmitResults(w http.ResponseWriter, r *http.Request) {
	var req dto.SubmitResultsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.RunnerPubkey == "" || len(req.Results) == 0 {
		respondError(w, http.StatusBadRequest, "runner_pubkey and results are required")
		return
	}

	ctx := r.Context()

	// Rate limit check before doing any work
	if err := c.validator.CheckRateLimit(ctx, req.RunnerPubkey, 600); err != nil {
		respondError(w, http.StatusTooManyRequests, err.Error())
		return
	}

	// Per-result nonce validation — invalid nonces are silently dropped
	// (same behaviour as gRPC path — we don't fail the whole batch)
	validResults := req.Results[:0]
	for _, res := range req.Results {
		if err := c.validator.ValidateJobID(ctx, res.JobID); err != nil {
			continue // nonce expired, consumed, or fabricated — skip
		}
		validResults = append(validResults, res)
	}
	if len(validResults) == 0 {
		respondError(w, http.StatusUnprocessableEntity, "all job_ids were invalid or expired")
		return
	}
	req.Results = validResults

	body, err := services.MarshalResultPacket(&req)
	if err != nil {
		respondError(w, http.StatusInternalServerError, "failed to marshal payload")
		return
	}

	if err := c.rabbitCh.PublishWithContext(ctx, "", "processing_queue", false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		},
	); err != nil {
		respondError(w, http.StatusServiceUnavailable, "queue unavailable")
		return
	}

	respondJSON(w, http.StatusAccepted, dto.MessageResponse{Message: "results queued"})
}

// ── Shared helpers ─────────────────────────────────────────────────────────

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func respondError(w http.ResponseWriter, status int, msg string) {
	respondJSON(w, status, dto.ErrorResponse{Error: msg})
}
