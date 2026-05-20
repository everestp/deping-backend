package controllers

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/everestp/depin-backend/dto"
	"github.com/everestp/depin-backend/middleware"
	"github.com/everestp/depin-backend/services"
)

type MonitorController struct {
	svc services.MonitorService
}

func NewMonitorController(svc services.MonitorService) *MonitorController {
	return &MonitorController{svc: svc}
}

func (c *MonitorController) Create(w http.ResponseWriter, r *http.Request) {
	var req dto.CreateMonitorRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	ownerID := middleware.GetUserID(r)
	resp, err := c.svc.Create(r.Context(), ownerID, &req)
	if err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	respondJSON(w, http.StatusCreated, resp)
}

func (c *MonitorController) List(w http.ResponseWriter, r *http.Request) {
	ownerID := middleware.GetUserID(r)
	resp, err := c.svc.ListByOwner(r.Context(), ownerID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, resp)
}

func (c *MonitorController) Stats(w http.ResponseWriter, r *http.Request) {
	monitorID := chi.URLParam(r, "id")
	ownerID := middleware.GetUserID(r)
	resp, err := c.svc.Stats(r.Context(), monitorID, ownerID)
	if err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, resp)
}

func (c *MonitorController) Pause(w http.ResponseWriter, r *http.Request) {
	monitorID := chi.URLParam(r, "id")
	ownerID := middleware.GetUserID(r)
	if err := c.svc.Pause(r.Context(), monitorID, ownerID); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, dto.MessageResponse{Message: "monitor paused"})
}

func (c *MonitorController) Resume(w http.ResponseWriter, r *http.Request) {
	monitorID := chi.URLParam(r, "id")
	ownerID := middleware.GetUserID(r)
	if err := c.svc.Resume(r.Context(), monitorID, ownerID); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, dto.MessageResponse{Message: "monitor resumed"})
}

func (c *MonitorController) Delete(w http.ResponseWriter, r *http.Request) {
	monitorID := chi.URLParam(r, "id")
	ownerID := middleware.GetUserID(r)
	if err := c.svc.Delete(r.Context(), monitorID, ownerID); err != nil {
		respondError(w, http.StatusInternalServerError, err.Error())
		return
	}
	respondJSON(w, http.StatusOK, dto.MessageResponse{Message: "monitor deleted"})
}
