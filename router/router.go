package router

import (
    "net/http"
    "time"

    "github.com/go-chi/chi/v5"
    chimiddleware "github.com/go-chi/chi/v5/middleware"
    "github.com/go-chi/cors"

    "github.com/everestp/depin-backend/config/env"
    "github.com/everestp/depin-backend/controllers"
    "github.com/everestp/depin-backend/middleware"
)

func New(
    cfg *env.Config,
    userCtrl *controllers.UserController,
    monitorCtrl *controllers.MonitorController,
    runnerCtrl *controllers.RunnerController,
    rewardCtrl *controllers.RewardController,
    pingCtrl *controllers.PingController,
    txCtrl *controllers.TransactionController, // Comma added here!
) http.Handler {
    r := chi.NewRouter()

    // ── Global middleware ──────────────────────────────────────────────────
    r.Use(chimiddleware.RequestID)
    r.Use(chimiddleware.RealIP)
    r.Use(chimiddleware.Logger)
    r.Use(chimiddleware.Recoverer)
    r.Use(chimiddleware.Timeout(30 * time.Second))
    r.Use(cors.Handler(cors.Options{
        AllowedOrigins:   []string{"https://*", "http://*"},
        AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
        AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
        AllowCredentials: true,
        MaxAge:           30000,
    }))

    // ── Health ─────────────────────────────────────────────────────────────
    r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        _, _ = w.Write([]byte(`{"status":"ok"}`))
    })

    // ── Auth & Protected Middleware ───────────────────────────────────────
    r.Route("/api/v1/auth", func(r chi.Router) {
        r.Post("/register", userCtrl.Register)
        r.Post("/login", userCtrl.Login)
    })

    jwt := middleware.JWTMiddleware(cfg.JWTSecret)

    // ── Routes ────────────────────────────────────────────────────────────
    r.Group(func(r chi.Router) {
        r.Use(jwt)

        r.Route("/api/v1/monitors", func(r chi.Router) {
            r.Post("/", monitorCtrl.Create)
            r.Get("/", monitorCtrl.List)
            r.Get("/{id}/stats", monitorCtrl.Stats)
            r.Put("/{id}/pause", monitorCtrl.Pause)
            r.Put("/{id}/resume", monitorCtrl.Resume)
            r.Delete("/{id}", monitorCtrl.Delete)
        })

        r.Route("/api/v1/runner", func(r chi.Router) {
            r.Post("/register", runnerCtrl.Register)
            r.Get("/me", runnerCtrl.Me)
            r.Post("/heartbeat", runnerCtrl.Heartbeat)
        })

        r.Route("/api/v1/results", func(r chi.Router) {
            r.Post("/", pingCtrl.SubmitResults)
        })

        r.Route("/api/v1/validate", func(r chi.Router) {
            r.Post("/transaction", txCtrl.ValidateTransaction)
        })

        r.Route("/api/v1/rewards", func(r chi.Router) {
            r.Get("/status", rewardCtrl.Status)
        })
    })

    return r
}
