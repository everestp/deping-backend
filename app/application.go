package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	"github.com/everestp/depin-backend/anticheat"
	configdb "github.com/everestp/depin-backend/config/db"
	"github.com/everestp/depin-backend/config/env"
	"github.com/everestp/depin-backend/controllers"
	"github.com/everestp/depin-backend/db/repositories"
	grpcserver "github.com/everestp/depin-backend/grpc"
	"github.com/everestp/depin-backend/router"
	"github.com/everestp/depin-backend/services"
	"github.com/everestp/depin-backend/workers"
)

type Application struct {
	cfg        *env.Config
	httpServer *http.Server
	grpcServer *grpc.Server
}

func New(cfg *env.Config) (*Application, error) {
	ctx := context.Background()

	// ── Infra ──────────────────────────────────────────────────────────────
	pool, err := configdb.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("db pool: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	// ── RabbitMQ Parent Connection Setup ───────────────────────────────────
	rabbitConn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, fmt.Errorf("rabbitmq dial: %w", err)
	}

	rabbitCh, err := rabbitConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := declareQueues(rabbitCh); err != nil {
		return nil, fmt.Errorf("declare queues: %w", err)
	}

	// ── Anti-cheat validator (shared by gRPC server + REST controller) ─────
	validator := anticheat.NewValidator(rdb)

	// ── Repositories ───────────────────────────────────────────────────────
	store := repositories.NewStorage(pool)

	// ── In-Memory DePIN Registry & Intelligent Matcher Engine ──────────────
	// ADDED: Thread-safe cache instances map live node connections without hitting Postgres
	memRegistry := services.NewMemoryRegistry()
	smartScheduler := services.NewSmartScheduler(memRegistry)

	// ── Services ───────────────────────────────────────────────────────────
	userSvc := services.NewUserService(store, cfg)
	monitorSvc := services.NewMonitorService(store, rdb, rabbitCh, cfg)

	// CRITICAL CHANGE: Pass memRegistry into runnerSvc so when gRPC streams receive
	// heartbeats, they update our live localized coordinate pool inside memRegistry.
	runnerSvc := services.NewRunnerService(store, rdb, rabbitCh, cfg, memRegistry)

	rewardSvc := services.NewRewardService(store, rabbitCh, cfg)
	pingLogSvc := services.NewPingLogService(store, pool)

	// ── Controllers ────────────────────────────────────────────────────────
	userCtrl := controllers.NewUserController(userSvc)
	monitorCtrl := controllers.NewMonitorController(monitorSvc)
	runnerCtrl := controllers.NewRunnerController(runnerSvc)
	rewardCtrl := controllers.NewRewardController(rewardSvc)
	pingCtrl := controllers.NewPingController(pingLogSvc, rabbitCh, validator)

	// ── HTTP ───────────────────────────────────────────────────────────────
	r := router.New(cfg, userCtrl, monitorCtrl, runnerCtrl, rewardCtrl, pingCtrl)
	httpSrv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── gRPC — Wired with parent rabbitConn instead of static channel ──────
	grpcSrv := grpcserver.NewServer(runnerSvc, monitorSvc, validator, rabbitConn)

	// ── Background workers ─────────────────────────────────────────────────
	// CRITICAL CHANGE: We inject our new shared memory registry tracking instances
	// directly into the background task distributor.
	workers.StartScheduler(ctx, rdb, pool, rabbitCh, memRegistry, smartScheduler)
	workers.StartResultProcessor(ctx, pool, rabbitConn, pingLogSvc, rewardSvc)
	workers.StartSolanaSync(ctx, pool, rabbitCh, cfg)
	workers.StartPartitionCron(ctx, pool)

	return &Application{
		cfg:        cfg,
		httpServer: httpSrv,
		grpcServer: grpcSrv,
	}, nil
}

func (a *Application) Run() error {
	errCh := make(chan error, 2)

	go func() {
		fmt.Printf("HTTP  → :%s\n", a.cfg.Port)
		if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("http: %w", err)
		}
	}()

	go func() {
		lis, err := net.Listen("tcp", ":"+a.cfg.GRPCPort)
		if err != nil {
			errCh <- fmt.Errorf("grpc listen: %w", err)
			return
		}
		fmt.Printf("gRPC  → :%s\n", a.cfg.GRPCPort)
		if err := a.grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc: %w", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		return err
	case <-quit:
		fmt.Println("shutting down gracefully…")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a.grpcServer.GracefulStop()
		return a.httpServer.Shutdown(ctx)
	}
}

func declareQueues(ch *amqp.Channel) error {
	for _, q := range []string{"job_queue", "processing_queue", "solana_sync_queue","telegram_queue"} {
		if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %s: %w", q, err)
		}
	}
	return nil
}
