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
	Rdb        *redis.Client
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

	// ── RabbitMQ ───────────────────────────────────────────────────────────
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

	// ── Initialize App Instance ───────────────────────────────────────────
	application := &Application{
		cfg: cfg,
		Rdb: rdb,
	}

	// ── Bootstrap Scheduler ──────────────────────────────────────────────
	store := repositories.NewStorage(pool)
if err := services.SyncSchedulerState(ctx, rdb, store); err != nil {
    return nil, fmt.Errorf("bootstrap scheduler: %w", err)
}

	// ── Services ───────────────────────────────────────────────────────────
	validator := anticheat.NewValidator(rdb)
	memRegistry := services.NewMemoryRegistry()
	smartScheduler := services.NewSmartScheduler(memRegistry)

	userSvc := services.NewUserService(store, cfg)
	monitorSvc := services.NewMonitorService(store, rdb, rabbitCh, cfg)
	runnerSvc := services.NewRunnerService(store, rdb, rabbitCh, cfg, memRegistry)
	rewardSvc := services.NewRewardService(store, rabbitCh, cfg)
	pingLogSvc := services.NewPingLogService(store, pool)

	// ── Controllers ────────────────────────────────────────────────────────
	r := router.New(cfg,
		controllers.NewUserController(userSvc),
		controllers.NewMonitorController(monitorSvc),
		controllers.NewRunnerController(runnerSvc),
		controllers.NewRewardController(rewardSvc),
		controllers.NewPingController(pingLogSvc, rabbitCh, validator),
	)

	application.httpServer = &http.Server{
		Addr: ":" + cfg.Port, Handler: r,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second,
	}
	application.grpcServer = grpcserver.NewServer(runnerSvc, monitorSvc, validator, rabbitConn)

	// ── Workers ────────────────────────────────────────────────────────────
	workers.StartScheduler(ctx, rdb, pool, rabbitCh, memRegistry, smartScheduler)
	workers.StartResultProcessor(ctx, pool, rabbitConn, pingLogSvc, rewardSvc)
	workers.StartSolanaSync(ctx, pool, rabbitCh, cfg)
	workers.StartPartitionCron(ctx, pool)
	workers.StartSyncCron(ctx, pool, rdb) // Start the periodic sync

	return application, nil
}

func (a *Application) BootstrapScheduler(ctx context.Context, store *repositories.Storage) error {
	monitors, err := store.Monitors.FindActive(ctx)
	if err != nil {
		return err
	}

	pipe := a.Rdb.Pipeline()
	for _, m := range monitors {
		pipe.ZAddNX(ctx, "scheduler:due", redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: "sched:monitor:" + m.ID,
		})
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (a *Application) Run() error {
	errCh := make(chan error, 2)
	// go func() { if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed { errCh <- err } }()
	go func() {
		lis, err := net.Listen("tcp", ":"+a.cfg.GRPCPort)
		if err == nil { errCh <- a.grpcServer.Serve(lis) } else { errCh <- err }
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case err := <-errCh: return err
	case <-quit:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a.grpcServer.GracefulStop()
		return a.httpServer.Shutdown(ctx)
	}
}

func declareQueues(ch *amqp.Channel) error {
    // 1. Declare the queues (as you do now)
    queues := []string{"job_queue", "processing_queue", "solana_sync_queue", "telegram_queue"}
    for _, q := range queues {
        if _, err := ch.QueueDeclare(q, true, false, false, false, nil); err != nil {
            return err
        }
    }

    // 2. Declare the Exchange
    exchange := "monitor_updates"
    if err := ch.ExchangeDeclare(exchange, "fanout", true, false, false, false, nil); err != nil {
        return err
    }

    // 3. Bind the queues that need notifications to this exchange
    // If you add a new service later, just add its queue name here!
    targetQueues := []string{"processing_queue", "telegram_queue"}
    for _, q := range targetQueues {
        if err := ch.QueueBind(q, "", exchange, false, nil); err != nil {
            return err
        }
    }

    return nil
}
