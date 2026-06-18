package app

import (
	"context"
	"fmt"
	"log"
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
	"github.com/everestp/depin-backend/solana"
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

	// rdb := redis.NewClient(&redis.Options{
	// 	Addr:     cfg.RedisAddr,
	// 	Password: cfg.RedisPassword,
	// 	DB:       cfg.RedisDB,
	// })
	// if err := rdb.Ping(ctx).Err(); err != nil {
	// 	return nil, fmt.Errorf("redis ping: %w", err)
	// }
opt, err := redis.ParseURL(cfg.UptashRedisAddr)
if err != nil {
	return nil, fmt.Errorf("parse redis url: %w", err)
}

rdb := redis.NewClient(opt)

if err := rdb.Ping(ctx).Err(); err != nil {
	return nil, fmt.Errorf("redis ping: %w", err)
}


	
	// ── RabbitMQ ───────────────────────────────────────────────────────────
	rabbitConn, err := amqp.Dial(cfg.CloudRabbitMQURL)
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
	solanaClient := solana.NewClient(cfg.SolanaRPCURL)

	// ── Controllers ────────────────────────────────────────────────────────
	r := router.New(cfg,
		controllers.NewUserController(userSvc),
		controllers.NewMonitorController(monitorSvc),
		controllers.NewRunnerController(runnerSvc),
		controllers.NewRewardController(rewardSvc),
		controllers.NewPingController(pingLogSvc, rabbitCh, validator),
		controllers.NewTransactionController(solanaClient),
	)

	application.httpServer = &http.Server{
		Addr: ":" + cfg.Port, Handler: r,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second,
	}
	application.grpcServer = grpcserver.NewServer(runnerSvc, monitorSvc, validator, rabbitConn)

	// ── Workers ────────────────────────────────────────────────────────────
	workers.StartScheduler(ctx, rdb, pool, rabbitConn, memRegistry, smartScheduler)
	workers.StartResultProcessor(ctx, pool, rabbitConn, pingLogSvc, rewardSvc)
	workers.StartSolanaSync(ctx, pool, cfg)
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

    // 1. Start HTTP Server
    // go func() {
    //     if err := a.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
    //         errCh <- fmt.Errorf("http server: %w", err)
    //     }
    // }()

    // 2. Start gRPC Server
    go func() {
        lis, err := net.Listen("tcp", ":"+a.cfg.GRPCPort)
        if err != nil {
            errCh <- fmt.Errorf("grpc listener: %w", err)
            return
        }
        if err := a.grpcServer.Serve(lis); err != nil {
            errCh <- fmt.Errorf("grpc server: %w", err)
        }
    }()

    // 3. Wait for signal or error
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

    select {
    case err := <-errCh:
        return err
    case <-quit:
        log.Println("Shutting down servers...")

        // Create a context for graceful shutdown
        ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
        defer cancel()

        // Stop gRPC immediately
        a.grpcServer.GracefulStop()

        // Shutdown HTTP gracefully
        if err := a.httpServer.Shutdown(ctx); err != nil {
            return fmt.Errorf("http shutdown: %w", err)
        }

        return nil
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
