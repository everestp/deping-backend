package workers

import (
	"context"
	"log"
	"time"

	"github.com/everestp/depin-backend/db/repositories"
	"github.com/everestp/depin-backend/services" // Import services
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func StartSyncCron(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client) {
	store := repositories.NewStorage(pool)
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		for {
			select {
			case <-ctx.Done(): return
			case <-ticker.C:
				if err := services.SyncSchedulerState(ctx, rdb, store); err != nil {
					log.Printf("[sync-cron] error: %v", err)
				}
			}
		}
	}()
}
