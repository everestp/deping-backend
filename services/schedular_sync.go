package services

import (
	"context"
	"time"

	"github.com/everestp/depin-backend/db/repositories"
	"github.com/redis/go-redis/v9"
)

// SyncSchedulerState is now a standalone function
func SyncSchedulerState(ctx context.Context, rdb *redis.Client, store *repositories.Storage) error {
	monitors, err := store.Monitors.FindActive(ctx)
	if err != nil {
		return err
	}

	pipe := rdb.Pipeline()
	for _, m := range monitors {
		pipe.ZAddNX(ctx, "scheduler:due", redis.Z{
			Score:  float64(time.Now().Unix()),
			Member: "sched:monitor:" + m.ID,
		})
	}
	_, err = pipe.Exec(ctx)
	return err
}
