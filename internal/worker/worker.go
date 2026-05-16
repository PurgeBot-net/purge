package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/PurgeBot-net/common/job"
	"github.com/PurgeBot-net/database"
	"github.com/PurgeBot-net/purger/config"
	"github.com/PurgeBot-net/purger/internal/engine"
)

type Worker struct {
	cfg    config.Config
	logger *zap.Logger
	db     *database.Database
	redis  *redis.Client
	client *bot.Client
}

func New(cfg config.Config, logger *zap.Logger, db *database.Database, redis *redis.Client) (*Worker, error) {
	client, err := disgo.New(cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("create discord client: %w", err)
	}
	return &Worker{cfg: cfg, logger: logger, db: db, redis: redis, client: client}, nil
}

// Run blocks, consuming purge jobs from Redis until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("purge worker started")
	w.recoverActiveJobs(ctx)
	eng := engine.New(w.cfg, w.logger, w.db, w.redis, w.client)
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("purge worker stopped")
			return
		default:
		}

		j, err := job.Dequeue(ctx, w.redis, 5*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("dequeue job", zap.Error(err))
			continue
		}
		if j == nil {
			continue // timeout, loop again
		}

		// Guard against double-enqueue: skip if this is a stale copy of a recovered job.
		active, err := job.GetActiveJob(ctx, w.redis, j.GuildID)
		if err != nil {
			w.logger.Error("verify active job", zap.String("id", j.ID), zap.Error(err))
			continue
		}
		if active == nil || active.ID != j.ID {
			w.logger.Info("skipping stale recovered job", zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID))
			continue
		}

		w.logger.Info("processing purge job",
			zap.String("id", j.ID),
			zap.Uint64("guild_id", j.GuildID),
			zap.String("type", string(j.PurgeType)),
		)

		if err := eng.Execute(ctx, j); err != nil {
			w.logger.Error("purge job failed", zap.String("id", j.ID), zap.Error(err))
		}

		job.DeleteActiveJob(ctx, w.redis, j.GuildID)
	}
}

// recoverActiveJobs re-queues any jobs that were active when the worker last crashed.
func (w *Worker) recoverActiveJobs(ctx context.Context) {
	jobs, err := job.GetAllActiveJobs(ctx, w.redis)
	if err != nil {
		w.logger.Error("scan active jobs for recovery", zap.Error(err))
		return
	}
	for _, j := range jobs {
		if err := job.Enqueue(ctx, w.redis, j); err != nil {
			w.logger.Error("re-enqueue recovered job", zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID), zap.Error(err))
		} else {
			w.logger.Info("recovered active job", zap.String("id", j.ID), zap.Uint64("guild_id", j.GuildID))
		}
	}
}
