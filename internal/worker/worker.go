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

		w.logger.Info("processing purge job",
			zap.String("id", j.ID),
			zap.Uint64("guild_id", j.GuildID),
			zap.String("type", string(j.PurgeType)),
		)

		eng := engine.New(w.cfg, w.logger, w.db, w.redis, w.client)
		if err := eng.Execute(ctx, j); err != nil {
			w.logger.Error("purge job failed", zap.String("id", j.ID), zap.Error(err))
		}

		job.UnlockGuild(ctx, w.redis, j.GuildID, j.ID) //nolint:errcheck
	}
}
