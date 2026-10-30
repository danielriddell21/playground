package bigdata

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// redis is opt in: with --redis the scenarios it can speed up race a fourth
// approach where redis holds the hot state and postgres only takes COPY
var redisAddr string

const keyPrefix = "bigdata:"

func openRedis(ctx context.Context) (*redis.Client, error) {
	addr := redisAddr
	if addr == "" {
		addr = os.Getenv("REDIS_URL")
	}
	if addr == "" {
		return nil, nil
	}
	opts := &redis.Options{Addr: addr}
	if strings.Contains(addr, "://") {
		var err error
		if opts, err = redis.ParseURL(addr); err != nil {
			return nil, err
		}
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, err
	}
	return rdb, nil
}

func (s *Session) withRedis() bool {
	if s.rdb == nil {
		s.Note("\n(redis approach skipped: pass --redis localhost:6379)")
	}
	return s.rdb != nil
}

// resetRedis drops every key this playground owns
func (s *Session) resetRedis() error {
	if s.rdb == nil {
		return nil
	}
	iter := s.rdb.Scan(s.ctx, 0, keyPrefix+"*", 1000).Iterator()
	var keys []string
	for iter.Next(s.ctx) {
		keys = append(keys, iter.Val())
		if len(keys) == 1000 {
			if err := s.rdb.Unlink(s.ctx, keys...).Err(); err != nil {
				return err
			}
			keys = keys[:0]
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	if len(keys) > 0 {
		return s.rdb.Unlink(s.ctx, keys...).Err()
	}
	return nil
}

// lane is a COPY fed by a channel, so a handler can answer from redis and
// leave the raw rows to land in postgres in the background
type lane[B any] struct {
	ch   chan B
	done chan error
}

func copyLane[B any](ctx context.Context, pool *pgxpool.Pool, table string, cols []string, size func(B) int, row func(B, int, []any)) *lane[B] {
	l := &lane[B]{ch: make(chan B, 16), done: make(chan error, 1)}
	go func() {
		recv := func() (B, error) {
			var zero B
			select {
			case b, ok := <-l.ch:
				if !ok {
					return zero, io.EOF
				}
				return b, nil
			case <-ctx.Done():
				return zero, ctx.Err()
			}
		}
		_, err := pool.CopyFrom(ctx, pgx.Identifier{table}, cols, pgx.CopyFromFunc(fromStream(len(cols), recv, size, row)))
		// a failed copy stops reading; keep draining so senders never block
		for range l.ch {
		}
		l.done <- err
	}()
	return l
}

func (l *lane[B]) send(ctx context.Context, b B) error {
	select {
	case l.ch <- b:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *lane[B]) close() error {
	close(l.ch)
	return <-l.done
}
