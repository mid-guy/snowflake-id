// Package workerid assigns Snowflake worker IDs via a Redis lease.
//
// Each worker holds a TTL-bound key worker:{id} = ownerToken. A background
// goroutine refreshes the TTL; if the process dies, the key expires and the
// ID is reusable. This avoids the classic INCR%N collision after N restarts.
package workerid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultKeyPrefix    = "snowflake:worker:"
	defaultLeaseTTL     = 30 * time.Second
	defaultRefreshEvery = 10 * time.Second
)

var ErrNoWorkerIDAvailable = errors.New("workerid: no worker ID slots available")

type Config struct {
	MaxWorkerID  int64         // inclusive upper bound, e.g. 1023
	KeyPrefix    string        // defaults to "snowflake:worker:"
	LeaseTTL     time.Duration // defaults to 30s
	RefreshEvery time.Duration // defaults to 10s
}

func (c *Config) applyDefaults() {
	if c.KeyPrefix == "" {
		c.KeyPrefix = defaultKeyPrefix
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = defaultLeaseTTL
	}
	if c.RefreshEvery <= 0 {
		c.RefreshEvery = defaultRefreshEvery
	}
}

type Lease struct {
	WorkerID int64
	token    string
	key      string
	cfg      Config
	rdb      *redis.Client
	cancel   context.CancelFunc
	done     chan struct{}
}

// Acquire scans IDs 0..MaxWorkerID and grabs the first free slot via SET NX.
// The returned Lease runs a background refresher; call Release() at shutdown.
func Acquire(ctx context.Context, rdb *redis.Client, cfg Config) (*Lease, error) {
	cfg.applyDefaults()

	token, err := randomToken()
	if err != nil {
		return nil, err
	}

	for id := int64(0); id <= cfg.MaxWorkerID; id++ {
		key := fmt.Sprintf("%s%d", cfg.KeyPrefix, id)
		ok, err := rdb.SetNX(ctx, key, token, cfg.LeaseTTL).Result()
		if err != nil {
			return nil, fmt.Errorf("workerid: redis SETNX failed: %w", err)
		}
		if ok {
			leaseCtx, cancel := context.WithCancel(context.Background())
			l := &Lease{
				WorkerID: id,
				token:    token,
				key:      key,
				cfg:      cfg,
				rdb:      rdb,
				cancel:   cancel,
				done:     make(chan struct{}),
			}
			go l.refreshLoop(leaseCtx)
			return l, nil
		}
	}
	return nil, ErrNoWorkerIDAvailable
}

func (l *Lease) refreshLoop(ctx context.Context) {
	defer close(l.done)
	t := time.NewTicker(l.cfg.RefreshEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			// Only extend if we still own the key. Use a CAS via Lua.
			_, _ = refreshScript.Run(ctx, l.rdb, []string{l.key}, l.token, int(l.cfg.LeaseTTL/time.Millisecond)).Result()
		}
	}
}

// Release stops refreshing and deletes the lease key if we still own it.
func (l *Lease) Release(ctx context.Context) error {
	l.cancel()
	<-l.done
	_, err := releaseScript.Run(ctx, l.rdb, []string{l.key}, l.token).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("workerid: release failed: %w", err)
	}
	return nil
}

var refreshScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
  return 0
end
`)

var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
else
  return 0
end
`)

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("workerid: rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
