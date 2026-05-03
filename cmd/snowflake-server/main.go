package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/mid-guy/snowflake-id/snowflake"
	"github.com/mid-guy/snowflake-id/workerid"
)

func main() {
	var (
		addr       = flag.String("addr", envOr("ADDR", ":8080"), "HTTP listen address")
		redisAddr  = flag.String("redis", envOr("REDIS_ADDR", "localhost:6379"), "Redis address")
		redisPass  = flag.String("redis-pass", os.Getenv("REDIS_PASSWORD"), "Redis password")
		staticID   = flag.Int64("worker-id", -1, "Use a static worker ID instead of Redis lease (-1 = lease)")
		leaseTTL   = flag.Duration("lease-ttl", 30*time.Second, "Worker ID lease TTL")
		refreshDur = flag.Duration("lease-refresh", 10*time.Second, "Worker ID refresh interval")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var (
		workerID int64
		release  func(context.Context) error = func(context.Context) error { return nil }
	)

	if *staticID >= 0 {
		workerID = *staticID
		log.Printf("using static worker ID = %d", workerID)
	} else {
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, Password: *redisPass})
		defer rdb.Close()

		if err := rdb.Ping(ctx).Err(); err != nil {
			log.Fatalf("redis ping failed: %v", err)
		}

		lease, err := workerid.Acquire(ctx, rdb, workerid.Config{
			MaxWorkerID:  snowflake.MaxWorkerID,
			LeaseTTL:     *leaseTTL,
			RefreshEvery: *refreshDur,
		})
		if err != nil {
			log.Fatalf("acquire worker ID: %v", err)
		}
		workerID = lease.WorkerID
		release = lease.Release
		log.Printf("acquired worker ID = %d via redis lease", workerID)
	}

	sf, err := snowflake.New(workerID)
	if err != nil {
		log.Fatalf("snowflake init: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/id", func(w http.ResponseWriter, r *http.Request) {
		id, err := sf.NextID()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"id":        id,
			"id_string": strconv.FormatInt(id, 10),
			"worker_id": workerID,
		})
	})
	mux.HandleFunc("/decode", func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Query().Get("id")
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			http.Error(w, "bad id query param", http.StatusBadRequest)
			return
		}
		writeJSON(w, http.StatusOK, sf.Decode(id))
	})

	srv := &http.Server{
		Addr:              *addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", *addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	if err := release(shutdownCtx); err != nil {
		log.Printf("release worker id: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
