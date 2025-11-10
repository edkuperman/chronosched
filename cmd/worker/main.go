package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/edkuperman/chronosched/internal/worker"
)

func main() {
	// Create a context that cancels on Ctrl+C or SIGTERM
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	r := &worker.Runner{
		DB:             pool,
		PollEvery:      800 * time.Millisecond,
		MaxBatch:       8,                // number of DAGs/jobs to dequeue per poll
		Concurrency:    4,                // number of parallel goroutines per worker
		LeaseDuration:  10 * time.Minute, // job lease
		HeartbeatEvery: 30 * time.Second, // extend lease
	}

	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("worker stopped with error: %v", err)
	}
}
