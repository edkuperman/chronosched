package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/edkuperman/chronosched/internal/logger"

	"github.com/edkuperman/chronosched/internal/worker"
)

func main() {
	baseURL := os.Getenv("CHRONOSCHED_BASE_URL")
	if baseURL == "" {
		baseURL = "http://localhost:8080"
	}
	workerID := os.Getenv("CHRONOSCHED_WORKER_ID")
	if workerID == "" {
		workerID = "worker-1"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	r := worker.NewRunner(baseURL, workerID)
	if err := r.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error(err, "worker stopped with error")
		os.Exit(1)
	}
}
