package main

import (
	"context"
	"net/http"
	"os"

	"github.com/edkuperman/chronosched/internal/logger"

	"github.com/edkuperman/chronosched/internal/api"
	"github.com/edkuperman/chronosched/internal/dal/sql"
	"github.com/edkuperman/chronosched/internal/repository"
	"github.com/edkuperman/chronosched/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Error(err, "failed to connect to db")
		os.Exit(1)
	}
	defer pool.Close()

	dal := sql.NewSQLDAL(pool)

	repos := &repository.Repos{
		Namespaces:  sql.NewNamespaceSQL(dal),
		DAGs:        sql.NewDAGSQL(dal),
		Definitions: sql.NewJobDefinitionSQL(dal),
		Jobs:        sql.NewJobSQL(dal),
		Queue:       sql.NewQueueSQL(dal),
		Deps:        sql.NewDependencySQL(dal),
		Admin:       sql.NewAdminSQL(dal),
	}

	// Start background scheduler to promote due 'waiting' jobs into the queue.
	sched := scheduler.NewScheduler(repos)
	go func() {
		if err := sched.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error(err, "scheduler stopped with error")
		}
	}()

	h := api.NewHandler(repos)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: api.NewHTTPHandler(h),
	}

	logger.Info("chronosched refactored API listening on :8080")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error(err, "server error")
		os.Exit(1)
	}
}
