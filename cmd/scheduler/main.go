package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/edkuperman/chronosched/internal/dal/sql"
	"github.com/edkuperman/chronosched/internal/logger"
	"github.com/edkuperman/chronosched/internal/repository"
	"github.com/edkuperman/chronosched/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

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
		Definitions: sql.NewJobDefinitionSQL(dal),
		DAGs:        sql.NewDAGSQL(dal),
		Runs:        sql.NewRunSQL(dal),
		Jobs:        sql.NewJobSQL(dal),
		Queue:       sql.NewQueueSQL(dal),
		Admin:       sql.NewAdminSQL(dal),
	}

	sched := scheduler.NewScheduler(repos)
	logger.Info("chronosched scheduler listening for work")
	if err := sched.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error(err, "scheduler stopped")
		os.Exit(1)
	}
}
