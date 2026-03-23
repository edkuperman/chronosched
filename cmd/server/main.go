package main

import (
	"context"
	"net/http"
	"os"

	"github.com/edkuperman/chronosched/internal/api"
	"github.com/edkuperman/chronosched/internal/dal/sql"
	"github.com/edkuperman/chronosched/internal/events"
	"github.com/edkuperman/chronosched/internal/logger"
	"github.com/edkuperman/chronosched/internal/repository"
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
	events.SetDefaultPublisher(events.NewLoggerPublisher())
	repos := &repository.Repos{
		Namespaces:  sql.NewNamespaceSQL(dal),
		Definitions: sql.NewJobDefinitionSQL(dal),
		DAGs:        sql.NewDAGSQL(dal),
		Runs:        sql.NewRunSQL(dal),
		Jobs:        sql.NewJobSQL(dal),
		Queue:       sql.NewQueueSQL(dal),
		Admin:       sql.NewAdminSQL(dal),
	}

	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	addr := ":" + port

	srv := &http.Server{
		Addr:    addr,
		Handler: api.NewHTTPHandler(api.NewHandler(repos)),
	}

	logger.Info("chronosched listening", "addr", addr)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error(err, "server error")
		os.Exit(1)
	}
}
