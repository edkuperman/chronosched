package main

import (
    "context"
    "log"
    "net/http"

    "github.com/edkuperman/chronosched/internal/api"
    "github.com/edkuperman/chronosched/internal/db"
    "github.com/edkuperman/chronosched/internal/scheduler"
)

func main() {
    ctx := context.Background()

    pool := db.MustPool(ctx)
    h := api.NewHandlers(pool)
    r := api.NewRouter(h)


    sched := scheduler.New(h.JobRepo(), pool)
    if err := sched.LoadAndRegister(ctx); err != nil {
        log.Fatalf("scheduler startup failed: %v", err)
    }
    go sched.Start()

    log.Println("chronosched API listening on :8080")
    if err := http.ListenAndServe(":8080", r); err != nil {
        log.Fatal(err)
    }
}
