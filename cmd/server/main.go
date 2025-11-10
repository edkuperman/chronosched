package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/edkuperman/chronosched/internal/api"
	"github.com/edkuperman/chronosched/internal/db"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool := db.MustPool(ctx)

	h := api.NewHandlers(pool)
	r := api.NewRouter(h)

	log.Println("chronosched API listening on :8080")
	if err := http.ListenAndServe(":8080", r); err != nil {
		log.Fatal(err)
	}
}
