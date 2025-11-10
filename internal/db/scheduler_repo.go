package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SchedulerRepo struct{ DB *pgxpool.Pool }

func NewSchedulerRepo(db *pgxpool.Pool) *SchedulerRepo { return &SchedulerRepo{DB: db} }

// FairDequeue returns up to n job IDs fairly across DAGs and leases them.
func (r *SchedulerRepo) FairDequeue(ctx context.Context, n int, worker string, lease time.Duration) ([]int64, error) {
	tx, err := r.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
	WITH ranked AS (
	  SELECT j.id, j.dag_id, j.priority, j.due_at,
	         ROW_NUMBER() OVER (PARTITION BY j.dag_id ORDER BY j.priority DESC, j.due_at ASC, j.id) AS rnk
	  FROM jobs j
	  JOIN job_frontier f ON f.job_id = j.id AND f.ready = TRUE
	  WHERE j.status = 'queued' AND j.due_at <= now()
	)
	SELECT id FROM ranked WHERE rnk = 1
	ORDER BY priority DESC, due_at ASC
	LIMIT $1
	FOR UPDATE SKIP LOCKED;`, n)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) == 0 {
		_ = tx.Commit(ctx)
		return ids, nil
	}

	leaseUntil := time.Now().Add(lease)
	_, err = tx.Exec(ctx, `
	UPDATE jobs SET status='running', lease_owner=$1, lease_until=$2, started_at=COALESCE(started_at, now())
	WHERE id = ANY($3);`, worker, leaseUntil, ids)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return ids, nil
}
