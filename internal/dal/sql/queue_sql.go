package sql

import (
    "context"
    "time"

    "github.com/edkuperman/chronosched/internal/repository"
)

type QueueSQL struct {
    dal *SQLDAL
}

func NewQueueSQL(dal *SQLDAL) *QueueSQL {
    return &QueueSQL{dal: dal}
}

func (q *QueueSQL) Enqueue(ctx context.Context, jobID int64, runAt time.Time, priority int) error {
    const stmt = `
INSERT INTO job_queue (job_id, available_at, priority)
VALUES ($1, $2, $3);
`
    _, err := q.dal.DB.Exec(ctx, stmt, jobID, runAt, priority)
    return err
}

func (q *QueueSQL) Dequeue(ctx context.Context, workerID string, n int, vt time.Duration) ([]repository.QueueItem, error) {
    const stmt = `
WITH cte AS (
    SELECT id
    FROM job_queue
    WHERE (reserved_until IS NULL OR reserved_until < now())
      AND available_at <= now()
    ORDER BY priority DESC, available_at
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE job_queue j
SET reserved_until = now() + $3::interval,
    consumer_id = $2,
    updated_at = now()
FROM cte
WHERE j.id = cte.id
RETURNING j.id, j.job_id, j.attempts, j.priority;
`
    rows, err := q.dal.DB.Query(ctx, stmt, n, workerID, vt.String())
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []repository.QueueItem
    for rows.Next() {
        var it repository.QueueItem
        if err := rows.Scan(&it.QueueID, &it.JobID, &it.Attempts, &it.Priority); err != nil {
            return nil, err
        }
        res = append(res, it)
    }
    return res, rows.Err()
}

func (q *QueueSQL) Ack(ctx context.Context, queueID int64, workerID string) error {
    const stmt = `
DELETE FROM job_queue
WHERE id = $1 AND consumer_id = $2;
`
    _, err := q.dal.DB.Exec(ctx, stmt, queueID, workerID)
    return err
}

func (q *QueueSQL) Fail(ctx context.Context, queueID int64, workerID string, delay time.Duration) error {
    const stmt = `
UPDATE job_queue
SET attempts = attempts + 1,
    reserved_until = NULL,
    consumer_id = NULL,
    available_at = now() + $3::interval
WHERE id = $1 AND consumer_id = $2;
`
    _, err := q.dal.DB.Exec(ctx, stmt, queueID, workerID, delay.String())
    return err
}
