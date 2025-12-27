package sql

import (
    "context"
    "time"

    "github.com/edkuperman/chronosched/internal/repository"
)

type JobSQL struct {
    dal *SQLDAL
}

func NewJobSQL(dal *SQLDAL) *JobSQL {
    return &JobSQL{dal: dal}
}

func (s *JobSQL) Create(ctx context.Context, dagID, defID string, payload []byte, priority int) (*repository.Job, error) {
    const q = `
INSERT INTO jobs (dag_id, def_id, priority, due_at, payload_json)
VALUES ($1, $2, $3, now(), $4::jsonb)
RETURNING id, dag_id, def_id, status, priority, due_at;
`
    var j repository.Job
    err := s.dal.DB.QueryRow(ctx, q, dagID, defID, priority, string(payload)).Scan(
        &j.ID, &j.DagID, &j.DefID, &j.Status, &j.Priority, &j.DueAt,
    )
    return &j, err
}

func (s *JobSQL) Get(ctx context.Context, id int64) (*repository.Job, error) {
    const q = `
SELECT id, dag_id, def_id, status, priority, due_at, payload_json::text
FROM jobs
WHERE id = $1;
`
    var j repository.Job
    var payloadStr string
    err := s.dal.DB.QueryRow(ctx, q, id).Scan(
        &j.ID, &j.DagID, &j.DefID, &j.Status, &j.Priority, &j.DueAt, &payloadStr,
    )
    if err != nil {
        return nil, err
    }
    j.Payload = []byte(payloadStr)
    return &j, nil
}

func (s *JobSQL) ListByDAG(ctx context.Context, dagID string) ([]repository.JobListItem, error) {
    const q = `
SELECT id, def_id, dag_id, status::text
FROM jobs
WHERE dag_id = $1 AND deleted = FALSE
ORDER BY id;
`
    rows, err := s.dal.DB.Query(ctx, q, dagID)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []repository.JobListItem
    for rows.Next() {
        var item repository.JobListItem
        if err := rows.Scan(&item.ID, &item.DefID, &item.DagID, &item.Status); err != nil {
            return nil, err
        }
        res = append(res, item)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return res, nil
}

func (s *JobSQL) MarkQueued(ctx context.Context, id int64) error {
    const q = `
UPDATE jobs 
SET status = 'queued', enqueued_at = now() 
WHERE id = $1;
`
    _, err := s.dal.DB.Exec(ctx, q, id)
    return err
}

func (s *JobSQL) MarkRunning(ctx context.Context, id int64) error {
    const q = `
UPDATE jobs 
SET status = 'running', started_at = COALESCE(started_at, now()) 
WHERE id = $1;
`
    _, err := s.dal.DB.Exec(ctx, q, id)
    return err
}

func (s *JobSQL) MarkSucceeded(ctx context.Context, id int64) error {
    const q = `
UPDATE jobs 
SET status = 'succeeded', finished_at = now() 
WHERE id = $1;
`
    _, err := s.dal.DB.Exec(ctx, q, id)
    return err
}

func (s *JobSQL) MarkFailed(ctx context.Context, id int64, reason string) error {
    const q = `
UPDATE jobs 
SET status = 'failed', last_error = $2, finished_at = now() 
WHERE id = $1;
`
    _, err := s.dal.DB.Exec(ctx, q, id, reason)
    return err
}

func (s *JobSQL) Delete(ctx context.Context, id int64) error {
    const q = `
UPDATE jobs
SET deleted = TRUE
WHERE id = $1;
`
    _, err := s.dal.DB.Exec(ctx, q, id)
    return err
}

// FindDueWaiting is used by the scheduler to locate jobs that are
// ready to be promoted from 'waiting' to 'queued'.
func (s *JobSQL) FindDueWaiting(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
    const q = `
SELECT id, dag_id, def_id, status, priority, due_at, payload_json::text
FROM jobs
WHERE status = 'waiting'
  AND deleted = FALSE
  AND due_at <= $1
ORDER BY due_at ASC, priority DESC
LIMIT $2;
`
    rows, err := s.dal.DB.Query(ctx, q, before, limit)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var res []*repository.Job
    for rows.Next() {
        var j repository.Job
        var payloadStr string
        if err := rows.Scan(&j.ID, &j.DagID, &j.DefID, &j.Status, &j.Priority, &j.DueAt, &payloadStr); err != nil {
            return nil, err
        }
        j.Payload = []byte(payloadStr)
        res = append(res, &j)
    }
    if err := rows.Err(); err != nil {
        return nil, err
    }
    return res, nil
}
