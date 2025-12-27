package sql

import (
    "context"
    "fmt"

    "github.com/edkuperman/chronosched/internal/repository"
	"github.com/edkuperman/chronosched/internal/dag"
)


type AdminSQL struct {
    dal *SQLDAL
}

func NewAdminSQL(dal *SQLDAL) *AdminSQL {
    return &AdminSQL{dal: dal}
}

func (a *AdminSQL) CheckGlobalCycles(ctx context.Context) (*repository.CycleCheckResponse, error) {
	const q = `
SELECT dag_id, parent_job_id, child_job_id
FROM job_dependencies
WHERE deleted = FALSE;
`
	rows, err := a.dal.DB.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type edge struct {
		parent int64
		child  int64
	}

	dagEdges := map[string][]edge{}

	for rows.Next() {
		var dagID string
		var parentID, childID int64
		if err := rows.Scan(&dagID, &parentID, &childID); err != nil {
			return nil, err
		}
		dagEdges[dagID] = append(dagEdges[dagID], edge{parent: parentID, child: childID})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	resp := &repository.CycleCheckResponse{}
	for dagID, edges := range dagEdges {
		var graphEdges []dag.Edge
		for _, e := range edges {
			graphEdges = append(graphEdges, dag.Edge{
				From: fmt.Sprintf("%d", e.parent),
				To:   fmt.Sprintf("%d", e.child),
			})
		}

		cycles := dag.DetectCycles(graphEdges)
		if len(cycles) > 0 {
			resp.Results = append(resp.Results, repository.CycleCheckResult{
				DAGID:  dagID,
				Cycles: cycles,
			})
			resp.Count += len(cycles)
		}
	}

	return resp, nil
}
func (a *AdminSQL) Prune(ctx context.Context) (*repository.PruneSummary, error) {
    summary := &repository.PruneSummary{}

    // job_definitions -> job_definitions_history
    if err := a.pruneJobDefinitions(ctx, summary); err != nil {
        return nil, err
    }
    // dags -> dags_history
    if err := a.pruneDAGs(ctx, summary); err != nil {
        return nil, err
    }
    // jobs -> jobs_history
    if err := a.pruneJobs(ctx, summary); err != nil {
        return nil, err
    }
    // job_dependencies -> job_dependencies_history
    if err := a.pruneDependencies(ctx, summary); err != nil {
        return nil, err
    }

    return summary, nil
}

func (a *AdminSQL) pruneJobDefinitions(ctx context.Context, summary *repository.PruneSummary) error {
    const countQ = `SELECT count(*) FROM job_definitions WHERE deleted = TRUE;`
    const insertQ = `
INSERT INTO job_definitions_history (def_id, namespace, name, version, kind, payload_template, cron_spec, delay_interval, created_at, deleted)
SELECT def_id, namespace, name, version, kind, payload_template, cron_spec, delay_interval, created_at, deleted
FROM job_definitions
WHERE deleted = TRUE;
`
    const deleteQ = `DELETE FROM job_definitions WHERE deleted = TRUE;`

    if err := a.dal.DB.QueryRow(ctx, countQ).Scan(&summary.JobDefinitionsArchived); err != nil {
        return err
    }
    if summary.JobDefinitionsArchived == 0 {
        return nil
    }
    if _, err := a.dal.DB.Exec(ctx, insertQ); err != nil {
        return err
    }
    if _, err := a.dal.DB.Exec(ctx, deleteQ); err != nil {
        return err
    }
    return nil
}

func (a *AdminSQL) pruneDAGs(ctx context.Context, summary *repository.PruneSummary) error {
    const countQ = `SELECT count(*) FROM dags WHERE deleted = TRUE;`
    const insertQ = `
INSERT INTO dags_history (id, namespace, name, version, created_at, deleted)
SELECT id, namespace, name, version, created_at, deleted
FROM dags
WHERE deleted = TRUE;
`
    const deleteQ = `DELETE FROM dags WHERE deleted = TRUE;`

    if err := a.dal.DB.QueryRow(ctx, countQ).Scan(&summary.DagsArchived); err != nil {
        return err
    }
    if summary.DagsArchived == 0 {
        return nil
    }
    if _, err := a.dal.DB.Exec(ctx, insertQ); err != nil {
        return err
    }
    if _, err := a.dal.DB.Exec(ctx, deleteQ); err != nil {
        return err
    }
    return nil
}

func (a *AdminSQL) pruneJobs(ctx context.Context, summary *repository.PruneSummary) error {
    const countQ = `
SELECT count(*) FROM jobs 
WHERE deleted = TRUE OR status IN ('succeeded','failed','cancelled');
`
    const insertQ = `
INSERT INTO jobs_history (id, dag_id, def_id, version, deleted, status, priority, due_at, payload_json, binary_data,
                          lease_owner, lease_until, enqueued_at, started_at, finished_at, last_error, last_scheduled_at)
SELECT id, dag_id, def_id, version, deleted, status, priority, due_at, payload_json, binary_data,
       lease_owner, lease_until, enqueued_at, started_at, finished_at, last_error, last_scheduled_at
FROM jobs
WHERE deleted = TRUE OR status IN ('succeeded','failed','cancelled');
`
    const deleteQ = `
DELETE FROM jobs
WHERE deleted = TRUE OR status IN ('succeeded','failed','cancelled');
`

    if err := a.dal.DB.QueryRow(ctx, countQ).Scan(&summary.JobsArchived); err != nil {
        return err
    }
    if summary.JobsArchived == 0 {
        return nil
    }
    if _, err := a.dal.DB.Exec(ctx, insertQ); err != nil {
        return err
    }
    if _, err := a.dal.DB.Exec(ctx, deleteQ); err != nil {
        return err
    }
    return nil
}

func (a *AdminSQL) pruneDependencies(ctx context.Context, summary *repository.PruneSummary) error {
    const countQ = `SELECT count(*) FROM job_dependencies WHERE deleted = TRUE;`
    const insertQ = `
INSERT INTO job_dependencies_history (dag_id, dag_version, parent_job_id, child_job_id, dependency_type, created_at, deleted)
SELECT dag_id, dag_version, parent_job_id, child_job_id, dependency_type, created_at, deleted
FROM job_dependencies
WHERE deleted = TRUE;
`
    const deleteQ = `DELETE FROM job_dependencies WHERE deleted = TRUE;`

    if err := a.dal.DB.QueryRow(ctx, countQ).Scan(&summary.DependenciesArchived); err != nil {
        return err
    }
    if summary.DependenciesArchived == 0 {
        return nil
    }
    if _, err := a.dal.DB.Exec(ctx, insertQ); err != nil {
        return err
    }
    if _, err := a.dal.DB.Exec(ctx, deleteQ); err != nil {
        return err
    }
    return nil
}
