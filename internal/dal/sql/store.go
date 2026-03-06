package sql

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/edkuperman/chronosched/internal/dag"
	"github.com/edkuperman/chronosched/internal/repository"
	"github.com/jackc/pgx/v5"
)

type Store struct {
	dal *SQLDAL
}

func NewStore(dal *SQLDAL) *Store { return &Store{dal: dal} }

func nullStringPtr(ns stdsql.NullString) *string {
	if !ns.Valid {
		return nil
	}
	s := ns.String
	return &s
}

func timePtr(nt stdsql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	t := nt.Time
	return &t
}

func normalizePayload(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

// ===== Namespaces =====
func (s *Store) ListNamespaces(ctx context.Context) ([]repository.Namespace, error) {
	rows, err := s.dal.DB.Query(ctx, `SELECT namespace_id::text, name, created_at FROM namespaces ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.Namespace
	for rows.Next() {
		var n repository.Namespace
		if err := rows.Scan(&n.ID, &n.Name, &n.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *Store) CreateNamespace(ctx context.Context, name string) (*repository.Namespace, error) {
	var n repository.Namespace
	err := s.dal.DB.QueryRow(ctx, `INSERT INTO namespaces(name) VALUES($1) RETURNING namespace_id::text, name, created_at`, name).
		Scan(&n.ID, &n.Name, &n.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func (s *Store) GetNamespaceByName(ctx context.Context, name string) (*repository.Namespace, error) {
	var n repository.Namespace
	err := s.dal.DB.QueryRow(ctx, `SELECT namespace_id::text, name, created_at FROM namespaces WHERE name=$1`, name).
		Scan(&n.ID, &n.Name, &n.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

// ===== Definitions =====
func schedulePolicyOrDefault(sched *repository.Schedule) string {
	if sched == nil || sched.OnFailure == "" {
		return "continue"
	}
	return sched.OnFailure
}

func scheduleTypeOrDefault(sched *repository.Schedule) string {
	if sched == nil || sched.Type == "" {
		if sched != nil && sched.IntervalSeconds != nil {
			return "interval"
		}
		if sched != nil && sched.Cron != "" {
			return "cron"
		}
		return ""
	}
	return sched.Type
}

func buildSchedule(scheduleType string, cron, tz, onFailure stdsql.NullString, intervalSeconds stdsql.NullInt32, startAt stdsql.NullTime) *repository.Schedule {
	if scheduleType == "" && !cron.Valid && !intervalSeconds.Valid {
		return nil
	}
	sched := &repository.Schedule{Type: scheduleType}
	if scheduleType == "" {
		if intervalSeconds.Valid {
			sched.Type = "interval"
		} else if cron.Valid {
			sched.Type = "cron"
		}
	}
	if cron.Valid {
		sched.Cron = cron.String
	}
	if intervalSeconds.Valid {
		v := int(intervalSeconds.Int32)
		sched.IntervalSeconds = &v
	}
	if startAt.Valid {
		t := startAt.Time.UTC()
		sched.StartAt = &t
	}
	if tz.Valid {
		sched.Timezone = tz.String
	}
	if onFailure.Valid {
		sched.OnFailure = onFailure.String
	}
	return sched
}

func (s *Store) ListByNamespace(ctx context.Context, namespaceID string) ([]repository.JobDefinition, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT definition_id::text, namespace_id::text, name, description, kind, payload_template, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused, created_at, updated_at
FROM job_definitions
WHERE namespace_id=$1
ORDER BY name`, namespaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.JobDefinition
	for rows.Next() {
		var d repository.JobDefinition
		var scheduleType, cron, tz, onFailure stdsql.NullString
		var intervalSeconds stdsql.NullInt32
		var startAt stdsql.NullTime
		if err := rows.Scan(&d.ID, &d.NamespaceID, &d.Name, &d.Description, &d.Kind, &d.PayloadTemplate, &scheduleType, &cron, &intervalSeconds, &startAt, &tz, &onFailure, &d.IsEnabled, &d.IsPaused, &d.CreatedAt, &d.UpdatedAt); err != nil {
			return nil, err
		}
		d.Schedule = buildSchedule(scheduleType.String, cron, tz, onFailure, intervalSeconds, startAt)
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) CreateDefinition(ctx context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
	var d repository.JobDefinition
	var scheduleType, cron, tz, onFailure *string
	var intervalSeconds *int
	var startAt *time.Time
	if def.Schedule != nil {
		st := scheduleTypeOrDefault(def.Schedule)
		if st != "" {
			scheduleType = &st
		}
		if def.Schedule.Cron != "" {
			cron = &def.Schedule.Cron
		}
		if def.Schedule.IntervalSeconds != nil {
			intervalSeconds = def.Schedule.IntervalSeconds
		}
		if def.Schedule.StartAt != nil {
			t := def.Schedule.StartAt.UTC()
			startAt = &t
		}
		if def.Schedule.Timezone != "" {
			tz = &def.Schedule.Timezone
		}
		policy := schedulePolicyOrDefault(def.Schedule)
		onFailure = &policy
	}
	var scheduleTypeOut, cronOut, tzOut, onFailureOut stdsql.NullString
	var intervalSecondsOut stdsql.NullInt32
	var startAtOut stdsql.NullTime
	err := s.dal.DB.QueryRow(ctx, `
INSERT INTO job_definitions(namespace_id, name, description, kind, payload_template, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
RETURNING definition_id::text, namespace_id::text, name, description, kind, payload_template, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused, created_at, updated_at`,
		def.NamespaceID, def.Name, def.Description, def.Kind, normalizePayload(def.PayloadTemplate), scheduleType, cron, intervalSeconds, startAt, tz, onFailure, def.IsEnabled, def.IsPaused).
		Scan(&d.ID, &d.NamespaceID, &d.Name, &d.Description, &d.Kind, &d.PayloadTemplate, &scheduleTypeOut, &cronOut, &intervalSecondsOut, &startAtOut, &tzOut, &onFailureOut, &d.IsEnabled, &d.IsPaused, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, err
	}
	d.Schedule = buildSchedule(scheduleTypeOut.String, cronOut, tzOut, onFailureOut, intervalSecondsOut, startAtOut)
	return &d, nil
}

func (s *Store) GetDefinition(ctx context.Context, id string) (*repository.JobDefinition, error) {
	var d repository.JobDefinition
	var scheduleType, cron, tz, onFailure stdsql.NullString
	var intervalSeconds stdsql.NullInt32
	var startAt stdsql.NullTime
	err := s.dal.DB.QueryRow(ctx, `
SELECT definition_id::text, namespace_id::text, name, description, kind, payload_template, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused, created_at, updated_at
FROM job_definitions WHERE definition_id=$1`, id).
		Scan(&d.ID, &d.NamespaceID, &d.Name, &d.Description, &d.Kind, &d.PayloadTemplate, &scheduleType, &cron, &intervalSeconds, &startAt, &tz, &onFailure, &d.IsEnabled, &d.IsPaused, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, err
	}
	d.Schedule = buildSchedule(scheduleType.String, cron, tz, onFailure, intervalSeconds, startAt)
	return &d, nil
}

func (s *Store) UpdateDefinition(ctx context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
	var d repository.JobDefinition
	var scheduleType, cron, tz, onFailure *string
	var intervalSeconds *int
	var startAt *time.Time
	if def.Schedule != nil {
		st := scheduleTypeOrDefault(def.Schedule)
		if st != "" {
			scheduleType = &st
		}
		if def.Schedule.Cron != "" {
			cron = &def.Schedule.Cron
		}
		if def.Schedule.IntervalSeconds != nil {
			intervalSeconds = def.Schedule.IntervalSeconds
		}
		if def.Schedule.StartAt != nil {
			t := def.Schedule.StartAt.UTC()
			startAt = &t
		}
		if def.Schedule.Timezone != "" {
			tz = &def.Schedule.Timezone
		}
		policy := schedulePolicyOrDefault(def.Schedule)
		onFailure = &policy
	}
	var scheduleTypeOut, cronOut, tzOut, onFailureOut stdsql.NullString
	var intervalSecondsOut stdsql.NullInt32
	var startAtOut stdsql.NullTime
	err := s.dal.DB.QueryRow(ctx, `
UPDATE job_definitions
SET name=$2, description=$3, kind=$4, payload_template=$5, schedule_type=$6, cron_spec=$7, interval_seconds=$8, interval_start_at=$9, timezone=$10, on_failure_policy=$11, is_enabled=$12, is_paused=$13, updated_at=now()
WHERE definition_id=$1
RETURNING definition_id::text, namespace_id::text, name, description, kind, payload_template, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused, created_at, updated_at`,
		def.ID, def.Name, def.Description, def.Kind, normalizePayload(def.PayloadTemplate), scheduleType, cron, intervalSeconds, startAt, tz, onFailure, def.IsEnabled, def.IsPaused).
		Scan(&d.ID, &d.NamespaceID, &d.Name, &d.Description, &d.Kind, &d.PayloadTemplate, &scheduleTypeOut, &cronOut, &intervalSecondsOut, &startAtOut, &tzOut, &onFailureOut, &d.IsEnabled, &d.IsPaused, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, err
	}
	d.Schedule = buildSchedule(scheduleTypeOut.String, cronOut, tzOut, onFailureOut, intervalSecondsOut, startAtOut)
	return &d, nil
}

func (s *Store) DeleteDefinition(ctx context.Context, id string) error {
	_, err := s.dal.DB.Exec(ctx, `DELETE FROM job_definitions WHERE definition_id=$1`, id)
	return err
}

func (s *Store) SetEnabled(ctx context.Context, id string, enabled bool) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE job_definitions SET is_enabled=$2, updated_at=now() WHERE definition_id=$1`, id, enabled)
	return err
}

func (s *Store) SetPaused(ctx context.Context, id string, paused bool) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE job_definitions SET is_paused=$2, updated_at=now() WHERE definition_id=$1`, id, paused)
	return err
}

func (s *Store) ApplyFailurePolicy(ctx context.Context, jobID int64) error {
	var definitionID, policy string
	err := s.dal.DB.QueryRow(ctx, `
SELECT j.job_definition_id::text, COALESCE(jd.on_failure_policy,'continue')
FROM jobs j
JOIN job_definitions jd ON jd.definition_id=j.job_definition_id
WHERE j.job_id=$1`, jobID).Scan(&definitionID, &policy)
	if err != nil {
		return err
	}
	switch policy {
	case "pause":
		return s.SetPaused(ctx, definitionID, true)
	case "disable":
		return s.SetEnabled(ctx, definitionID, false)
	default:
		return nil
	}
}

func (s *Store) ListUsages(ctx context.Context, definitionID string) ([]repository.DefinitionUsage, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT d.dag_id::text, dg.name, d.dag_version_id::text, d.version_number, n.node_key, n.display_name,
       (dg.active_version_id = d.dag_version_id) AS is_active
FROM dag_version_nodes n
JOIN dag_versions d ON d.dag_version_id=n.dag_version_id
JOIN dags dg ON dg.dag_id=d.dag_id
WHERE n.job_definition_id=$1
ORDER BY dg.name, d.version_number, n.node_key`, definitionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.DefinitionUsage
	for rows.Next() {
		var u repository.DefinitionUsage
		if err := rows.Scan(&u.DAGID, &u.DAGName, &u.DAGVersionID, &u.VersionNumber, &u.NodeKey, &u.DisplayName, &u.IsActive); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Store) ListScheduledUsages(ctx context.Context) ([]repository.ScheduledUsage, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT d.dag_id::text, v.dag_version_id::text, d.name, v.version_number,
       n.node_id::text, n.node_key, n.display_name,
       jd.definition_id::text, jd.name, COALESCE(jd.schedule_type,''), COALESCE(jd.cron_spec,''), jd.interval_seconds, jd.interval_start_at, COALESCE(jd.timezone,''), jd.is_enabled, jd.is_paused, COALESCE(jd.on_failure_policy,'continue'), v.created_at
FROM dags d
JOIN dag_versions v ON v.dag_version_id = d.active_version_id
JOIN dag_version_nodes n ON n.dag_version_id = v.dag_version_id
JOIN job_definitions jd ON jd.definition_id = n.job_definition_id
WHERE jd.is_enabled = TRUE
  AND jd.is_paused = FALSE
  AND (
    (COALESCE(jd.schedule_type,'') IN ('', 'cron') AND jd.cron_spec IS NOT NULL AND btrim(jd.cron_spec) <> '')
    OR
    (jd.schedule_type='interval' AND jd.interval_seconds IS NOT NULL AND jd.interval_seconds > 0 AND jd.interval_start_at IS NOT NULL)
  )
ORDER BY d.name, n.node_key`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.ScheduledUsage
	for rows.Next() {
		var u repository.ScheduledUsage
		var intervalSeconds stdsql.NullInt32
		var startAt stdsql.NullTime
		if err := rows.Scan(&u.DAGID, &u.DAGVersionID, &u.DAGName, &u.VersionNumber, &u.NodeID, &u.NodeKey, &u.DisplayName, &u.DefinitionID, &u.DefinitionName, &u.ScheduleType, &u.CronSpec, &intervalSeconds, &startAt, &u.Timezone, &u.DefinitionEnabled, &u.DefinitionPaused, &u.OnFailurePolicy, &u.VersionCreatedAt); err != nil {
			return nil, err
		}
		if intervalSeconds.Valid {
			v := int(intervalSeconds.Int32)
			u.IntervalSeconds = &v
		}
		if startAt.Valid {
			t := startAt.Time.UTC()
			u.StartAt = &t
		}
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Store) ListScheduledParents(ctx context.Context, dagVersionID, nodeID string) ([]repository.ScheduledParent, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT pn.node_id::text, pn.node_key, jd.definition_id::text, jd.name, COALESCE(jd.schedule_type,''), COALESCE(jd.cron_spec,''), jd.interval_seconds, jd.interval_start_at, COALESCE(jd.timezone,''), jd.is_enabled, jd.is_paused
FROM dag_version_edges e
JOIN dag_version_nodes pn ON pn.node_id=e.from_node_id
JOIN job_definitions jd ON jd.definition_id=pn.job_definition_id
WHERE e.dag_version_id=$1 AND e.to_node_id=$2
ORDER BY pn.node_key`, dagVersionID, nodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.ScheduledParent
	for rows.Next() {
		var p repository.ScheduledParent
		var intervalSeconds stdsql.NullInt32
		var startAt stdsql.NullTime
		if err := rows.Scan(&p.NodeID, &p.NodeKey, &p.DefinitionID, &p.DefinitionName, &p.ScheduleType, &p.CronSpec, &intervalSeconds, &startAt, &p.Timezone, &p.DefinitionEnabled, &p.DefinitionPaused); err != nil {
			return nil, err
		}
		if intervalSeconds.Valid {
			v := int(intervalSeconds.Int32)
			p.IntervalSeconds = &v
		}
		if startAt.Valid {
			t := startAt.Time.UTC()
			p.StartAt = &t
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) GetCronFireStatus(ctx context.Context, nodeID string, scheduledAt time.Time) (*repository.CronFireStatus, error) {
	var status repository.RunStatus
	var runID int64
	err := s.dal.DB.QueryRow(ctx, `
SELECT r.status, r.run_id
FROM cron_fires f
JOIN dag_runs r ON r.run_id=f.run_id
WHERE f.node_id=$1 AND f.scheduled_at=$2`, nodeID, scheduledAt).Scan(&status, &runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return &repository.CronFireStatus{Exists: false}, nil
	}
	if err != nil {
		return nil, err
	}
	return &repository.CronFireStatus{Exists: true, RunID: runID, Status: status}, nil
}

func (s *Store) GetCronNextRun(ctx context.Context, nodeID string) (*time.Time, error) {
	var nt stdsql.NullTime
	err := s.dal.DB.QueryRow(ctx, `SELECT next_run_at FROM cron_state WHERE node_id=$1`, nodeID).Scan(&nt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if !nt.Valid {
		return nil, nil
	}
	t := nt.Time
	return &t, nil
}

func (s *Store) SetCronNextRun(ctx context.Context, nodeID string, nextRunAt time.Time) error {
	_, err := s.dal.DB.Exec(ctx, `
INSERT INTO cron_state(node_id, next_run_at)
VALUES($1,$2)
ON CONFLICT (node_id) DO UPDATE SET next_run_at=EXCLUDED.next_run_at`, nodeID, nextRunAt)
	return err
}

func (s *Store) GetRunSchedulingMeta(ctx context.Context, runID int64) (*repository.RunSchedulingMeta, error) {
	const q = `
SELECT
  trigger_type,
  dag_version_id::text,
  trigger_node_id::text,
  scheduled_at
FROM dag_runs
WHERE run_id = $1
`

	var meta repository.RunSchedulingMeta
	var triggerNodeID stdsql.NullString

	err := s.dal.DB.QueryRow(ctx, q, runID).Scan(
		&meta.TriggerType,
		&meta.DAGVersionID,
		&triggerNodeID,
		&meta.ScheduledAt,
	)
	if err != nil {
		return nil, err
	}

	if triggerNodeID.Valid {
		meta.TriggerNodeID = triggerNodeID.String
	}

	return &meta, nil
}

// ===== DAGs =====
func (s *Store) CreateDAG(ctx context.Context, namespaceID, name, description string) (*repository.DAG, error) {
	var dagRow repository.DAG
	err := s.dal.DB.QueryRow(ctx, `
INSERT INTO dags(namespace_id, name, description)
VALUES($1,$2,$3)
RETURNING dag_id::text, namespace_id::text, name, description, active_version_id::text, 0, created_at`, namespaceID, name, description).
		Scan(&dagRow.ID, &dagRow.NamespaceID, &dagRow.Name, &dagRow.Description, &dagRow.ActiveVersionID, &dagRow.LatestVersionNumber, &dagRow.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &dagRow, nil
}

func (s *Store) ListByNamespaceDAG(ctx context.Context, namespaceID string) ([]repository.DAG, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT d.dag_id::text, d.namespace_id::text, d.name, d.description, d.active_version_id::text,
       COALESCE(MAX(v.version_number),0) AS latest_version_number,
       d.created_at
FROM dags d
LEFT JOIN dag_versions v ON v.dag_id=d.dag_id
WHERE d.namespace_id=$1
GROUP BY d.dag_id, d.namespace_id, d.name, d.description, d.active_version_id, d.created_at
ORDER BY d.name`, namespaceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.DAG
	for rows.Next() {
		var d repository.DAG
		if err := rows.Scan(&d.ID, &d.NamespaceID, &d.Name, &d.Description, &d.ActiveVersionID, &d.LatestVersionNumber, &d.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func (s *Store) GetDAG(ctx context.Context, dagID string) (*repository.DAG, error) {
	var d repository.DAG
	err := s.dal.DB.QueryRow(ctx, `
SELECT d.dag_id::text, d.namespace_id::text, d.name, d.description, d.active_version_id::text,
       COALESCE((SELECT MAX(version_number) FROM dag_versions v WHERE v.dag_id=d.dag_id),0) AS latest_version_number,
       d.created_at
FROM dags d WHERE d.dag_id=$1`, dagID).
		Scan(&d.ID, &d.NamespaceID, &d.Name, &d.Description, &d.ActiveVersionID, &d.LatestVersionNumber, &d.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

func validateVersionInput(input repository.DAGVersionCreateInput) error {
	if len(input.Nodes) == 0 {
		return errors.New("at least one node is required")
	}
	nodeKeys := map[string]bool{}
	definitionIDs := map[string]bool{}
	for _, n := range input.Nodes {
		if n.NodeKey == "" || n.JobDefinitionID == "" {
			return errors.New("each node requires node_key and job_definition_id")
		}
		if nodeKeys[n.NodeKey] {
			return fmt.Errorf("duplicate node_key: %s", n.NodeKey)
		}
		nodeKeys[n.NodeKey] = true
		definitionIDs[n.JobDefinitionID] = true
	}
	var edges []dag.Edge
	for _, e := range input.Edges {
		if !nodeKeys[e.From] || !nodeKeys[e.To] {
			return fmt.Errorf("edge references unknown node: %s -> %s", e.From, e.To)
		}
		edges = append(edges, dag.Edge{From: e.From, To: e.To})
	}
	if dag.HasCycle(edges) {
		return errors.New("dag version contains a cycle")
	}
	return nil
}

func (s *Store) DeleteDAG(ctx context.Context, dagID string) error {
	_, err := s.dal.DB.Exec(ctx, `DELETE FROM dags WHERE dag_id=$1`, dagID)
	return err
}

func (s *Store) CreateVersion(ctx context.Context, dagID string, input repository.DAGVersionCreateInput) (*repository.DAGVersion, error) {
	if input.BasedOnVersionID != nil && len(input.Nodes) == 0 && len(input.Edges) == 0 {
		graph, err := s.GetVersionGraph(ctx, *input.BasedOnVersionID)
		if err != nil {
			return nil, err
		}
		input.Nodes = make([]repository.DAGVersionInputNode, 0, len(graph.Nodes))
		for _, n := range graph.Nodes {
			input.Nodes = append(input.Nodes, repository.DAGVersionInputNode{NodeKey: n.NodeKey, DisplayName: n.DisplayName, JobDefinitionID: n.JobDefinitionID})
		}
		input.Edges = make([]repository.DAGVersionInputEdge, 0, len(graph.Edges))
		for _, e := range graph.Edges {
			input.Edges = append(input.Edges, repository.DAGVersionInputEdge{From: e.FromNodeKey, To: e.ToNodeKey})
		}
	}
	if err := validateVersionInput(input); err != nil {
		return nil, err
	}

	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	var nextVersion int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(version_number),0)+1 FROM dag_versions WHERE dag_id=$1`, dagID).Scan(&nextVersion); err != nil {
		return nil, err
	}

	var v repository.DAGVersion
	err = tx.QueryRow(ctx, `
INSERT INTO dag_versions(dag_id, version_number, version_note, based_on_version_id)
VALUES($1,$2,$3,$4)
RETURNING dag_version_id::text, dag_id::text, version_number, version_note, based_on_version_id::text, FALSE, created_at`, dagID, nextVersion, input.VersionNote, input.BasedOnVersionID).
		Scan(&v.ID, &v.DAGID, &v.VersionNumber, &v.VersionNote, &v.BasedOnVersionID, &v.IsActive, &v.CreatedAt)
	if err != nil {
		return nil, err
	}

	type nodeRef struct{ id string }
	nodeIDs := map[string]string{}
	for _, n := range input.Nodes {
		displayName := n.DisplayName
		if displayName == "" {
			displayName = n.NodeKey
		}
		var nodeID string
		err := tx.QueryRow(ctx, `
INSERT INTO dag_version_nodes(dag_version_id, node_key, display_name, job_definition_id)
VALUES($1,$2,$3,$4)
RETURNING node_id::text`, v.ID, n.NodeKey, displayName, n.JobDefinitionID).Scan(&nodeID)
		if err != nil {
			return nil, err
		}
		nodeIDs[n.NodeKey] = nodeID
	}
	for _, e := range input.Edges {
		if _, err := tx.Exec(ctx, `INSERT INTO dag_version_edges(dag_version_id, from_node_id, to_node_id) VALUES($1,$2,$3)`, v.ID, nodeIDs[e.From], nodeIDs[e.To]); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &v, nil
}

func (s *Store) ListVersions(ctx context.Context, dagID string) ([]repository.DAGVersion, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT v.dag_version_id::text, v.dag_id::text, v.version_number, v.version_note, v.based_on_version_id::text,
       (d.active_version_id = v.dag_version_id) AS is_active,
       v.created_at
FROM dag_versions v
JOIN dags d ON d.dag_id=v.dag_id
WHERE v.dag_id=$1
ORDER BY v.version_number DESC`, dagID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.DAGVersion
	for rows.Next() {
		var v repository.DAGVersion
		if err := rows.Scan(&v.ID, &v.DAGID, &v.VersionNumber, &v.VersionNote, &v.BasedOnVersionID, &v.IsActive, &v.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (s *Store) GetVersion(ctx context.Context, dagVersionID string) (*repository.DAGVersion, error) {
	var v repository.DAGVersion
	err := s.dal.DB.QueryRow(ctx, `
SELECT v.dag_version_id::text, v.dag_id::text, v.version_number, v.version_note, v.based_on_version_id::text,
       (d.active_version_id = v.dag_version_id) AS is_active,
       v.created_at
FROM dag_versions v
JOIN dags d ON d.dag_id=v.dag_id
WHERE v.dag_version_id=$1`, dagVersionID).
		Scan(&v.ID, &v.DAGID, &v.VersionNumber, &v.VersionNote, &v.BasedOnVersionID, &v.IsActive, &v.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (s *Store) ActivateVersion(ctx context.Context, dagVersionID string) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE dags d SET active_version_id=v.dag_version_id FROM dag_versions v WHERE v.dag_version_id=$1 AND d.dag_id=v.dag_id`, dagVersionID)
	if err != nil {
		return err
	}
	_, err = s.dal.DB.Exec(ctx, `
INSERT INTO cron_state(node_id, next_run_at)
SELECT n.node_id, NULL
FROM dag_version_nodes n
JOIN dag_versions v ON v.dag_version_id=n.dag_version_id
JOIN dags d ON d.active_version_id=v.dag_version_id
JOIN job_definitions jd ON jd.definition_id=n.job_definition_id
WHERE v.dag_version_id=$1 AND ((COALESCE(jd.schedule_type,'') IN ('', 'cron') AND jd.cron_spec IS NOT NULL AND btrim(jd.cron_spec)<>'') OR (jd.schedule_type='interval' AND jd.interval_seconds IS NOT NULL AND jd.interval_seconds > 0 AND jd.interval_start_at IS NOT NULL))
ON CONFLICT (node_id) DO NOTHING`, dagVersionID)
	return err
}

func (s *Store) RevertVersion(ctx context.Context, dagVersionID string, activate bool, note string) (*repository.DAGVersion, error) {
	src, err := s.GetVersion(ctx, dagVersionID)
	if err != nil {
		return nil, err
	}
	v, err := s.CreateVersion(ctx, src.DAGID, repository.DAGVersionCreateInput{VersionNote: note, BasedOnVersionID: &dagVersionID})
	if err != nil {
		return nil, err
	}
	if activate {
		if err := s.ActivateVersion(ctx, v.ID); err != nil {
			return nil, err
		}
		v.IsActive = true
	}
	return v, nil
}

func (s *Store) GetVersionGraph(ctx context.Context, dagVersionID string) (*repository.DAGVersionGraph, error) {
	graph := &repository.DAGVersionGraph{}
	err := s.dal.DB.QueryRow(ctx, `
SELECT v.dag_version_id::text, v.dag_id::text, d.name, v.version_number, (d.active_version_id = v.dag_version_id) AS is_active
FROM dag_versions v JOIN dags d ON d.dag_id=v.dag_id WHERE v.dag_version_id=$1`, dagVersionID).
		Scan(&graph.DAGVersionID, &graph.DAGID, &graph.DAGName, &graph.VersionNumber, &graph.IsActive)
	if err != nil {
		return nil, err
	}

	nodeRows, err := s.dal.DB.Query(ctx, `
SELECT n.node_id::text, n.node_key, n.display_name, jd.definition_id::text, jd.name, COALESCE(jd.schedule_type,''), jd.cron_spec, jd.interval_seconds, jd.interval_start_at, jd.timezone, jd.on_failure_policy
FROM dag_version_nodes n
JOIN job_definitions jd ON jd.definition_id=n.job_definition_id
WHERE n.dag_version_id=$1
ORDER BY n.node_key`, dagVersionID)
	if err != nil {
		return nil, err
	}
	defer nodeRows.Close()
	for nodeRows.Next() {
		var n repository.DAGVersionNode
		var scheduleType, cron, tz, onFailure stdsql.NullString
		var intervalSeconds stdsql.NullInt32
		var startAt stdsql.NullTime
		if err := nodeRows.Scan(&n.ID, &n.NodeKey, &n.DisplayName, &n.JobDefinitionID, &n.JobDefinitionName, &scheduleType, &cron, &intervalSeconds, &startAt, &tz, &onFailure); err != nil {
			return nil, err
		}
		n.Schedule = buildSchedule(scheduleType.String, cron, tz, onFailure, intervalSeconds, startAt)
		graph.Nodes = append(graph.Nodes, n)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, err
	}

	edgeRows, err := s.dal.DB.Query(ctx, `
SELECT pn.node_key, cn.node_key
FROM dag_version_edges e
JOIN dag_version_nodes pn ON pn.node_id=e.from_node_id
JOIN dag_version_nodes cn ON cn.node_id=e.to_node_id
WHERE e.dag_version_id=$1
ORDER BY pn.node_key, cn.node_key`, dagVersionID)
	if err != nil {
		return nil, err
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var e repository.DAGVersionEdge
		if err := edgeRows.Scan(&e.FromNodeKey, &e.ToNodeKey); err != nil {
			return nil, err
		}
		graph.Edges = append(graph.Edges, e)
	}
	return graph, edgeRows.Err()
}

// ===== Runs and materialization =====
type materializeNode struct {
	NodeID            string
	NodeKey           string
	DisplayName       string
	JobDefinitionID   string
	JobDefinitionName string
	PayloadTemplate   json.RawMessage
}

type materializeEdge struct{ FromNodeID, ToNodeID string }

func (s *Store) loadVersionForMaterialization(ctx context.Context, tx pgx.Tx, dagVersionID string) (string, string, int, []materializeNode, []materializeEdge, error) {
	var dagID, dagName string
	var versionNumber int
	if err := tx.QueryRow(ctx, `SELECT v.dag_id::text, d.name, v.version_number FROM dag_versions v JOIN dags d ON d.dag_id=v.dag_id WHERE v.dag_version_id=$1`, dagVersionID).Scan(&dagID, &dagName, &versionNumber); err != nil {
		return "", "", 0, nil, nil, err
	}
	nodeRows, err := tx.Query(ctx, `
SELECT n.node_id::text, n.node_key, n.display_name, jd.definition_id::text, jd.name, jd.payload_template
FROM dag_version_nodes n
JOIN job_definitions jd ON jd.definition_id=n.job_definition_id
WHERE n.dag_version_id=$1`, dagVersionID)
	if err != nil {
		return "", "", 0, nil, nil, err
	}
	defer nodeRows.Close()
	var nodes []materializeNode
	for nodeRows.Next() {
		var n materializeNode
		if err := nodeRows.Scan(&n.NodeID, &n.NodeKey, &n.DisplayName, &n.JobDefinitionID, &n.JobDefinitionName, &n.PayloadTemplate); err != nil {
			return "", "", 0, nil, nil, err
		}
		nodes = append(nodes, n)
	}
	if err := nodeRows.Err(); err != nil {
		return "", "", 0, nil, nil, err
	}
	edgeRows, err := tx.Query(ctx, `SELECT from_node_id::text, to_node_id::text FROM dag_version_edges WHERE dag_version_id=$1`, dagVersionID)
	if err != nil {
		return "", "", 0, nil, nil, err
	}
	defer edgeRows.Close()
	var edges []materializeEdge
	for edgeRows.Next() {
		var e materializeEdge
		if err := edgeRows.Scan(&e.FromNodeID, &e.ToNodeID); err != nil {
			return "", "", 0, nil, nil, err
		}
		edges = append(edges, e)
	}
	return dagID, dagName, versionNumber, nodes, edges, edgeRows.Err()
}

func relevantNodeIDs(triggerNodeID string, nodes []materializeNode, edges []materializeEdge) map[string]bool {
	if triggerNodeID == "" {
		out := map[string]bool{}
		for _, n := range nodes {
			out[n.NodeID] = true
		}
		return out
	}
	return map[string]bool{triggerNodeID: true}
}

func triggerDefinitionName(nodes []materializeNode, nodeID string) *string {
	for _, n := range nodes {
		if n.NodeID == nodeID {
			name := n.JobDefinitionName
			return &name
		}
	}
	return nil
}

func (s *Store) materializeJobsForRun(ctx context.Context, tx pgx.Tx, runID int64, triggerNodeID string, scheduledAt time.Time, nodes []materializeNode, edges []materializeEdge) error {
	include := relevantNodeIDs(triggerNodeID, nodes, edges)
	nodeToJob := map[string]int64{}
	for _, n := range nodes {
		if !include[n.NodeID] {
			continue
		}
		var jobID int64
		err := tx.QueryRow(ctx, `
INSERT INTO jobs(run_id, dag_version_node_id, job_definition_id, node_key, display_name, status, priority, due_at, payload_json)
VALUES($1,$2,$3,$4,$5,'waiting',0,$6,$7)
RETURNING job_id`, runID, n.NodeID, n.JobDefinitionID, n.NodeKey, n.DisplayName, scheduledAt, normalizePayload(n.PayloadTemplate)).Scan(&jobID)
		if err != nil {
			return err
		}
		nodeToJob[n.NodeID] = jobID
	}
	parentCount := map[int64]int{}
	for _, e := range edges {
		fromJob, okFrom := nodeToJob[e.FromNodeID]
		toJob, okTo := nodeToJob[e.ToNodeID]
		if !okFrom || !okTo {
			continue
		}
		if _, err := tx.Exec(ctx, `INSERT INTO job_dependencies(parent_job_id, child_job_id) VALUES($1,$2)`, fromJob, toJob); err != nil {
			return err
		}
		parentCount[toJob]++
	}
	for _, jobID := range nodeToJob {
		ready := parentCount[jobID] == 0
		if _, err := tx.Exec(ctx, `INSERT INTO job_frontier(job_id, ready) VALUES($1,$2)`, jobID, ready); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) materializeRun(ctx context.Context, tx pgx.Tx, dagVersionID, triggerNodeID string, scheduledAt time.Time, triggerType string, definitionID *string) (*repository.DAGRun, error) {
	dagID, dagName, versionNumber, nodes, edges, err := s.loadVersionForMaterialization(ctx, tx, dagVersionID)
	if err != nil {
		return nil, err
	}
	var run repository.DAGRun
	var trigDefName *string
	if triggerNodeID != "" {
		trigDefName = triggerDefinitionName(nodes, triggerNodeID)
	}
	err = tx.QueryRow(ctx, `
INSERT INTO dag_runs(dag_id, dag_version_id, trigger_type, trigger_node_id, trigger_definition_id, scheduled_at, status)
VALUES($1,$2,$3,NULLIF($4,'')::uuid,$5,$6,'waiting')
RETURNING run_id, dag_id::text, dag_version_id::text, scheduled_at, status, created_at`, dagID, dagVersionID, triggerType, triggerNodeID, definitionID, scheduledAt).
		Scan(&run.ID, &run.DAGID, &run.DAGVersionID, &run.ScheduledAt, &run.Status, &run.CreatedAt)
	if err != nil {
		return nil, err
	}
	run.DAGName = dagName
	run.VersionNumber = versionNumber
	run.Trigger = repository.RunTrigger{Type: triggerType, DefinitionID: definitionID, DefinitionName: trigDefName}
	if err := s.materializeJobsForRun(ctx, tx, run.ID, triggerNodeID, scheduledAt, nodes, edges); err != nil {
		return nil, err
	}
	return &run, nil
}

func (s *Store) CreateManualRun(ctx context.Context, dagID string, dagVersionID *string, scheduledAt time.Time) (*repository.DAGRun, error) {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	chosen := ""
	if dagVersionID != nil && *dagVersionID != "" {
		chosen = *dagVersionID
	} else {
		if err := tx.QueryRow(ctx, `SELECT active_version_id::text FROM dags WHERE dag_id=$1`, dagID).Scan(&chosen); err != nil {
			return nil, err
		}
	}
	run, err := s.materializeRun(ctx, tx, chosen, "", scheduledAt, "manual", nil)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return run, nil
}

func (s *Store) CreateScheduledRun(ctx context.Context, dagVersionID, triggerNodeID, definitionID, triggerType string, scheduledAt time.Time) (*repository.DAGRun, error) {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	dagID, dagName, versionNumber, nodes, edges, err := s.loadVersionForMaterialization(ctx, tx, dagVersionID)
	if err != nil {
		return nil, err
	}
	var run repository.DAGRun
	var createdAt time.Time
	trigDefName := triggerDefinitionName(nodes, triggerNodeID)
	err = tx.QueryRow(ctx, `
INSERT INTO dag_runs(dag_id, dag_version_id, trigger_type, trigger_node_id, trigger_definition_id, scheduled_at, status)
VALUES($1,$2,$3,$4,$5,$6,'waiting')
ON CONFLICT DO NOTHING
RETURNING run_id, created_at`, dagID, dagVersionID, triggerType, triggerNodeID, definitionID, scheduledAt).Scan(&run.ID, &createdAt)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if err == nil {
		run.DAGID = dagID
		run.DAGVersionID = dagVersionID
		run.DAGName = dagName
		run.VersionNumber = versionNumber
		run.ScheduledAt = scheduledAt
		run.Status = repository.RunStatusWaiting
		run.CreatedAt = createdAt
		run.Trigger = repository.RunTrigger{Type: triggerType, DefinitionID: &definitionID, DefinitionName: trigDefName}
		if err := s.materializeJobsForRun(ctx, tx, run.ID, triggerNodeID, scheduledAt, nodes, edges); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO cron_fires(node_id, scheduled_at, run_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, triggerNodeID, scheduledAt, run.ID); err != nil {
			return nil, err
		}
	} else {
		existing, err := s.getScheduledRunTx(ctx, tx, triggerNodeID, triggerType, scheduledAt)
		if err != nil {
			return nil, err
		}
		run = *existing
	}
	if _, err := tx.Exec(ctx, `UPDATE cron_state SET next_run_at=NULL WHERE node_id=$1`, triggerNodeID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &run, nil
}

func (s *Store) getScheduledRunTx(ctx context.Context, tx pgx.Tx, triggerNodeID, triggerType string, scheduledAt time.Time) (*repository.DAGRun, error) {
	var run repository.DAGRun
	var triggerDefID, triggerDefName stdsql.NullString
	var started, finished stdsql.NullTime
	err := tx.QueryRow(ctx, `
SELECT r.run_id, r.dag_id::text, r.dag_version_id::text, d.name, v.version_number,
       r.trigger_type, r.trigger_definition_id::text, jd.name,
       r.status, r.scheduled_at, r.created_at, r.started_at, r.finished_at
FROM dag_runs r
JOIN dags d ON d.dag_id=r.dag_id
JOIN dag_versions v ON v.dag_version_id=r.dag_version_id
LEFT JOIN job_definitions jd ON jd.definition_id=r.trigger_definition_id
WHERE r.trigger_type=$2 AND r.trigger_node_id=$1 AND r.scheduled_at=$3`, triggerNodeID, triggerType, scheduledAt).
		Scan(&run.ID, &run.DAGID, &run.DAGVersionID, &run.DAGName, &run.VersionNumber, &triggerType, &triggerDefID, &triggerDefName, &run.Status, &run.ScheduledAt, &run.CreatedAt, &started, &finished)
	if err != nil {
		return nil, err
	}
	run.Trigger = repository.RunTrigger{Type: triggerType, DefinitionID: nullStringPtr(triggerDefID), DefinitionName: nullStringPtr(triggerDefName)}
	run.StartedAt = timePtr(started)
	run.FinishedAt = timePtr(finished)
	return &run, nil
}

func (s *Store) GetSchedulingMeta(ctx context.Context, runID int64) (*repository.RunSchedulingMeta, error) {
	var meta repository.RunSchedulingMeta
	var triggerNodeID stdsql.NullString
	err := s.dal.DB.QueryRow(ctx, `
SELECT trigger_type, dag_version_id::text, trigger_node_id::text, scheduled_at
FROM dag_runs
WHERE run_id=$1`, runID).Scan(&meta.TriggerType, &meta.DAGVersionID, &triggerNodeID, &meta.ScheduledAt)
	if err != nil {
		return nil, err
	}
	if triggerNodeID.Valid {
		meta.TriggerNodeID = triggerNodeID.String
	}
	return &meta, nil
}

func (s *Store) ListByDAGRuns(ctx context.Context, dagID string) ([]repository.DAGRun, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT r.run_id, r.dag_id::text, r.dag_version_id::text, d.name, v.version_number,
       r.trigger_type, r.trigger_definition_id::text, jd.name,
       r.status, r.scheduled_at, r.created_at, r.started_at, r.finished_at
FROM dag_runs r
JOIN dags d ON d.dag_id=r.dag_id
JOIN dag_versions v ON v.dag_version_id=r.dag_version_id
LEFT JOIN job_definitions jd ON jd.definition_id=r.trigger_definition_id
WHERE r.dag_id=$1
ORDER BY r.run_id DESC`, dagID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.DAGRun
	for rows.Next() {
		var run repository.DAGRun
		var triggerDefID, triggerDefName stdsql.NullString
		var triggerType string
		var started, finished stdsql.NullTime
		if err := rows.Scan(&run.ID, &run.DAGID, &run.DAGVersionID, &run.DAGName, &run.VersionNumber, &triggerType, &triggerDefID, &triggerDefName, &run.Status, &run.ScheduledAt, &run.CreatedAt, &started, &finished); err != nil {
			return nil, err
		}
		run.Trigger = repository.RunTrigger{Type: triggerType, DefinitionID: nullStringPtr(triggerDefID), DefinitionName: nullStringPtr(triggerDefName)}
		run.StartedAt = timePtr(started)
		run.FinishedAt = timePtr(finished)
		out = append(out, run)
	}
	return out, rows.Err()
}

func (s *Store) GetRun(ctx context.Context, runID int64) (*repository.DAGRun, error) {
	var run repository.DAGRun
	var triggerDefID, triggerDefName stdsql.NullString
	var triggerType string
	var started, finished stdsql.NullTime
	err := s.dal.DB.QueryRow(ctx, `
SELECT r.run_id, r.dag_id::text, r.dag_version_id::text, d.name, v.version_number,
       r.trigger_type, r.trigger_definition_id::text, jd.name,
       r.status, r.scheduled_at, r.created_at, r.started_at, r.finished_at
FROM dag_runs r
JOIN dags d ON d.dag_id=r.dag_id
JOIN dag_versions v ON v.dag_version_id=r.dag_version_id
LEFT JOIN job_definitions jd ON jd.definition_id=r.trigger_definition_id
WHERE r.run_id=$1`, runID).
		Scan(&run.ID, &run.DAGID, &run.DAGVersionID, &run.DAGName, &run.VersionNumber, &triggerType, &triggerDefID, &triggerDefName, &run.Status, &run.ScheduledAt, &run.CreatedAt, &started, &finished)
	if err != nil {
		return nil, err
	}
	run.Trigger = repository.RunTrigger{Type: triggerType, DefinitionID: nullStringPtr(triggerDefID), DefinitionName: nullStringPtr(triggerDefName)}
	run.StartedAt = timePtr(started)
	run.FinishedAt = timePtr(finished)
	return &run, nil
}

func (s *Store) ListJobs(ctx context.Context, runID int64) ([]repository.RunJob, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT j.job_id, j.run_id, j.node_key, j.display_name, j.job_definition_id::text, jd.name, j.status,
       j.due_at, j.started_at, j.finished_at, j.last_error, COALESCE(f.ready, FALSE)
FROM jobs j
JOIN job_definitions jd ON jd.definition_id=j.job_definition_id
LEFT JOIN job_frontier f ON f.job_id=j.job_id
WHERE j.run_id=$1
ORDER BY j.job_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.RunJob
	for rows.Next() {
		var j repository.RunJob
		var started, finished stdsql.NullTime
		var lastErr stdsql.NullString
		var ready bool
		if err := rows.Scan(&j.JobID, &j.RunID, &j.NodeKey, &j.DisplayName, &j.JobDefinitionID, &j.JobDefinitionName, &j.Status, &j.DueAt, &started, &finished, &lastErr, &ready); err != nil {
			return nil, err
		}
		j.StartedAt = timePtr(started)
		j.FinishedAt = timePtr(finished)
		j.LastError = nullStringPtr(lastErr)
		j.IsReady = &ready
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) GetGraph(ctx context.Context, runID int64) (*repository.RunGraph, error) {
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	jobs, err := s.ListJobs(ctx, runID)
	if err != nil {
		return nil, err
	}
	rows, err := s.dal.DB.Query(ctx, `
SELECT d.parent_job_id, d.child_job_id
FROM job_dependencies d
JOIN jobs j ON j.job_id=d.parent_job_id
WHERE j.run_id=$1
ORDER BY d.parent_job_id, d.child_job_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var edges []repository.RunGraphEdge
	for rows.Next() {
		var e repository.RunGraphEdge
		if err := rows.Scan(&e.FromJobID, &e.ToJobID); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	return &repository.RunGraph{Run: *run, Nodes: jobs, Edges: edges}, rows.Err()
}

func (s *Store) RefreshStatus(ctx context.Context, runID int64) error {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var waiting, queued, running, succeeded, failed, missed int
	err = tx.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE status='waiting'),
  count(*) FILTER (WHERE status='queued'),
  count(*) FILTER (WHERE status='running'),
  count(*) FILTER (WHERE status='succeeded'),
  count(*) FILTER (WHERE status='failed'),
  count(*) FILTER (WHERE status='missed')
FROM jobs WHERE run_id=$1`, runID).Scan(&waiting, &queued, &running, &succeeded, &failed, &missed)
	if err != nil {
		return err
	}
	status := repository.RunStatusWaiting
	if failed > 0 {
		status = repository.RunStatusFailed
	} else if missed > 0 {
		status = repository.RunStatusMissed
	} else if waiting == 0 && queued == 0 && running == 0 {
		status = repository.RunStatusSucceeded
	} else if running > 0 || queued > 0 {
		status = repository.RunStatusRunning
	}
	_, err = tx.Exec(ctx, `
UPDATE dag_runs
SET status=$2::run_status,
    started_at = CASE WHEN started_at IS NULL AND $2::run_status='running'::run_status THEN now() ELSE started_at END,
    finished_at = CASE WHEN $2::run_status IN ('succeeded'::run_status,'failed'::run_status,'missed'::run_status,'cancelled'::run_status) THEN now() ELSE finished_at END
WHERE run_id=$1`, runID, status)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ===== Jobs =====
func (s *Store) GetJobExecution(ctx context.Context, id int64) (*repository.JobExecution, error) {
	var ex repository.JobExecution
	err := s.dal.DB.QueryRow(ctx, `
SELECT j.job_id, j.node_key, jd.kind, j.payload_json, j.job_definition_id::text
FROM jobs j
JOIN job_definitions jd ON jd.definition_id=j.job_definition_id
WHERE j.job_id=$1`, id).Scan(&ex.JobID, &ex.NodeKey, &ex.Kind, &ex.Payload, &ex.Definition)
	if err != nil {
		return nil, err
	}
	return &ex, nil
}

func (s *Store) FindDueReadyWaiting(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT j.job_id, j.run_id, j.status, j.priority, j.due_at, j.node_key, j.job_definition_id::text
FROM jobs j
JOIN job_frontier f ON f.job_id=j.job_id
WHERE j.status='waiting' AND f.ready=TRUE AND j.due_at <= $1
ORDER BY j.due_at, j.job_id
LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*repository.Job
	for rows.Next() {
		var j repository.Job
		if err := rows.Scan(&j.ID, &j.RunID, &j.Status, &j.Priority, &j.DueAt, &j.NodeKey, &j.DefID); err != nil {
			return nil, err
		}
		out = append(out, &j)
	}
	return out, rows.Err()
}

func (s *Store) MarkQueued(ctx context.Context, id int64) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='queued' WHERE job_id=$1 AND status='waiting'`, id)
	return err
}

func (s *Store) MarkRunning(ctx context.Context, id int64) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='running', started_at=COALESCE(started_at, now()) WHERE job_id=$1`, id)
	return err
}

func (s *Store) MarkSucceeded(ctx context.Context, id int64) error {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `UPDATE jobs SET status='succeeded', finished_at=now(), lease_owner=NULL, lease_until=NULL WHERE job_id=$1`, id)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
UPDATE job_frontier f
SET ready = NOT EXISTS (
    SELECT 1
    FROM job_dependencies d
    JOIN jobs p ON p.job_id=d.parent_job_id
    WHERE d.child_job_id = f.job_id AND p.status <> 'succeeded'
)
WHERE f.job_id IN (SELECT child_job_id FROM job_dependencies WHERE parent_job_id=$1)`, id)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) MarkFailed(ctx context.Context, id int64, reason string) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='failed', finished_at=now(), last_error=$2, lease_owner=NULL, lease_until=NULL WHERE job_id=$1`, id, reason)
	return err
}

func (s *Store) MarkMissed(ctx context.Context, id int64, reason string) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='missed', finished_at=now(), last_error=$2, lease_owner=NULL, lease_until=NULL WHERE job_id=$1`, id, reason)
	return err
}

func (s *Store) GetReadiness(ctx context.Context, id int64) (*repository.JobReadiness, error) {
	var r repository.JobReadiness
	if err := s.dal.DB.QueryRow(ctx, `
SELECT j.job_id, j.status, COALESCE(f.ready, FALSE)
FROM jobs j LEFT JOIN job_frontier f ON f.job_id=j.job_id
WHERE j.job_id=$1`, id).Scan(&r.JobID, &r.Status, &r.IsReady); err != nil {
		return nil, err
	}
	rows, err := s.dal.DB.Query(ctx, `
SELECT p.job_id, p.node_key, p.status
FROM job_dependencies d
JOIN jobs p ON p.job_id=d.parent_job_id
WHERE d.child_job_id=$1 AND p.status <> 'succeeded'
ORDER BY p.job_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b repository.BlockingParent
		if err := rows.Scan(&b.JobID, &b.NodeKey, &b.Status); err != nil {
			return nil, err
		}
		r.BlockingUpstreams = append(r.BlockingUpstreams, b)
	}
	return &r, rows.Err()
}

func (s *Store) GetRunID(ctx context.Context, id int64) (int64, error) {
	var runID int64
	err := s.dal.DB.QueryRow(ctx, `SELECT run_id FROM jobs WHERE job_id=$1`, id).Scan(&runID)
	return runID, err
}

// ===== Queue =====
func (s *Store) Enqueue(ctx context.Context, jobID int64, runAt time.Time, priority int) error {
	_, err := s.dal.DB.Exec(ctx, `INSERT INTO job_queue(job_id, available_at, priority) VALUES($1,$2,$3) ON CONFLICT (job_id) DO NOTHING`, jobID, runAt, priority)
	return err
}

func (s *Store) Dequeue(ctx context.Context, workerID string, n int, vt time.Duration) ([]repository.QueueItem, error) {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
WITH picked AS (
  SELECT q.id
  FROM job_queue q
  JOIN jobs j ON j.job_id=q.job_id
  WHERE (q.reserved_until IS NULL OR q.reserved_until < now())
    AND q.available_at <= now()
    AND j.status='queued'
  ORDER BY q.priority DESC, q.available_at, q.id
  FOR UPDATE SKIP LOCKED
  LIMIT $1
)
UPDATE job_queue q
SET reserved_until = now() + $3::interval,
    consumer_id = $2,
    updated_at = now()
FROM picked, jobs j, job_definitions jd
WHERE q.id = picked.id
  AND j.job_id = q.job_id
  AND jd.definition_id = j.job_definition_id
RETURNING q.id, q.job_id, q.attempts, q.priority, j.node_key, jd.kind, j.payload_json, j.job_definition_id::text`, n, workerID, vt.String())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []repository.QueueItem
	var jobIDs []int64
	for rows.Next() {
		var it repository.QueueItem
		if err := rows.Scan(&it.QueueID, &it.JobID, &it.Attempts, &it.Priority, &it.NodeKey, &it.Kind, &it.Payload, &it.Definition); err != nil {
			return nil, err
		}
		items = append(items, it)
		jobIDs = append(jobIDs, it.JobID)
	}
	for _, jobID := range jobIDs {
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status='running', started_at=COALESCE(started_at, now()), lease_owner=$2, lease_until=now()+$3::interval WHERE job_id=$1`, jobID, workerID, vt.String()); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) Ack(ctx context.Context, queueID int64, workerID string) error {
	_, err := s.dal.DB.Exec(ctx, `DELETE FROM job_queue WHERE id=$1 AND consumer_id=$2`, queueID, workerID)
	return err
}

func (s *Store) Fail(ctx context.Context, queueID int64, workerID string, delay time.Duration) error {
	_, err := s.dal.DB.Exec(ctx, `
UPDATE job_queue
SET attempts=attempts+1, reserved_until=NULL, consumer_id=NULL, available_at=now()+$3::interval, updated_at=now()
WHERE id=$1 AND consumer_id=$2`, queueID, workerID, delay.String())
	return err
}

// ===== Admin =====
func (s *Store) CheckGlobalCycles(ctx context.Context) (map[string]any, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT v.dag_version_id::text, d.name, pn.node_key, cn.node_key
FROM dag_version_edges e
JOIN dag_versions v ON v.dag_version_id=e.dag_version_id
JOIN dags d ON d.dag_id=v.dag_id
JOIN dag_version_nodes pn ON pn.node_id=e.from_node_id
JOIN dag_version_nodes cn ON cn.node_id=e.to_node_id
ORDER BY v.dag_version_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type entry struct {
		dagName string
		edges   []dag.Edge
	}
	graphs := map[string]*entry{}
	for rows.Next() {
		var versionID, dagName, from, to string
		if err := rows.Scan(&versionID, &dagName, &from, &to); err != nil {
			return nil, err
		}
		if graphs[versionID] == nil {
			graphs[versionID] = &entry{dagName: dagName}
		}
		graphs[versionID].edges = append(graphs[versionID].edges, dag.Edge{From: from, To: to})
	}
	results := []map[string]any{}
	for versionID, g := range graphs {
		cycles := dag.DetectCycles(g.edges)
		if len(cycles) > 0 {
			results = append(results, map[string]any{"dag_version_id": versionID, "dag_name": g.dagName, "cycles": cycles})
		}
	}
	sort.Slice(results, func(i, j int) bool { return results[i]["dag_name"].(string) < results[j]["dag_name"].(string) })
	return map[string]any{"count": len(results), "results": results}, nil
}
