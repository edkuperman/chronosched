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
	"github.com/edkuperman/chronosched/internal/events"
	"github.com/edkuperman/chronosched/internal/logger"
	"github.com/edkuperman/chronosched/internal/repository"
	"github.com/jackc/pgx/v5"
)

type Store struct {
	dal       *SQLDAL
	publisher events.EventPublisher
}

func NewStore(dal *SQLDAL) *Store { return &Store{dal: dal, publisher: events.DefaultPublisher()} }

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

type rowQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type jobEventMeta struct {
	NamespaceID string
	DAGID       string
	RunID       int64
	JobID       int64
	NodeKey     string
	Status      repository.JobStatus
}

type runEventMeta struct {
	NamespaceID string
	DAGID       string
	RunID       int64
	Status      repository.RunStatus
	TriggerType string
}

func (s *Store) publishEvent(ctx context.Context, evt events.Event) {
	if s.publisher == nil {
		return
	}
	if evt.EventID == "" {
		evt.EventID = events.NewEventID()
	}
	if evt.OccurredAt.IsZero() {
		evt.OccurredAt = time.Now().UTC()
	}
	if err := s.publisher.Publish(ctx, evt); err != nil {
		logger.Error(err, "failed to publish event", "event_type", evt.EventType, "job_id", evt.JobID, "run_id", evt.RunID)
	}
}

func (s *Store) loadJobEventMeta(ctx context.Context, q rowQueryer, jobID int64) (*jobEventMeta, error) {
	var meta jobEventMeta
	err := q.QueryRow(ctx, `
		SELECT 
			d.namespace_id::text, 
			dr.dag_id::text, 
			j.run_id, 
			j.job_id, 
			j.node_key, 
			j.status
		FROM jobs j
		JOIN dag_runs dr ON dr.run_id=j.run_id
		JOIN dags d ON d.dag_id=dr.dag_id
		WHERE j.job_id=$1`, jobID).Scan(&meta.NamespaceID, &meta.DAGID, &meta.RunID, &meta.JobID, &meta.NodeKey, &meta.Status)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *Store) loadRunEventMeta(ctx context.Context, q rowQueryer, runID int64) (*runEventMeta, error) {
	var meta runEventMeta
	err := q.QueryRow(ctx, `
		SELECT 
			d.namespace_id::text, 
			dr.dag_id::text, 
			dr.run_id, dr.status, 
			dr.trigger_type
		FROM dag_runs dr
		JOIN dags d ON d.dag_id=dr.dag_id
		WHERE dr.run_id=$1`, runID).Scan(&meta.NamespaceID, &meta.DAGID, &meta.RunID, &meta.Status, &meta.TriggerType)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

// ===== Namespaces =====
func (s *Store) ListNamespaces(ctx context.Context) ([]repository.Namespace, error) {
	rows, err := s.dal.DB.Query(ctx, `
		SELECT 
			namespace_id::text, 
			name, 
			created_at 
		FROM namespaces 
		ORDER BY name`)
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
	err := s.dal.DB.QueryRow(ctx, `
		INSERT INTO namespaces(name) 
		VALUES($1) 
		RETURNING 
			namespace_id::text, 
			name, 
			created_at`, 
		name).Scan(&n.ID, &n.Name, &n.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &n, nil
}

func (s *Store) GetNamespaceByName(ctx context.Context, name string) (*repository.Namespace, error) {
	var n repository.Namespace
	err := s.dal.DB.QueryRow(ctx, `
		SELECT 
			namespace_id::text, 
			name, 
			created_at 
		FROM namespaces 
		WHERE name=$1`, 
		name).Scan(&n.ID, &n.Name, &n.CreatedAt)
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

func scheduleParams(sched *repository.Schedule) (scheduleType, cron, tz, onFailure *string, intervalSeconds *int, startAt *time.Time) {
	if sched == nil {
		return nil, nil, nil, nil, nil, nil
	}
	st := scheduleTypeOrDefault(sched)
	if st != "" {
		scheduleType = &st
	}
	if sched.Cron != "" {
		cron = &sched.Cron
	}
	if sched.IntervalSeconds != nil {
		intervalSeconds = sched.IntervalSeconds
	}
	if sched.StartAt != nil {
		t := sched.StartAt.UTC()
		startAt = &t
	}
	if sched.Timezone != "" {
		tz = &sched.Timezone
	}
	policy := schedulePolicyOrDefault(sched)
	onFailure = &policy
	return
}

func hasScheduleConfig(sched *repository.Schedule) bool {
	if sched == nil {
		return false
	}
	return scheduleTypeOrDefault(sched) != "" || sched.Cron != "" || sched.IntervalSeconds != nil
}

func (s *Store) syncBindingsForDefinition(ctx context.Context, definitionID string) error {
	_, err := s.dal.DB.Exec(ctx, `
		WITH 
		def_sched AS (
			SELECT 
				definition_id, 
				schedule_type, 
				cron_spec, 
				interval_seconds, 
				interval_start_at, 
				timezone, 
				on_failure_policy, 
				is_enabled, 
				is_paused
			FROM job_definitions
			WHERE definition_id=$1
		), 
		deleted AS (
			DELETE FROM schedule_bindings sb
			USING def_sched ds
			WHERE 
				sb.definition_id=ds.definition_id
				AND sb.source_type='definition_inline'
				AND (ds.schedule_type IS NULL OR 
					((ds.schedule_type='cron') AND (ds.cron_spec IS NULL OR btrim(ds.cron_spec)='')) OR 
					(ds.schedule_type='interval' AND (ds.interval_seconds IS NULL OR ds.interval_start_at IS NULL)))
			RETURNING sb.binding_id
		)
		INSERT INTO schedule_bindings(dag_version_id, node_id, definition_id, source_type, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused)
		SELECT n.dag_version_id, n.node_id, n.job_definition_id, 'definition_inline', ds.schedule_type, ds.cron_spec, ds.interval_seconds, ds.interval_start_at, ds.timezone, ds.on_failure_policy, ds.is_enabled, ds.is_paused
		FROM dag_version_nodes n
		JOIN def_sched ds ON ds.definition_id=n.job_definition_id
		WHERE ds.schedule_type IS NOT NULL
			AND ((ds.schedule_type='cron' AND ds.cron_spec IS NOT NULL AND btrim(ds.cron_spec) <> '') OR 
				(ds.schedule_type='interval' AND ds.interval_seconds IS NOT NULL AND ds.interval_seconds > 0 AND ds.interval_start_at IS NOT NULL))
		ON CONFLICT (node_id) DO 
			UPDATE SET
				schedule_type=EXCLUDED.schedule_type,
				cron_spec=EXCLUDED.cron_spec,
				interval_seconds=EXCLUDED.interval_seconds,
				interval_start_at=EXCLUDED.interval_start_at,
				timezone=EXCLUDED.timezone,
				on_failure_policy=EXCLUDED.on_failure_policy,
				is_enabled=EXCLUDED.is_enabled,
				is_paused=EXCLUDED.is_paused,
				updated_at=now()`, 
		definitionID)
	return err
}

func (s *Store) syncNodeBindingFromDefinitionTx(ctx context.Context, tx pgx.Tx, dagVersionID, nodeID, definitionID string) error {
	_, err := tx.Exec(ctx, `
		WITH 
		def_sched AS (
			SELECT 
				definition_id, 
				schedule_type, 
				cron_spec, 
				interval_seconds, 
				interval_start_at, 
				timezone, 
				on_failure_policy, 
				is_enabled, 
				is_paused
			FROM job_definitions
			WHERE definition_id=$1
		), 
		deleted AS (
			DELETE FROM schedule_bindings sb
			USING def_sched ds
			WHERE sb.node_id=$2
				AND sb.source_type='definition_inline'
				AND (ds.schedule_type IS NULL OR ((ds.schedule_type='cron') AND (ds.cron_spec IS NULL OR btrim(ds.cron_spec)='')) OR (ds.schedule_type='interval' AND (ds.interval_seconds IS NULL OR ds.interval_start_at IS NULL)))
			RETURNING sb.binding_id
		)
		INSERT INTO schedule_bindings(dag_version_id, node_id, definition_id, source_type, schedule_type, cron_spec, interval_seconds, interval_start_at, timezone, on_failure_policy, is_enabled, is_paused)
		SELECT $3, $2, ds.definition_id, 'definition_inline', ds.schedule_type, ds.cron_spec, ds.interval_seconds, ds.interval_start_at, ds.timezone, ds.on_failure_policy, ds.is_enabled, ds.is_paused
		FROM def_sched ds
		WHERE ds.schedule_type IS NOT NULL
			AND ((ds.schedule_type='cron' AND ds.cron_spec IS NOT NULL AND btrim(ds.cron_spec) <> '') OR (ds.schedule_type='interval' AND ds.interval_seconds IS NOT NULL AND ds.interval_seconds > 0 AND ds.interval_start_at IS NOT NULL))
		ON CONFLICT (node_id) DO 
			UPDATE SET
				schedule_type=EXCLUDED.schedule_type,
				cron_spec=EXCLUDED.cron_spec,
				interval_seconds=EXCLUDED.interval_seconds,
				interval_start_at=EXCLUDED.interval_start_at,
				timezone=EXCLUDED.timezone,
				on_failure_policy=EXCLUDED.on_failure_policy,
				is_enabled=EXCLUDED.is_enabled,
				is_paused=EXCLUDED.is_paused,
				updated_at=now()`, 
		definitionID, nodeID, dagVersionID)
	return err
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
	scheduleType, cron, tz, onFailure, intervalSeconds, startAt := scheduleParams(def.Schedule)
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
	if hasScheduleConfig(d.Schedule) {
		if err := s.syncBindingsForDefinition(ctx, d.ID); err != nil {
			return nil, err
		}
	}
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
	scheduleType, cron, tz, onFailure, intervalSeconds, startAt := scheduleParams(def.Schedule)
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
	if err := s.syncBindingsForDefinition(ctx, d.ID); err != nil {
		return nil, err
	}
	return &d, nil
}

func (s *Store) DeleteDefinition(ctx context.Context, id string) error {
	_, err := s.dal.DB.Exec(ctx, `DELETE FROM job_definitions WHERE definition_id=$1`, id)
	return err
}

func (s *Store) SetEnabled(ctx context.Context, id string, enabled bool) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE job_definitions SET is_enabled=$2, updated_at=now() WHERE definition_id=$1`, id, enabled)
	if err != nil {
		return err
	}
	_, err = s.dal.DB.Exec(ctx, `UPDATE schedule_bindings SET is_enabled=$2, updated_at=now() WHERE definition_id=$1 AND source_type='definition_inline'`, id, enabled)
	return err
}

func (s *Store) SetPaused(ctx context.Context, id string, paused bool) error {
	_, err := s.dal.DB.Exec(ctx, `UPDATE job_definitions SET is_paused=$2, updated_at=now() WHERE definition_id=$1`, id, paused)
	if err != nil {
		return err
	}
	_, err = s.dal.DB.Exec(ctx, `UPDATE schedule_bindings SET is_paused=$2, updated_at=now() WHERE definition_id=$1 AND source_type='definition_inline'`, id, paused)
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
       jd.definition_id::text, jd.name, sb.schedule_type, COALESCE(sb.cron_spec,''), sb.interval_seconds, sb.interval_start_at, COALESCE(sb.timezone,''), sb.is_enabled, sb.is_paused, COALESCE(sb.on_failure_policy,'continue'), v.created_at
FROM dags d
JOIN dag_versions v ON v.dag_version_id = d.active_version_id
JOIN dag_version_nodes n ON n.dag_version_id = v.dag_version_id
JOIN job_definitions jd ON jd.definition_id = n.job_definition_id
JOIN schedule_bindings sb ON sb.node_id = n.node_id AND sb.dag_version_id = v.dag_version_id
WHERE sb.is_enabled = TRUE
  AND sb.is_paused = FALSE
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
SELECT pn.node_id::text, pn.node_key, jd.definition_id::text, jd.name, sb.schedule_type, COALESCE(sb.cron_spec,''), sb.interval_seconds, sb.interval_start_at, COALESCE(sb.timezone,''), sb.is_enabled, sb.is_paused
FROM dag_version_edges e
JOIN dag_version_nodes pn ON pn.node_id=e.from_node_id
JOIN job_definitions jd ON jd.definition_id=pn.job_definition_id
JOIN schedule_bindings sb ON sb.node_id=pn.node_id AND sb.dag_version_id=e.dag_version_id
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
		if err := s.syncNodeBindingFromDefinitionTx(ctx, tx, v.ID, nodeID, n.JobDefinitionID); err != nil {
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
SELECT sb.node_id, NULL
FROM schedule_bindings sb
WHERE sb.dag_version_id=$1 AND sb.is_enabled=TRUE AND sb.is_paused=FALSE
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
SELECT n.node_id::text, n.node_key, n.display_name, jd.definition_id::text, jd.name, COALESCE(sb.schedule_type,''), sb.cron_spec, sb.interval_seconds, sb.interval_start_at, sb.timezone, sb.on_failure_policy
FROM dag_version_nodes n
JOIN job_definitions jd ON jd.definition_id=n.job_definition_id
LEFT JOIN schedule_bindings sb ON sb.node_id=n.node_id AND sb.dag_version_id=n.dag_version_id
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
	if meta, err := s.loadRunEventMeta(ctx, s.dal.DB, run.ID); err == nil {
		s.publishEvent(ctx, events.Event{EventType: "run.created", NamespaceID: meta.NamespaceID, DAGID: meta.DAGID, RunID: meta.RunID, TriggerType: meta.TriggerType})
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
	if run.ID != 0 {
		if meta, err := s.loadRunEventMeta(ctx, s.dal.DB, run.ID); err == nil {
			s.publishEvent(ctx, events.Event{EventType: "run.created", NamespaceID: meta.NamespaceID, DAGID: meta.DAGID, RunID: meta.RunID, TriggerType: meta.TriggerType})
		}
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
       j.due_at, j.dispatched_at, j.started_at, j.last_heartbeat_at, j.finished_at, j.external_execution_id, j.reason_code, j.last_error, COALESCE(f.ready, FALSE)
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
		var dispatched, started, heartbeat, finished stdsql.NullTime
		var externalID, reasonCode, lastErr stdsql.NullString
		var ready bool
		if err := rows.Scan(&j.JobID, &j.RunID, &j.NodeKey, &j.DisplayName, &j.JobDefinitionID, &j.JobDefinitionName, &j.Status, &j.DueAt, &dispatched, &started, &heartbeat, &finished, &externalID, &reasonCode, &lastErr, &ready); err != nil {
			return nil, err
		}
		j.DispatchedAt = timePtr(dispatched)
		j.StartedAt = timePtr(started)
		j.LastHeartbeatAt = timePtr(heartbeat)
		j.FinishedAt = timePtr(finished)
		j.ExternalExecutionID = nullStringPtr(externalID)
		j.ReasonCode = nullStringPtr(reasonCode)
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
	if run.Trigger.Type == "manual" {
		return s.getRunGraph(ctx, run, runID)
	}
	graph, err := s.getWorkflowGraph(ctx, run)
	if err == nil {
		return graph, nil
	}
	return s.getRunGraph(ctx, run, runID)
}

func (s *Store) getRunGraph(ctx context.Context, run *repository.DAGRun, runID int64) (*repository.RunGraph, error) {
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
	edges := make([]repository.RunGraphEdge, 0)
	for rows.Next() {
		var e repository.RunGraphEdge
		if err := rows.Scan(&e.FromJobID, &e.ToJobID); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	return &repository.RunGraph{Run: *run, Nodes: jobs, Edges: edges}, rows.Err()
}

func (s *Store) getWorkflowGraph(ctx context.Context, run *repository.DAGRun) (*repository.RunGraph, error) {
	jobsRows, err := s.dal.DB.Query(ctx, `
SELECT j.job_id, j.run_id, j.node_key, j.display_name, j.job_definition_id::text, jd.name, j.status,
       j.due_at, j.dispatched_at, j.started_at, j.last_heartbeat_at, j.finished_at, j.external_execution_id, j.reason_code, j.last_error, COALESCE(f.ready, FALSE)
FROM jobs j
JOIN job_definitions jd ON jd.definition_id=j.job_definition_id
LEFT JOIN job_frontier f ON f.job_id=j.job_id
WHERE j.run_id IN (
    SELECT run_id
    FROM dag_runs
    WHERE dag_version_id=$1 AND scheduled_at=$2
)
ORDER BY j.run_id, j.job_id`, run.DAGVersionID, run.ScheduledAt)
	if err != nil {
		return nil, err
	}
	defer jobsRows.Close()
	jobs := make([]repository.RunJob, 0)
	for jobsRows.Next() {
		var j repository.RunJob
		var dispatched, started, heartbeat, finished stdsql.NullTime
		var externalID, reasonCode, lastErr stdsql.NullString
		var ready bool
		if err := jobsRows.Scan(&j.JobID, &j.RunID, &j.NodeKey, &j.DisplayName, &j.JobDefinitionID, &j.JobDefinitionName, &j.Status, &j.DueAt, &dispatched, &started, &heartbeat, &finished, &externalID, &reasonCode, &lastErr, &ready); err != nil {
			return nil, err
		}
		j.DispatchedAt = timePtr(dispatched)
		j.StartedAt = timePtr(started)
		j.LastHeartbeatAt = timePtr(heartbeat)
		j.FinishedAt = timePtr(finished)
		j.ExternalExecutionID = nullStringPtr(externalID)
		j.ReasonCode = nullStringPtr(reasonCode)
		j.LastError = nullStringPtr(lastErr)
		j.IsReady = &ready
		jobs = append(jobs, j)
	}
	if err := jobsRows.Err(); err != nil {
		return nil, err
	}
	edgeRows, err := s.dal.DB.Query(ctx, `
SELECT DISTINCT fromj.job_id, toj.job_id
FROM dag_version_edges e
JOIN dag_version_nodes fromn ON fromn.node_id=e.from_node_id
JOIN dag_version_nodes ton ON ton.node_id=e.to_node_id
JOIN jobs fromj ON fromj.node_key=fromn.node_key
JOIN jobs toj ON toj.node_key=ton.node_key
JOIN dag_runs fromr ON fromr.run_id=fromj.run_id
JOIN dag_runs tor ON tor.run_id=toj.run_id
WHERE e.dag_version_id=$1
  AND fromr.dag_version_id=$1 AND fromr.scheduled_at=$2
  AND tor.dag_version_id=$1 AND tor.scheduled_at=$2
ORDER BY fromj.job_id, toj.job_id`, run.DAGVersionID, run.ScheduledAt)
	if err != nil {
		return nil, err
	}
	defer edgeRows.Close()
	edges := make([]repository.RunGraphEdge, 0)
	for edgeRows.Next() {
		var e repository.RunGraphEdge
		if err := edgeRows.Scan(&e.FromJobID, &e.ToJobID); err != nil {
			return nil, err
		}
		edges = append(edges, e)
	}
	if err := edgeRows.Err(); err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return s.getRunGraph(ctx, run, run.ID)
	}
	return &repository.RunGraph{Run: *run, Nodes: jobs, Edges: edges}, nil
}

func (s *Store) RefreshStatus(ctx context.Context, runID int64) error {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	prevMeta, err := s.loadRunEventMeta(ctx, tx, runID)
	if err != nil {
		return err
	}

	var waiting, queued, running, succeeded, failed, missed, blocked int
	err = tx.QueryRow(ctx, `
SELECT
  count(*) FILTER (WHERE status='waiting'),
  count(*) FILTER (WHERE status='queued'),
  count(*) FILTER (WHERE status IN ('dispatching','dispatched','running')),
  count(*) FILTER (WHERE status='succeeded'),
  count(*) FILTER (WHERE status IN ('failed','lost')),
  count(*) FILTER (WHERE status='missed'),
  count(*) FILTER (WHERE status='blocked')
FROM jobs WHERE run_id=$1`, runID).Scan(&waiting, &queued, &running, &succeeded, &failed, &missed, &blocked)
	if err != nil {
		return err
	}
	status := repository.RunStatusWaiting
	if failed > 0 || blocked > 0 {
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
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if prevMeta.Status != status {
		s.publishEvent(ctx, events.Event{
			EventType:   "run.status_changed",
			NamespaceID: prevMeta.NamespaceID,
			DAGID:       prevMeta.DAGID,
			RunID:       prevMeta.RunID,
			OldStatus:   string(prevMeta.Status),
			NewStatus:   string(status),
			TriggerType: prevMeta.TriggerType,
		})
	}
	return nil
}

// ===== Jobs =====
func (s *Store) GetJobExecution(ctx context.Context, id int64) (*repository.JobExecution, error) {
	var ex repository.JobExecution
	err := s.dal.DB.QueryRow(ctx, `
SELECT j.job_id, j.run_id, j.node_key, jd.kind, j.payload_json, j.job_definition_id::text
FROM jobs j
JOIN job_definitions jd ON jd.definition_id=j.job_definition_id
WHERE j.job_id=$1`, id).Scan(&ex.JobID, &ex.RunID, &ex.NodeKey, &ex.Kind, &ex.Payload, &ex.Definition)
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
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='queued' WHERE job_id=$1 AND status='waiting'`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:   "job.status_changed",
			NamespaceID: meta.NamespaceID,
			DAGID:       meta.DAGID,
			RunID:       meta.RunID,
			JobID:       meta.JobID,
			NodeKey:     meta.NodeKey,
			OldStatus:   string(meta.Status),
			NewStatus:   string(repository.JobStatusQueued),
		})
	}
	return nil
}
func (s *Store) MarkDispatching(ctx context.Context, id int64, workerID string, leaseFor time.Duration) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='dispatching', lease_owner=$2, lease_until=now()+$3::interval WHERE job_id=$1 AND status='queued'`, id, workerID, leaseFor.String())
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:   "job.status_changed",
			NamespaceID: meta.NamespaceID,
			DAGID:       meta.DAGID,
			RunID:       meta.RunID,
			JobID:       meta.JobID,
			NodeKey:     meta.NodeKey,
			OldStatus:   string(meta.Status),
			NewStatus:   string(repository.JobStatusDispatching),
		})
	}
	return nil
}
func (s *Store) RecordDispatchAccepted(ctx context.Context, id int64, externalExecutionID string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='dispatched', dispatched_at=now(), external_execution_id=NULLIF($2,''), reason_code=NULL, reason_detail=NULL, last_error=NULL, lease_owner=NULL, lease_until=NULL, dispatch_attempts=dispatch_attempts+1 WHERE job_id=$1`, id, externalExecutionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:   "job.status_changed",
			NamespaceID: meta.NamespaceID,
			DAGID:       meta.DAGID,
			RunID:       meta.RunID,
			JobID:       meta.JobID,
			NodeKey:     meta.NodeKey,
			OldStatus:   string(meta.Status),
			NewStatus:   string(repository.JobStatusDispatched),
		})
	}
	return nil
}
func (s *Store) RecordDispatchRetry(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='queued', reason_code=$2, reason_detail=$3, last_error=$3, lease_owner=NULL, lease_until=NULL, dispatch_attempts=dispatch_attempts+1 WHERE job_id=$1`, id, reasonCode, reasonDetail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:    "job.status_changed",
			NamespaceID:  meta.NamespaceID,
			DAGID:        meta.DAGID,
			RunID:        meta.RunID,
			JobID:        meta.JobID,
			NodeKey:      meta.NodeKey,
			OldStatus:    string(meta.Status),
			NewStatus:    string(repository.JobStatusQueued),
			ReasonCode:   reasonCode,
			ReasonDetail: reasonDetail,
		})
	}
	return nil
}
func (s *Store) RecordDispatchFailed(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='failed', finished_at=now(), reason_code=$2, reason_detail=$3, last_error=$3, lease_owner=NULL, lease_until=NULL, dispatch_attempts=dispatch_attempts+1 WHERE job_id=$1`, id, reasonCode, reasonDetail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:    "job.status_changed",
			NamespaceID:  meta.NamespaceID,
			DAGID:        meta.DAGID,
			RunID:        meta.RunID,
			JobID:        meta.JobID,
			NodeKey:      meta.NodeKey,
			OldStatus:    string(meta.Status),
			NewStatus:    string(repository.JobStatusFailed),
			ReasonCode:   reasonCode,
			ReasonDetail: reasonDetail,
		})
	}
	return nil
}
func (s *Store) RecordStarted(ctx context.Context, id int64, externalExecutionID string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='running', started_at=COALESCE(started_at, now()), last_heartbeat_at=now(), external_execution_id=COALESCE(NULLIF($2,''), external_execution_id), reason_code=NULL, reason_detail=NULL WHERE job_id=$1 AND status IN ('dispatched','running')`, id, externalExecutionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 && meta.Status != repository.JobStatusRunning {
		s.publishEvent(ctx, events.Event{
			EventType:   "job.status_changed",
			NamespaceID: meta.NamespaceID,
			DAGID:       meta.DAGID,
			RunID:       meta.RunID,
			JobID:       meta.JobID,
			NodeKey:     meta.NodeKey,
			OldStatus:   string(meta.Status),
			NewStatus:   string(repository.JobStatusRunning),
		})
	}
	return nil
}
func (s *Store) RecordHeartbeat(ctx context.Context, id int64, heartbeatAt time.Time, detail string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `
	UPDATE jobs 
	SET 
		status=CASE WHEN status='dispatched' THEN 'running' ELSE status END, 
		started_at=COALESCE(started_at, CASE WHEN status='dispatched' THEN now() ELSE started_at END), 
		last_heartbeat_at=$2, 
		reason_detail=CASE WHEN NULLIF($3,'') IS NULL THEN reason_detail ELSE $3 END 
	WHERE job_id=$1 AND status IN ('dispatched','running')`, id, heartbeatAt, detail)

	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:    "job.heartbeat",
			NamespaceID:  meta.NamespaceID,
			DAGID:        meta.DAGID,
			RunID:        meta.RunID,
			JobID:        meta.JobID,
			NodeKey:      meta.NodeKey,
			OldStatus:    string(meta.Status),
			NewStatus:    string(meta.Status),
			ReasonDetail: detail,
		})
	}
	return nil
}
func (s *Store) RecordCompletion(ctx context.Context, id int64, success bool, reasonCode, reasonDetail string) error {
	status := repository.JobStatusSucceeded
	if !success {
		status = repository.JobStatusFailed
	}

	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}

	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status=$2::job_status, finished_at=now(), reason_code=NULLIF($3,''), reason_detail=NULLIF($4,''), last_error=NULLIF($4,''), lease_owner=NULL, lease_until=NULL WHERE job_id=$1 AND status IN ('dispatched','running')`, id, status, reasonCode, reasonDetail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return nil
	}
	if success {
		_, err = s.dal.DB.Exec(ctx, `
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
	}
	s.publishEvent(ctx, events.Event{
		EventType:    "job.status_changed",
		NamespaceID:  meta.NamespaceID,
		DAGID:        meta.DAGID,
		RunID:        meta.RunID,
		JobID:        meta.JobID,
		NodeKey:      meta.NodeKey,
		OldStatus:    string(meta.Status),
		NewStatus:    string(status),
		ReasonCode:   reasonCode,
		ReasonDetail: reasonDetail,
	})
	return nil
}
func (s *Store) MarkLost(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='lost', finished_at=now(), reason_code=$2, reason_detail=$3, last_error=$3, lease_owner=NULL, lease_until=NULL WHERE job_id=$1 AND status IN ('dispatched','running','dispatching')`, id, reasonCode, reasonDetail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:    "job.status_changed",
			NamespaceID:  meta.NamespaceID,
			DAGID:        meta.DAGID,
			RunID:        meta.RunID,
			JobID:        meta.JobID,
			NodeKey:      meta.NodeKey,
			OldStatus:    string(meta.Status),
			NewStatus:    string(repository.JobStatusLost),
			ReasonCode:   reasonCode,
			ReasonDetail: reasonDetail,
		})
	}
	return nil
}
func (s *Store) MarkMissed(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='missed', finished_at=now(), reason_code=$2, reason_detail=$3, last_error=$3, lease_owner=NULL, lease_until=NULL WHERE job_id=$1`, id, reasonCode, reasonDetail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:    "job.status_changed",
			NamespaceID:  meta.NamespaceID,
			DAGID:        meta.DAGID,
			RunID:        meta.RunID,
			JobID:        meta.JobID,
			NodeKey:      meta.NodeKey,
			OldStatus:    string(meta.Status),
			NewStatus:    string(repository.JobStatusMissed),
			ReasonCode:   reasonCode,
			ReasonDetail: reasonDetail,
		})
	}
	return nil
}
func (s *Store) MarkBlocked(ctx context.Context, id int64, reasonCode, reasonDetail string) error {
	meta, err := s.loadJobEventMeta(ctx, s.dal.DB, id)
	if err != nil {
		return err
	}
	tag, err := s.dal.DB.Exec(ctx, `UPDATE jobs SET status='blocked', finished_at=now(), reason_code=$2, reason_detail=$3, last_error=$3, lease_owner=NULL, lease_until=NULL WHERE job_id=$1 AND status='waiting'`, id, reasonCode, reasonDetail)
	if err != nil {
		return err
	}
	if tag.RowsAffected() > 0 {
		s.publishEvent(ctx, events.Event{
			EventType:    "job.status_changed",
			NamespaceID:  meta.NamespaceID,
			DAGID:        meta.DAGID,
			RunID:        meta.RunID,
			JobID:        meta.JobID,
			NodeKey:      meta.NodeKey,
			OldStatus:    string(meta.Status),
			NewStatus:    string(repository.JobStatusBlocked),
			ReasonCode:   reasonCode,
			ReasonDetail: reasonDetail,
		})
	}
	return nil
}
func (s *Store) FindWaitingBlockedByFailedDependency(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	rows, err := s.dal.DB.Query(ctx, `
SELECT DISTINCT j.job_id, j.run_id, j.status, j.priority, j.due_at, j.node_key, j.job_definition_id::text
FROM jobs j
JOIN job_dependencies d ON d.child_job_id=j.job_id
JOIN jobs p ON p.job_id=d.parent_job_id
WHERE j.status='waiting' AND j.due_at <= $1 AND p.status IN ('failed','lost','missed','blocked','cancelled','skipped')
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

func (s *Store) FindStaleDispatching(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	rows, err := s.dal.DB.Query(ctx, `SELECT job_id, run_id, status, priority, due_at, node_key, job_definition_id::text, dispatched_at, started_at, last_heartbeat_at, finished_at, external_execution_id, reason_code, reason_detail FROM jobs WHERE status='dispatching' AND lease_until IS NOT NULL AND lease_until <= $1 ORDER BY lease_until LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*repository.Job
	for rows.Next() {
		var j repository.Job
		var dispatched, started, heartbeat, finished stdsql.NullTime
		var externalID, reasonCode, reasonDetail stdsql.NullString
		if err := rows.Scan(&j.ID, &j.RunID, &j.Status, &j.Priority, &j.DueAt, &j.NodeKey, &j.DefID, &dispatched, &started, &heartbeat, &finished, &externalID, &reasonCode, &reasonDetail); err != nil {
			return nil, err
		}
		j.DispatchedAt = timePtr(dispatched)
		j.StartedAt = timePtr(started)
		j.LastHeartbeatAt = timePtr(heartbeat)
		j.FinishedAt = timePtr(finished)
		j.ExternalExecutionID = nullStringPtr(externalID)
		j.ReasonCode = nullStringPtr(reasonCode)
		j.ReasonDetail = nullStringPtr(reasonDetail)
		out = append(out, &j)
	}
	return out, rows.Err()
}

func (s *Store) FindStaleDispatched(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	rows, err := s.dal.DB.Query(ctx, `SELECT job_id, run_id, status, priority, due_at, node_key, job_definition_id::text, dispatched_at, started_at, last_heartbeat_at, finished_at, external_execution_id, reason_code, reason_detail FROM jobs WHERE status='dispatched' AND dispatched_at IS NOT NULL AND dispatched_at <= $1 ORDER BY dispatched_at LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*repository.Job
	for rows.Next() {
		var j repository.Job
		var dispatched, started, heartbeat, finished stdsql.NullTime
		var externalID, reasonCode, reasonDetail stdsql.NullString
		if err := rows.Scan(&j.ID, &j.RunID, &j.Status, &j.Priority, &j.DueAt, &j.NodeKey, &j.DefID, &dispatched, &started, &heartbeat, &finished, &externalID, &reasonCode, &reasonDetail); err != nil {
			return nil, err
		}
		j.DispatchedAt = timePtr(dispatched)
		j.StartedAt = timePtr(started)
		j.LastHeartbeatAt = timePtr(heartbeat)
		j.FinishedAt = timePtr(finished)
		j.ExternalExecutionID = nullStringPtr(externalID)
		j.ReasonCode = nullStringPtr(reasonCode)
		j.ReasonDetail = nullStringPtr(reasonDetail)
		out = append(out, &j)
	}
	return out, rows.Err()
}

func (s *Store) FindStaleRunning(ctx context.Context, before time.Time, limit int) ([]*repository.Job, error) {
	rows, err := s.dal.DB.Query(ctx, `SELECT job_id, run_id, status, priority, due_at, node_key, job_definition_id::text, dispatched_at, started_at, last_heartbeat_at, finished_at, external_execution_id, reason_code, reason_detail FROM jobs WHERE status='running' AND COALESCE(last_heartbeat_at, started_at) IS NOT NULL AND COALESCE(last_heartbeat_at, started_at) <= $1 ORDER BY COALESCE(last_heartbeat_at, started_at) LIMIT $2`, before, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*repository.Job
	for rows.Next() {
		var j repository.Job
		var dispatched, started, heartbeat, finished stdsql.NullTime
		var externalID, reasonCode, reasonDetail stdsql.NullString
		if err := rows.Scan(&j.ID, &j.RunID, &j.Status, &j.Priority, &j.DueAt, &j.NodeKey, &j.DefID, &dispatched, &started, &heartbeat, &finished, &externalID, &reasonCode, &reasonDetail); err != nil {
			return nil, err
		}
		j.DispatchedAt = timePtr(dispatched)
		j.StartedAt = timePtr(started)
		j.LastHeartbeatAt = timePtr(heartbeat)
		j.FinishedAt = timePtr(finished)
		j.ExternalExecutionID = nullStringPtr(externalID)
		j.ReasonCode = nullStringPtr(reasonCode)
		j.ReasonDetail = nullStringPtr(reasonDetail)
		out = append(out, &j)
	}
	return out, rows.Err()
}

func (s *Store) ListProblemJobs(ctx context.Context, namespaceID string, dagID *string, statuses []repository.JobStatus, limit int) ([]repository.ProblemJob, error) {
	if limit <= 0 {
		limit = 100
	}
	if len(statuses) == 0 {
		statuses = []repository.JobStatus{repository.JobStatusFailed, repository.JobStatusLost, repository.JobStatusMissed, repository.JobStatusBlocked, repository.JobStatusCancelled, repository.JobStatusSkipped}
	}
	args := []any{namespaceID, statuses, limit}
	query := `
SELECT j.job_id, j.run_id, d.namespace_id::text, dr.dag_id::text, d.name, j.node_key, j.display_name, j.status, j.dispatch_attempts,
       j.reason_code, j.reason_detail, j.last_error, j.started_at, j.finished_at, COALESCE(f.ready, FALSE)
FROM jobs j
JOIN dag_runs dr ON dr.run_id=j.run_id
JOIN dags d ON d.dag_id=dr.dag_id
LEFT JOIN job_frontier f ON f.job_id=j.job_id
WHERE d.namespace_id=$1::uuid
  AND j.status = ANY($2::job_status[])`
	if dagID != nil && *dagID != "" {
		query += ` AND dr.dag_id=$4::uuid`
		args = []any{namespaceID, statuses, limit, *dagID}
	}
	query += `
ORDER BY COALESCE(j.finished_at, j.started_at, j.due_at) DESC, j.job_id DESC
LIMIT $3`
	rows, err := s.dal.DB.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.ProblemJob
	for rows.Next() {
		var pj repository.ProblemJob
		var reasonCode, reasonDetail, lastErr stdsql.NullString
		var started, finished stdsql.NullTime
		if err := rows.Scan(&pj.JobID, &pj.RunID, &pj.NamespaceID, &pj.DAGID, &pj.DAGName, &pj.NodeKey, &pj.DisplayName, &pj.Status, &pj.DispatchAttempts, &reasonCode, &reasonDetail, &lastErr, &started, &finished, &pj.IsReady); err != nil {
			return nil, err
		}
		pj.ReasonCode = nullStringPtr(reasonCode)
		pj.ReasonDetail = nullStringPtr(reasonDetail)
		pj.LastError = nullStringPtr(lastErr)
		pj.StartedAt = timePtr(started)
		pj.FinishedAt = timePtr(finished)
		pj.IsRestartable = pj.Status == repository.JobStatusFailed || pj.Status == repository.JobStatusLost || pj.Status == repository.JobStatusMissed || pj.Status == repository.JobStatusBlocked || pj.Status == repository.JobStatusCancelled || pj.Status == repository.JobStatusSkipped
		out = append(out, pj)
	}
	return out, rows.Err()
}

func (s *Store) RestartJob(ctx context.Context, namespaceID string, jobID int64, opts repository.RestartJobOptions) (*repository.RestartJobResult, error) {
	tx, err := s.dal.DB.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rootMeta, err := s.loadJobEventMeta(ctx, tx, jobID)
	if err != nil {
		return nil, err
	}

	var runID int64
	var status repository.JobStatus
	if err := tx.QueryRow(ctx, `
SELECT j.run_id, j.status
FROM jobs j
JOIN dag_runs dr ON dr.run_id=j.run_id
JOIN dags d ON d.dag_id=dr.dag_id
WHERE j.job_id=$1 AND d.namespace_id=$2::uuid`, jobID, namespaceID).Scan(&runID, &status); err != nil {
		return nil, err
	}
	switch status {
	case repository.JobStatusFailed, repository.JobStatusLost, repository.JobStatusMissed, repository.JobStatusBlocked, repository.JobStatusCancelled, repository.JobStatusSkipped:
	default:
		return nil, fmt.Errorf("job %d with status %s is not restartable", jobID, status)
	}
	rows, err := tx.Query(ctx, `
WITH RECURSIVE affected AS (
  SELECT $1::bigint AS job_id
  UNION ALL
  SELECT d.child_job_id
  FROM job_dependencies d
  JOIN affected a ON a.job_id=d.parent_job_id
  WHERE $2
)
SELECT DISTINCT job_id FROM affected ORDER BY job_id`, jobID, opts.Cascade)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) == 0 {
		return nil, fmt.Errorf("job %d not found", jobID)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM job_queue WHERE job_id = ANY($1)`, ids); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
UPDATE jobs
SET status='waiting',
    lease_owner=NULL,
    lease_until=NULL,
    dispatched_at=NULL,
    started_at=NULL,
    last_heartbeat_at=NULL,
    finished_at=NULL,
    external_execution_id=NULL,
    reason_code=NULL,
    reason_detail=NULL,
    last_error=NULL
WHERE job_id = ANY($1)`, ids); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
UPDATE job_frontier f
SET ready = NOT EXISTS (
    SELECT 1
    FROM job_dependencies d
    JOIN jobs p ON p.job_id=d.parent_job_id
    WHERE d.child_job_id = f.job_id
      AND p.status <> 'succeeded'
)
WHERE f.job_id = ANY($1)`, ids); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE dag_runs SET finished_at=NULL WHERE run_id=$1`, runID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	if err := s.RefreshStatus(ctx, runID); err != nil {
		return nil, err
	}
	cascade := opts.Cascade
	s.publishEvent(ctx, events.Event{
		EventType:   "job.restarted",
		NamespaceID: rootMeta.NamespaceID,
		DAGID:       rootMeta.DAGID,
		RunID:       rootMeta.RunID,
		JobID:       rootMeta.JobID,
		NodeKey:     rootMeta.NodeKey,
		OldStatus:   string(rootMeta.Status),
		NewStatus:   string(repository.JobStatusWaiting),
		Cascade:     &cascade,
		ResetJobIDs: ids,
	})
	return &repository.RestartJobResult{JobID: jobID, RunID: runID, Cascade: opts.Cascade, ResetJobIDs: ids, RestartedAt: time.Now().UTC()}, nil
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
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status='dispatching', lease_owner=$2, lease_until=now()+$3::interval WHERE job_id=$1`, jobID, workerID, vt.String()); err != nil {
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
