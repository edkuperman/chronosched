package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/edkuperman/chronosched/internal/repository"
)

type fakeDefs struct {
	scheduledUsages  []repository.ScheduledUsage
	scheduledParents []repository.ScheduledParent
	cronStatus       map[string]*repository.CronFireStatus
	nextRun          map[string]time.Time
	setNextCalls     []struct {
		nodeID string
		at     time.Time
	}
}

func (f *fakeDefs) ListByNamespace(context.Context, string) ([]repository.JobDefinition, error) {
	return nil, nil
}
func (f *fakeDefs) Create(context.Context, repository.JobDefinition) (*repository.JobDefinition, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeDefs) Get(context.Context, string) (*repository.JobDefinition, error) { return nil, nil }
func (f *fakeDefs) Update(context.Context, repository.JobDefinition) (*repository.JobDefinition, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeDefs) Delete(context.Context, string) error            { return nil }
func (f *fakeDefs) SetEnabled(context.Context, string, bool) error  { return nil }
func (f *fakeDefs) SetPaused(context.Context, string, bool) error   { return nil }
func (f *fakeDefs) ApplyFailurePolicy(context.Context, int64) error { return nil }
func (f *fakeDefs) ListUsages(context.Context, string) ([]repository.DefinitionUsage, error) {
	return nil, nil
}
func (f *fakeDefs) ListScheduledUsages(context.Context) ([]repository.ScheduledUsage, error) {
	return f.scheduledUsages, nil
}
func (f *fakeDefs) ListScheduledParents(context.Context, string, string) ([]repository.ScheduledParent, error) {
	return f.scheduledParents, nil
}
func (f *fakeDefs) GetCronFireStatus(_ context.Context, nodeID string, scheduledAt time.Time) (*repository.CronFireStatus, error) {
	if st, ok := f.cronStatus[nodeID+"|"+scheduledAt.UTC().Format(time.RFC3339)]; ok {
		return st, nil
	}
	return &repository.CronFireStatus{}, nil
}
func (f *fakeDefs) GetCronNextRun(_ context.Context, nodeID string) (*time.Time, error) {
	if t, ok := f.nextRun[nodeID]; ok {
		copy := t
		return &copy, nil
	}
	return nil, nil
}
func (f *fakeDefs) SetCronNextRun(_ context.Context, nodeID string, nextRunAt time.Time) error {
	if f.nextRun == nil {
		f.nextRun = map[string]time.Time{}
	}
	f.nextRun[nodeID] = nextRunAt.UTC()
	f.setNextCalls = append(f.setNextCalls, struct {
		nodeID string
		at     time.Time
	}{nodeID, nextRunAt.UTC()})
	return nil
}

type fakeRuns struct {
	meta    *repository.RunSchedulingMeta
	created []struct {
		dagVersionID, nodeID, definitionID, triggerType string
		scheduledAt                                     time.Time
	}
}

func (f *fakeRuns) CreateManualRun(context.Context, string, *string, time.Time) (*repository.DAGRun, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeRuns) CreateScheduledRun(_ context.Context, dagVersionID, triggerNodeID, definitionID, triggerType string, scheduledAt time.Time) (*repository.DAGRun, error) {
	f.created = append(f.created, struct {
		dagVersionID, nodeID, definitionID, triggerType string
		scheduledAt                                     time.Time
	}{dagVersionID, triggerNodeID, definitionID, triggerType, scheduledAt.UTC()})
	return &repository.DAGRun{ID: int64(len(f.created))}, nil
}
func (f *fakeRuns) ListByDAG(context.Context, string) ([]repository.DAGRun, error) { return nil, nil }
func (f *fakeRuns) Get(context.Context, int64) (*repository.DAGRun, error)         { return nil, nil }
func (f *fakeRuns) GetSchedulingMeta(context.Context, int64) (*repository.RunSchedulingMeta, error) {
	return f.meta, nil
}
func (f *fakeRuns) ListJobs(context.Context, int64) ([]repository.RunJob, error)  { return nil, nil }
func (f *fakeRuns) GetGraph(context.Context, int64) (*repository.RunGraph, error) { return nil, nil }
func (f *fakeRuns) RefreshStatus(context.Context, int64) error                    { return nil }

type fakeJobs struct {
	jobs   []*repository.Job
	queued []int64
}

func (f *fakeJobs) FindDueReadyWaiting(context.Context, time.Time, int) ([]*repository.Job, error) {
	return f.jobs, nil
}
func (f *fakeJobs) MarkQueued(_ context.Context, id int64) error {
	f.queued = append(f.queued, id)
	return nil
}
func (f *fakeJobs) MarkRunning(context.Context, int64) error        { return nil }
func (f *fakeJobs) MarkSucceeded(context.Context, int64) error      { return nil }
func (f *fakeJobs) MarkFailed(context.Context, int64, string) error { return nil }
func (f *fakeJobs) MarkMissed(context.Context, int64, string) error { return nil }
func (f *fakeJobs) GetReadiness(context.Context, int64) (*repository.JobReadiness, error) {
	return nil, nil
}
func (f *fakeJobs) GetRunID(context.Context, int64) (int64, error) { return 0, nil }
func (f *fakeJobs) GetExecution(context.Context, int64) (*repository.JobExecution, error) {
	return nil, nil
}

type fakeQueue struct{ enqueued []int64 }

func (f *fakeQueue) Enqueue(_ context.Context, jobID int64, _ time.Time, _ int) error {
	f.enqueued = append(f.enqueued, jobID)
	return nil
}
func (f *fakeQueue) Dequeue(context.Context, string, int, time.Duration) ([]repository.QueueItem, error) {
	return nil, nil
}
func (f *fakeQueue) Ack(context.Context, int64, string) error                 { return nil }
func (f *fakeQueue) Fail(context.Context, int64, string, time.Duration) error { return nil }

func TestIntervalPrevOrSame(t *testing.T) {
	start := time.Date(2026, 3, 6, 12, 0, 7, 0, time.UTC)
	got, ok := intervalPrevOrSame(start, 10, time.Date(2026, 3, 6, 12, 0, 28, 0, time.UTC))
	if !ok {
		t.Fatal("expected ok")
	}
	want := time.Date(2026, 3, 6, 12, 0, 27, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
}

func TestInitialAndNextScheduledTime(t *testing.T) {
	interval := 10
	start := time.Date(2026, 3, 6, 12, 0, 7, 0, time.UTC)
	intervalUsage := repository.ScheduledUsage{ScheduleType: "interval", IntervalSeconds: &interval, StartAt: &start}
	got, err := initialScheduledTime(intervalUsage)
	if err != nil || !got.Equal(start) {
		t.Fatalf("initial interval time = %v, %v", got, err)
	}
	next, err := nextScheduledTime(intervalUsage, start)
	if err != nil || !next.Equal(start.Add(10*time.Second)) {
		t.Fatalf("next interval time = %v, %v", next, err)
	}

	created := time.Date(2026, 3, 6, 12, 0, 4, 0, time.UTC)
	cronUsage := repository.ScheduledUsage{ScheduleType: "cron", CronSpec: "*/5 * * * * *", VersionCreatedAt: created}
	cronFirst, err := initialScheduledTime(cronUsage)
	if err != nil {
		t.Fatalf("cron initial error: %v", err)
	}
	if !cronFirst.Equal(time.Date(2026, 3, 6, 12, 0, 5, 0, time.UTC)) {
		t.Fatalf("unexpected first cron fire: %v", cronFirst)
	}
}

func TestScheduledParentsSatisfied_CronAndInterval(t *testing.T) {
	interval := 10
	start := time.Date(2026, 3, 6, 12, 0, 7, 0, time.UTC)
	defs := &fakeDefs{scheduledParents: []repository.ScheduledParent{
		{NodeID: "cron-parent", ScheduleType: "cron", CronSpec: "*/5 * * * * *", DefinitionEnabled: true},
		{NodeID: "interval-parent", ScheduleType: "interval", IntervalSeconds: &interval, StartAt: &start, DefinitionEnabled: true},
	}, cronStatus: map[string]*repository.CronFireStatus{
		"cron-parent|2026-03-06T12:00:25Z":     {Exists: true, Status: repository.RunStatusSucceeded},
		"interval-parent|2026-03-06T12:00:27Z": {Exists: true, Status: repository.RunStatusSucceeded},
	}}
	runs := &fakeRuns{meta: &repository.RunSchedulingMeta{TriggerType: "cron", DAGVersionID: "dv1", TriggerNodeID: "child", ScheduledAt: time.Date(2026, 3, 6, 12, 0, 27, 0, time.UTC)}}
	repos := &repository.Repos{Definitions: defs, Runs: runs}
	ok, err := scheduledParentsSatisfied(context.Background(), repos, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected parents to be satisfied")
	}
}

func TestMaterializeScheduledRuns_CreatesCronAndIntervalRuns(t *testing.T) {
	interval := 10
	start := time.Now().UTC().Add(-2 * time.Second).Truncate(time.Second)
	created := start.Add(-3 * time.Second)
	defs := &fakeDefs{scheduledUsages: []repository.ScheduledUsage{
		{NodeID: "cron-node", NodeKey: "cron", DefinitionID: "d1", DAGVersionID: "dv1", ScheduleType: "cron", CronSpec: "*/1 * * * * *", VersionCreatedAt: created},
		{NodeID: "int-node", NodeKey: "interval", DefinitionID: "d2", DAGVersionID: "dv1", ScheduleType: "interval", IntervalSeconds: &interval, StartAt: &start, VersionCreatedAt: created},
	}, nextRun: map[string]time.Time{
		"cron-node": time.Now().UTC().Add(-1 * time.Second),
		"int-node":  start,
	}}
	runs := &fakeRuns{}
	s := &Scheduler{repos: &repository.Repos{Definitions: defs, Runs: runs}, batchSize: 128}
	if err := s.materializeScheduledRuns(context.Background()); err != nil {
		t.Fatalf("materializeScheduledRuns error: %v", err)
	}
	if len(runs.created) != 2 {
		t.Fatalf("expected 2 created runs, got %d", len(runs.created))
	}
	if len(defs.setNextCalls) < 2 {
		t.Fatalf("expected next run state updates, got %d", len(defs.setNextCalls))
	}
}

func TestEnqueueReadyJobs_QueuesOnlyWhenScheduledParentsSatisfied(t *testing.T) {
	defs := &fakeDefs{scheduledParents: []repository.ScheduledParent{{NodeID: "p1", ScheduleType: "cron", CronSpec: "*/5 * * * * *", DefinitionEnabled: true}}, cronStatus: map[string]*repository.CronFireStatus{
		"p1|2026-03-06T12:00:25Z": {Exists: true, Status: repository.RunStatusSucceeded},
	}}
	runs := &fakeRuns{meta: &repository.RunSchedulingMeta{TriggerType: "cron", DAGVersionID: "dv1", TriggerNodeID: "child", ScheduledAt: time.Date(2026, 3, 6, 12, 0, 27, 0, time.UTC)}}
	jobs := &fakeJobs{jobs: []*repository.Job{{ID: 1, RunID: 10, DueAt: time.Now(), Priority: 3}}}
	queue := &fakeQueue{}
	s := &Scheduler{repos: &repository.Repos{Definitions: defs, Runs: runs, Jobs: jobs, Queue: queue}, batchSize: 128}
	if err := s.enqueueReadyJobs(context.Background()); err != nil {
		t.Fatalf("enqueueReadyJobs error: %v", err)
	}
	if len(jobs.queued) != 1 || len(queue.enqueued) != 1 {
		t.Fatalf("expected job to be queued and enqueued, got queued=%v enqueued=%v", jobs.queued, queue.enqueued)
	}
}
