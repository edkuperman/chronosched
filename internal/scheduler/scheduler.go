package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/edkuperman/chronosched/internal/logger"
	"github.com/edkuperman/chronosched/internal/repository"
	cronlib "github.com/robfig/cron/v3"
)

type parentCheckResult int

const (
	parentCheckBlocked parentCheckResult = iota
	parentCheckReady
	parentCheckImpossible
)

type Scheduler struct {
	repos              *repository.Repos
	cron               *cronlib.Cron
	batchSize          int
	startTimeout       time.Duration
	heartbeatTimeout   time.Duration
	dependencySweepLag time.Duration
}

func NewScheduler(repos *repository.Repos) *Scheduler {
	c := cronlib.New(cronlib.WithSeconds())
	s := &Scheduler{repos: repos, cron: c, batchSize: 128, startTimeout: 30 * time.Second, heartbeatTimeout: 60 * time.Second, dependencySweepLag: 1 * time.Second}
	_, _ = c.AddFunc("@every 1s", func() {
		if err := s.tick(context.Background()); err != nil {
			logger.Error(err, "scheduler tick error")
		}
	})
	return s
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.cron.Start()
	defer s.cron.Stop()
	if err := s.tick(ctx); err != nil && ctx.Err() == nil {
		logger.Error(err, "initial tick error")
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *Scheduler) tick(ctx context.Context) error {
	if err := s.materializeScheduledRuns(ctx); err != nil {
		return err
	}
	if err := s.enqueueReadyJobs(ctx); err != nil {
		return err
	}
	if err := s.sweepBlockedJobs(ctx); err != nil {
		return err
	}
	return s.reapLostJobs(ctx)
}

func parseCronSchedule(spec string) (cronlib.Schedule, error) {
	withSeconds := cronlib.NewParser(cronlib.Second | cronlib.Minute | cronlib.Hour | cronlib.Dom | cronlib.Month | cronlib.Dow | cronlib.Descriptor)
	if sch, err := withSeconds.Parse(spec); err == nil {
		return sch, nil
	}
	return cronlib.ParseStandard(spec)
}

func prevOrSame(schedule cronlib.Schedule, at time.Time) time.Time {
	at = at.UTC()
	for probe := at; ; probe = probe.Add(-1 * time.Second) {
		next := schedule.Next(probe.Add(-1 * time.Second))
		if !next.After(at) && next.Equal(probe) {
			return probe
		}
	}
}

func intervalPrevOrSame(startAt time.Time, intervalSeconds int, at time.Time) (time.Time, bool) {
	if intervalSeconds <= 0 {
		return time.Time{}, false
	}
	anchor := startAt.UTC()
	target := at.UTC()
	if target.Before(anchor) {
		return time.Time{}, false
	}
	interval := time.Duration(intervalSeconds) * time.Second
	steps := int64(target.Sub(anchor) / interval)
	return anchor.Add(time.Duration(steps) * interval), true
}

func initialScheduledTime(u repository.ScheduledUsage) (time.Time, error) {
	switch u.ScheduleType {
	case "", "cron":
		if u.CronSpec == "" {
			return time.Time{}, fmt.Errorf("missing cron spec")
		}
		schedule, err := parseCronSchedule(u.CronSpec)
		if err != nil {
			return time.Time{}, err
		}
		seed := u.VersionCreatedAt.UTC().Add(-1 * time.Second)
		return schedule.Next(seed), nil
	case "interval":
		if u.IntervalSeconds == nil || *u.IntervalSeconds <= 0 {
			return time.Time{}, fmt.Errorf("missing interval_seconds")
		}
		if u.StartAt == nil {
			return time.Time{}, fmt.Errorf("missing start_at")
		}
		return u.StartAt.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported schedule type %q", u.ScheduleType)
	}
}

func nextScheduledTime(u repository.ScheduledUsage, fireAt time.Time) (time.Time, error) {
	switch u.ScheduleType {
	case "", "cron":
		schedule, err := parseCronSchedule(u.CronSpec)
		if err != nil {
			return time.Time{}, err
		}
		return schedule.Next(fireAt.UTC()), nil
	case "interval":
		if u.IntervalSeconds == nil || *u.IntervalSeconds <= 0 {
			return time.Time{}, fmt.Errorf("missing interval_seconds")
		}
		return fireAt.UTC().Add(time.Duration(*u.IntervalSeconds) * time.Second), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported schedule type %q", u.ScheduleType)
	}
}

func scheduledParentsSatisfied(ctx context.Context, repos *repository.Repos, runID int64) (parentCheckResult, string, error) {
	meta, err := repos.Runs.GetSchedulingMeta(ctx, runID)
	if err != nil {
		return parentCheckBlocked, "", err
	}
	if meta.TriggerNodeID == "" {
		return parentCheckReady, "", nil
	}
	if meta.TriggerType != "cron" && meta.TriggerType != "interval" {
		return parentCheckReady, "", nil
	}
	parents, err := repos.Definitions.ListScheduledParents(ctx, meta.DAGVersionID, meta.TriggerNodeID)
	if err != nil {
		return parentCheckBlocked, "", err
	}
	for _, parent := range parents {
		if !parent.DefinitionEnabled || parent.DefinitionPaused {
			return parentCheckImpossible, fmt.Sprintf("scheduled parent %s is disabled or paused", parent.NodeKey), nil
		}
		var requiredAt time.Time
		switch parent.ScheduleType {
		case "", "cron":
			if parent.CronSpec == "" {
				return parentCheckImpossible, fmt.Sprintf("scheduled parent %s has no cron spec", parent.NodeKey), nil
			}
			ps, err := parseCronSchedule(parent.CronSpec)
			if err != nil {
				return parentCheckBlocked, "", err
			}
			requiredAt = prevOrSame(ps, meta.ScheduledAt.UTC())
		case "interval":
			if parent.IntervalSeconds == nil || *parent.IntervalSeconds <= 0 || parent.StartAt == nil {
				return parentCheckImpossible, fmt.Sprintf("scheduled parent %s has invalid interval schedule", parent.NodeKey), nil
			}
			var ok bool
			requiredAt, ok = intervalPrevOrSame(parent.StartAt.UTC(), *parent.IntervalSeconds, meta.ScheduledAt.UTC())
			if !ok {
				return parentCheckImpossible, fmt.Sprintf("scheduled parent %s has no occurrence for child time", parent.NodeKey), nil
			}
		default:
			return parentCheckImpossible, fmt.Sprintf("scheduled parent %s has unsupported schedule type %s", parent.NodeKey, parent.ScheduleType), nil
		}
		st, err := repos.Definitions.GetCronFireStatus(ctx, parent.NodeID, requiredAt)
		if err != nil {
			return parentCheckBlocked, "", err
		}
		if !st.Exists {
			return parentCheckBlocked, fmt.Sprintf("waiting for scheduled parent %s at %s", parent.NodeKey, requiredAt.Format(time.RFC3339)), nil
		}
		if st.Status == repository.RunStatusSucceeded {
			continue
		}
		if st.Status == repository.RunStatusFailed || st.Status == repository.RunStatusMissed || st.Status == repository.RunStatusCancelled {
			return parentCheckImpossible, fmt.Sprintf("scheduled parent %s at %s finished with %s", parent.NodeKey, requiredAt.Format(time.RFC3339), st.Status), nil
		}
		return parentCheckBlocked, fmt.Sprintf("waiting for scheduled parent %s at %s", parent.NodeKey, requiredAt.Format(time.RFC3339)), nil
	}
	return parentCheckReady, "", nil
}

func (s *Scheduler) materializeScheduledRuns(ctx context.Context) error {
	usages, err := s.repos.Definitions.ListScheduledUsages(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, u := range usages {
		nextRun, err := s.repos.Definitions.GetCronNextRun(ctx, u.NodeID)
		if err != nil {
			return err
		}
		if nextRun == nil {
			candidate, err := initialScheduledTime(u)
			if err != nil {
				logger.Error(err, "invalid schedule", "definitionID", u.DefinitionID, "nodeID", u.NodeID, "scheduleType", u.ScheduleType)
				continue
			}
			if err := s.repos.Definitions.SetCronNextRun(ctx, u.NodeID, candidate); err != nil {
				return err
			}
			nextRun = &candidate
		}
		if nextRun.After(now) {
			continue
		}
		fireAt := nextRun.UTC()
		if _, err := s.repos.Runs.CreateScheduledRun(ctx, u.DAGVersionID, u.NodeID, u.DefinitionID, u.ScheduleType, fireAt); err != nil {
			logger.Error(err, "failed to materialize scheduled run", "dagVersionID", u.DAGVersionID, "nodeKey", u.NodeKey, "scheduleType", u.ScheduleType)
			continue
		}
		logger.Info("scheduled run", "dagVersionID", u.DAGVersionID, "nodeKey", u.NodeKey, "scheduleType", u.ScheduleType, "scheduledAt", fireAt)
		following, err := nextScheduledTime(u, fireAt)
		if err != nil {
			return err
		}
		if err := s.repos.Definitions.SetCronNextRun(ctx, u.NodeID, following); err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) enqueueReadyJobs(ctx context.Context) error {
	now := time.Now().UTC()
	jobs, err := s.repos.Jobs.FindDueReadyWaiting(ctx, now, s.batchSize)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		decision, reason, err := scheduledParentsSatisfied(ctx, s.repos, j.RunID)
		if err != nil {
			logger.Error(err, "scheduled parent readiness check failed", "jobID", j.ID, "runID", j.RunID)
			continue
		}
		switch decision {
		case parentCheckBlocked:
			continue
		case parentCheckImpossible:
			if err := s.repos.Jobs.MarkBlocked(ctx, j.ID, "failed_dependency", reason); err == nil {
				if runID, err := s.repos.Jobs.GetRunID(ctx, j.ID); err == nil {
					_ = s.repos.Runs.RefreshStatus(ctx, runID)
				}
			}
			continue
		}
		if err := s.repos.Jobs.MarkQueued(ctx, j.ID); err != nil {
			logger.Error(err, "mark queued failed", "jobID", j.ID)
			continue
		}
		if err := s.repos.Queue.Enqueue(ctx, j.ID, j.DueAt, j.Priority); err != nil {
			logger.Error(err, "enqueue failed", "jobID", j.ID)
			continue
		}
	}
	return nil
}

func (s *Scheduler) sweepBlockedJobs(ctx context.Context) error {
	jobs, err := s.repos.Jobs.FindWaitingBlockedByFailedDependency(ctx, time.Now().UTC().Add(-s.dependencySweepLag), s.batchSize)
	if err != nil {
		return err
	}
	for _, j := range jobs {
		if err := s.repos.Jobs.MarkBlocked(ctx, j.ID, "failed_dependency", "an upstream dependency completed without success"); err != nil {
			logger.Error(err, "mark blocked failed", "jobID", j.ID)
			continue
		}
		if runID, err := s.repos.Jobs.GetRunID(ctx, j.ID); err == nil {
			_ = s.repos.Runs.RefreshStatus(ctx, runID)
		}
	}
	return nil
}

func (s *Scheduler) reapLostJobs(ctx context.Context) error {
	dispatched, err := s.repos.Jobs.FindStaleDispatched(ctx, time.Now().UTC().Add(-s.startTimeout), s.batchSize)
	if err != nil {
		return err
	}
	for _, j := range dispatched {
		if err := s.repos.Jobs.MarkLost(ctx, j.ID, "start_timeout", "job was dispatched but never reported started"); err != nil {
			logger.Error(err, "mark lost (start timeout) failed", "jobID", j.ID)
			continue
		}
		if runID, err := s.repos.Jobs.GetRunID(ctx, j.ID); err == nil {
			_ = s.repos.Runs.RefreshStatus(ctx, runID)
		}
	}
	running, err := s.repos.Jobs.FindStaleRunning(ctx, time.Now().UTC().Add(-s.heartbeatTimeout), s.batchSize)
	if err != nil {
		return err
	}
	for _, j := range running {
		if err := s.repos.Jobs.MarkLost(ctx, j.ID, "heartbeat_timeout", "job stopped sending heartbeats"); err != nil {
			logger.Error(err, "mark lost (heartbeat timeout) failed", "jobID", j.ID)
			continue
		}
		if runID, err := s.repos.Jobs.GetRunID(ctx, j.ID); err == nil {
			_ = s.repos.Runs.RefreshStatus(ctx, runID)
		}
	}
	return nil
}
