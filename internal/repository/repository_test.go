package repository

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestScheduleJSONForInterval(t *testing.T) {
	secs := 10
	start := time.Date(2026, 3, 6, 12, 0, 0, 0, time.UTC)
	s := Schedule{Type: "interval", IntervalSeconds: &secs, StartAt: &start, OnFailure: "continue"}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	got := string(data)
	for _, want := range []string{`"type":"interval"`, `"interval_seconds":10`, `"start_at":"2026-03-06T12:00:00Z"`, `"on_failure":"continue"`} {
		if !strings.Contains(got, want) {
			t.Fatalf("json %q missing %q", got, want)
		}
	}
}

func TestStatusesAreStableStrings(t *testing.T) {
	if RunStatusSucceeded != "succeeded" || JobStatusQueued != "queued" || JobStatusSkipped != "skipped" {
		t.Fatal("unexpected status constant values")
	}
}
