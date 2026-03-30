package events

import (
    "context"
    "regexp"
    "sync"
    "testing"
    "time"
)

type recordingPublisher struct {
    mu     sync.Mutex
    events []Event
}

func (r *recordingPublisher) Publish(_ context.Context, evt Event) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.events = append(r.events, evt)
    return nil
}

func (r *recordingPublisher) snapshot() []Event {
    r.mu.Lock()
    defer r.mu.Unlock()
    out := make([]Event, len(r.events))
    copy(out, r.events)
    return out
}

func TestSetDefaultPublisherAndDefaultPublisher(t *testing.T) {
    orig := DefaultPublisher()
    defer SetDefaultPublisher(orig)

    recorder := &recordingPublisher{}
    SetDefaultPublisher(recorder)

    evt := Event{EventID: "evt-1", EventType: "job.status_changed", OccurredAt: time.Now().UTC(), JobID: 17}
    if err := DefaultPublisher().Publish(context.Background(), evt); err != nil {
        t.Fatalf("publish via default publisher: %v", err)
    }
    got := recorder.snapshot()
    if len(got) != 1 || got[0].EventID != "evt-1" || got[0].JobID != 17 {
        t.Fatalf("unexpected recorded events: %#v", got)
    }

    SetDefaultPublisher(nil)
    if err := DefaultPublisher().Publish(context.Background(), evt); err != nil {
        t.Fatalf("nil reset should fall back to noop publisher: %v", err)
    }
}

func TestNewEventID_FormatAndUniqueness(t *testing.T) {
    pattern := regexp.MustCompile(`^[0-9a-f]{16}-\d{8}T\d{6}\.\d{9}Z$|^\d{8}T\d{6}\.\d{9}Z-[0-9a-f]{16}$`)
    seen := map[string]bool{}
    for i := 0; i < 20; i++ {
        id := NewEventID()
        if !pattern.MatchString(id) {
            t.Fatalf("event id %q did not match expected format", id)
        }
        if seen[id] {
            t.Fatalf("duplicate event id generated: %q", id)
        }
        seen[id] = true
    }
}

func TestLoggerPublisherPublishReturnsNil(t *testing.T) {
    publisher := NewLoggerPublisher()
    evt := Event{EventID: "evt-2", EventType: "run.failed", OccurredAt: time.Now().UTC(), RunID: 42, NodeKey: "report"}
    if err := publisher.Publish(context.Background(), evt); err != nil {
        t.Fatalf("logger publisher returned error: %v", err)
    }
}
