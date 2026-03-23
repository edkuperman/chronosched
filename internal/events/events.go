package events

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edkuperman/chronosched/internal/logger"
)

type Event struct {
	EventID      string    `json:"event_id"`
	EventType    string    `json:"event_type"`
	OccurredAt   time.Time `json:"occurred_at"`
	NamespaceID  string    `json:"namespace_id,omitempty"`
	DAGID        string    `json:"dag_id,omitempty"`
	RunID        int64     `json:"run_id,omitempty"`
	JobID        int64     `json:"job_id,omitempty"`
	NodeKey      string    `json:"node_key,omitempty"`
	OldStatus    string    `json:"old_status,omitempty"`
	NewStatus    string    `json:"new_status,omitempty"`
	ReasonCode   string    `json:"reason_code,omitempty"`
	ReasonDetail string    `json:"reason_detail,omitempty"`
	TriggerType  string    `json:"trigger_type,omitempty"`
	Cascade      *bool     `json:"cascade,omitempty"`
	ResetJobIDs  []int64   `json:"reset_job_ids,omitempty"`
}

type EventPublisher interface {
	Publish(ctx context.Context, evt Event) error
}

type noopPublisher struct{}

func (noopPublisher) Publish(ctx context.Context, evt Event) error { return nil }

type LoggerPublisher struct{}

func NewLoggerPublisher() EventPublisher { return LoggerPublisher{} }

func (LoggerPublisher) Publish(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	logger.Info("chronosched event", "event", string(payload))
	return nil
}

var (
	defaultPublisher EventPublisher = noopPublisher{}
	publisherMu      sync.RWMutex
	eventCounter     uint64
)

func DefaultPublisher() EventPublisher {
	publisherMu.RLock()
	defer publisherMu.RUnlock()
	return defaultPublisher
}

func SetDefaultPublisher(p EventPublisher) {
	if p == nil {
		p = noopPublisher{}
	}
	publisherMu.Lock()
	defaultPublisher = p
	publisherMu.Unlock()
}

func NewEventID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:]) + "-" + time.Now().UTC().Format("20060102T150405.000000000Z07:00")
	}
	n := atomic.AddUint64(&eventCounter, 1)
	return time.Now().UTC().Format("20060102T150405.000000000Z07:00") + "-" + hex.EncodeToString([]byte{
		byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
		byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
	})
}
