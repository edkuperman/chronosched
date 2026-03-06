package logger

import (
	"bytes"
	"errors"
	"log"
	"strings"
	"testing"
)

type sampleReceiver struct{}

func (sampleReceiver) logSomething(l *Logger) {
	l.Info("hello", "k", "v")
}

func TestFormatKeyValue(t *testing.T) {
	if got := formatKeyValue([]interface{}{"a", 1, "b", "two"}); got != "a=1 b=two" {
		t.Fatalf("unexpected kv format: %q", got)
	}
	if got := formatKeyValue([]interface{}{"a"}); got != "invalid_key_value_pairs=1" {
		t.Fatalf("unexpected odd kv response: %q", got)
	}
}

func TestLoggerWritesStructuredLine(t *testing.T) {
	var buf bytes.Buffer
	l := New(log.New(&buf, "", 0))
	sampleReceiver{}.logSomething(l)
	out := buf.String()
	for _, want := range []string{"level=INFO", "pkg=logger", "type=none", "method=TestLoggerWritesStructuredLine", `msg="hello"`, "k=v"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output %q missing %q", out, want)
		}
	}
}

func TestLoggerLevelFilteringAndError(t *testing.T) {
	var buf bytes.Buffer
	l := New(log.New(&buf, "", 0))
	l.SetLevel(WarnLevel)
	l.Info("ignored")
	if buf.Len() != 0 {
		t.Fatalf("expected no output at warn level, got %q", buf.String())
	}
	l.Error(errors.New("boom"), "failed", "job", 42)
	out := buf.String()
	for _, want := range []string{"level=ERROR", `error="boom"`, `msg="failed"`, "job=42"} {
		if !strings.Contains(out, want) {
			t.Fatalf("output %q missing %q", out, want)
		}
	}
}
