package ai

import (
    "context"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "strings"
    "testing"
    "time"

    "github.com/edkuperman/chronosched/internal/repository"
)

func TestHeuristicSummary_FocusesHighestPriorityProblemAndCapturesImpact(t *testing.T) {
    reasonCode := "dependency_failed"
    failure := "intentional demo failure"
    now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
    started := now.Add(-2 * time.Minute)
    finished := now.Add(-1 * time.Minute)

    graph := &repository.RunGraph{
        Run: repository.DAGRun{ID: 42, DAGID: "dag-1", DAGVersionID: "dv-1", Status: repository.RunStatusFailed},
        Nodes: []repository.RunJob{
            {JobID: 10, RunID: 42, NodeKey: "hello_5s", DisplayName: "Hello 5s", Status: repository.JobStatusSucceeded, StartedAt: &started, FinishedAt: &finished},
            {JobID: 20, RunID: 42, NodeKey: "hello_10s", DisplayName: "Hello 10s", Status: repository.JobStatusFailed, ReasonCode: &reasonCode, LastError: &failure},
            {JobID: 30, RunID: 42, NodeKey: "reportJob", DisplayName: "Report", Status: repository.JobStatusBlocked},
            {JobID: 40, RunID: 42, NodeKey: "archive", DisplayName: "Archive", Status: repository.JobStatusWaiting},
        },
        Edges: []repository.RunGraphEdge{{FromJobID: 10, ToJobID: 20}, {FromJobID: 10, ToJobID: 30}, {FromJobID: 20, ToJobID: 30}, {FromJobID: 30, ToJobID: 40}},
    }

    summary := heuristicSummary(graph)
    if summary == nil {
        t.Fatal("expected heuristic summary")
    }
    if summary.FailedNode != "hello_10s" {
        t.Fatalf("expected focus node hello_10s, got %q", summary.FailedNode)
    }
    if summary.Context.RootCauseNodeKey != "hello_10s" {
        t.Fatalf("expected root cause hello_10s, got %q", summary.Context.RootCauseNodeKey)
    }
    if !strings.Contains(summary.Cause, failure) {
        t.Fatalf("expected cause to include recorded failure, got %q", summary.Cause)
    }
    if len(summary.Context.UpstreamCompleted) != 1 || summary.Context.UpstreamCompleted[0] != "hello_5s" {
        t.Fatalf("unexpected upstream completed list: %#v", summary.Context.UpstreamCompleted)
    }
    if len(summary.Context.DownstreamImpacted) != 2 || summary.Context.DownstreamImpacted[0] != "archive" || summary.Context.DownstreamImpacted[1] != "reportJob" {
        t.Fatalf("unexpected downstream impacted list: %#v", summary.Context.DownstreamImpacted)
    }
    if summary.Retry != "requires_manual_review" {
        t.Fatalf("expected manual review retry guidance, got %q", summary.Retry)
    }
}

func TestHeuristicCause_BlockedNodeUsesUpstreamRootCause(t *testing.T) {
    reasonCode := "dependency_failed"
    failure := "intentional demo failure"
    blocked := &repository.RunJob{JobID: 30, NodeKey: "reportJob", Status: repository.JobStatusBlocked}
    rootCause := &repository.RunJob{JobID: 20, NodeKey: "hello_10s", Status: repository.JobStatusFailed, ReasonCode: &reasonCode, LastError: &failure}

    got := heuristicCause(blocked, rootCause)
    if !strings.Contains(got, "reportJob") || !strings.Contains(got, "hello_10s") || !strings.Contains(got, "blocked") {
        t.Fatalf("expected blocked cause to mention focus and upstream root cause, got %q", got)
    }
    if heuristicType(blocked, rootCause) != "dependency" {
        t.Fatalf("expected blocked node with root cause to normalize as dependency")
    }
}

func TestSummarizeRun_OpenAIResponseOverridesHeuristic(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        if r.Method != http.MethodPost {
            t.Fatalf("expected POST, got %s", r.Method)
        }
        if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
            t.Fatalf("unexpected path: %s", r.URL.Path)
        }
        var req map[string]any
        if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
            t.Fatalf("decode request: %v", err)
        }
        if req["model"] != "test-model" {
            t.Fatalf("unexpected model: %#v", req["model"])
        }
        w.Header().Set("Content-Type", "application/json")
        content := "```json\n{\"type\":\"timeout\",\"cause\":\"worker heartbeat expired\",\"impact\":\"downstream report did not run\",\"next_steps\":[\"check worker logs\",\"check worker logs\",\"inspect heartbeat timeout\"],\"confidence\":1.2,\"retry\":\"safe\"}\n```"
        resp := map[string]any{"choices": []map[string]any{{"message": map[string]any{"content": content}}}}
        if err := json.NewEncoder(w).Encode(resp); err != nil {
            t.Fatalf("encode response: %v", err)
        }
    }))
    defer server.Close()

    s := &Summarizer{client: &OpenAIClient{APIKey: "key", Model: "test-model", BaseURL: server.URL, HTTPClient: server.Client()}}
    summary, err := s.SummarizeRun(context.Background(), testRunGraph())
    if err != nil {
        t.Fatalf("SummarizeRun error: %v", err)
    }
    if summary.Source != "openai" {
        t.Fatalf("expected openai source, got %q", summary.Source)
    }
    if summary.Model != "test-model" {
        t.Fatalf("expected model to be set, got %q", summary.Model)
    }
    if summary.Type != "timeout" || summary.Cause != "worker heartbeat expired" || summary.Impact != "downstream report did not run" {
        t.Fatalf("unexpected AI summary payload: %#v", summary)
    }
    if summary.Retry != "safe" {
        t.Fatalf("expected normalized retry=safe, got %q", summary.Retry)
    }
    if summary.Confidence != 1 {
        t.Fatalf("expected confidence to clamp to 1, got %v", summary.Confidence)
    }
    if len(summary.NextSteps) != 2 || summary.NextSteps[0] != "check worker logs" || summary.NextSteps[1] != "inspect heartbeat timeout" {
        t.Fatalf("unexpected next steps: %#v", summary.NextSteps)
    }
}

func TestSummarizeRun_FallsBackToHeuristicWhenOpenAIFails(t *testing.T) {
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusBadGateway)
        _, _ = w.Write([]byte(`{"error":{"message":"upstream unavailable"}}`))
    }))
    defer server.Close()

    s := &Summarizer{client: &OpenAIClient{APIKey: "key", Model: "test-model", BaseURL: server.URL, HTTPClient: server.Client()}}
    summary, err := s.SummarizeRun(context.Background(), testRunGraph())
    if err != nil {
        t.Fatalf("SummarizeRun error: %v", err)
    }
    if summary.Source != "heuristic" {
        t.Fatalf("expected heuristic fallback, got %q", summary.Source)
    }
    if summary.Model != "test-model" {
        t.Fatalf("expected model to be preserved, got %q", summary.Model)
    }
    if !strings.Contains(summary.AIError, "upstream unavailable") {
        t.Fatalf("expected AI error to contain service message, got %q", summary.AIError)
    }
    if summary.Cause == "" || summary.GeneratedAt.IsZero() {
        t.Fatalf("expected heuristic fields to remain populated: %#v", summary)
    }
}

func TestNormalizeNextSteps_HandlesMixedPayloads(t *testing.T) {
    steps := normalizeNextSteps([]interface{}{"check logs", map[string]interface{}{"step": "rerun workflow"}, 12, "check logs"})
    if len(steps) != 3 {
        t.Fatalf("expected deduped steps, got %#v", steps)
    }
    if steps[0] != "check logs" || steps[1] != "rerun workflow" || steps[2] != "12" {
        t.Fatalf("unexpected normalized steps: %#v", steps)
    }
}

func testRunGraph() *repository.RunGraph {
    reasonCode := "heartbeat_timeout"
    failure := "worker heartbeat expired"
    started := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
    return &repository.RunGraph{
        Run: repository.DAGRun{ID: 7, DAGID: "dag-1", DAGVersionID: "dv-1", Status: repository.RunStatusFailed},
        Nodes: []repository.RunJob{
            {JobID: 1, RunID: 7, NodeKey: "extract", DisplayName: "Extract", Status: repository.JobStatusSucceeded, StartedAt: &started},
            {JobID: 2, RunID: 7, NodeKey: "transform", DisplayName: "Transform", Status: repository.JobStatusLost, ReasonCode: &reasonCode, LastError: &failure},
            {JobID: 3, RunID: 7, NodeKey: "report", DisplayName: "Report", Status: repository.JobStatusBlocked},
        },
        Edges: []repository.RunGraphEdge{{FromJobID: 1, ToJobID: 2}, {FromJobID: 2, ToJobID: 3}},
    }
}
