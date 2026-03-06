package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/edkuperman/chronosched/internal/repository"
)

func TestExecuteREST_SuccessAndHeaders(t *testing.T) {
	var gotJobID, gotNodeKey, gotDef string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotJobID = r.Header.Get("X-Chronosched-Job-ID")
		gotNodeKey = r.Header.Get("X-Chronosched-Node-Key")
		gotDef = r.Header.Get("X-Chronosched-Definition-ID")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"message":"ok"}`))
	}))
	defer srv.Close()

	payload, _ := json.Marshal(map[string]any{"url": srv.URL, "body": map[string]any{"x": 1}})
	r := NewRunner("http://example", "worker-1")
	success, msg := r.executeREST(context.Background(), repository.QueueItem{JobID: 7, NodeKey: "hello", Definition: "def1", Payload: payload})
	if !success || msg != "ok" {
		t.Fatalf("expected success/ok, got %v/%q", success, msg)
	}
	if gotJobID != "7" || gotNodeKey != "hello" || gotDef != "def1" {
		t.Fatalf("unexpected propagated headers: %q %q %q", gotJobID, gotNodeKey, gotDef)
	}
}

func TestExecuteREST_FailureCases(t *testing.T) {
	r := NewRunner("http://example", "worker-1")
	if ok, msg := r.executeREST(context.Background(), repository.QueueItem{Payload: []byte(`{"body":{}}`)}); ok || msg == "" {
		t.Fatalf("expected missing url failure, got ok=%v msg=%q", ok, msg)
	}

	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusBadGateway)
	}))
	defer badSrv.Close()
	payload, _ := json.Marshal(map[string]any{"url": badSrv.URL})
	if ok, msg := r.executeREST(context.Background(), repository.QueueItem{Payload: payload}); ok || msg == "" {
		t.Fatalf("expected callback failure, got ok=%v msg=%q", ok, msg)
	}
}

func TestPollOnce_LeasesAndReportsResult(t *testing.T) {
	var mu sync.Mutex
	var reported []resultRequest
	leasePayload, _ := json.Marshal(leaseResponse{Items: []repository.QueueItem{{QueueID: 11, JobID: 22, NodeKey: "hello", Kind: "rest", Definition: "def1", Payload: json.RawMessage(`{"url":"http://placeholder"}`)}}})

	var serverURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/workers/lease":
			var resp leaseResponse
			_ = json.Unmarshal(leasePayload, &resp)
			resp.Items[0].Payload = json.RawMessage(`{"url":"` + serverURL + `/callback"}`)
			_ = json.NewEncoder(w).Encode(resp)
		case "/callback":
			_, _ = w.Write([]byte(`{"success":true}`))
		case "/internal/workers/result":
			var rr resultRequest
			_ = json.NewDecoder(r.Body).Decode(&rr)
			mu.Lock()
			reported = append(reported, rr)
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	serverURL = srv.URL

	r := NewRunner(serverURL, "worker-1")
	if err := r.pollOnce(context.Background()); err != nil {
		t.Fatalf("pollOnce error: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(reported)
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 1 {
		t.Fatalf("expected one reported result, got %d", len(reported))
	}
	if !reported[0].Success || reported[0].QueueID != 11 || reported[0].JobID != 22 {
		t.Fatalf("unexpected reported payload: %#v", reported[0])
	}
}
