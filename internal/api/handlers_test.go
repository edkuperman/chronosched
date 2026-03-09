package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/edkuperman/chronosched/internal/repository"
	"github.com/go-chi/chi/v5"
)

type fakeNamespaces struct {
	list      []repository.Namespace
	created   *repository.Namespace
	getBy     *repository.Namespace
	listErr   error
	createErr error
	getErr    error
}

func (f *fakeNamespaces) List(context.Context) ([]repository.Namespace, error) {
	return f.list, f.listErr
}
func (f *fakeNamespaces) Create(_ context.Context, name string) (*repository.Namespace, error) {
	if f.created == nil {
		f.created = &repository.Namespace{Name: name}
	}
	return f.created, f.createErr
}
func (f *fakeNamespaces) GetByName(context.Context, string) (*repository.Namespace, error) {
	return f.getBy, f.getErr
}

type fakeDefinitions struct {
	created   *repository.JobDefinition
	createErr error
}

func (f *fakeDefinitions) ListByNamespace(context.Context, string) ([]repository.JobDefinition, error) {
	return nil, nil
}
func (f *fakeDefinitions) Create(_ context.Context, def repository.JobDefinition) (*repository.JobDefinition, error) {
	f.created = &def
	return f.created, f.createErr
}
func (f *fakeDefinitions) Get(context.Context, string) (*repository.JobDefinition, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeDefinitions) Update(context.Context, repository.JobDefinition) (*repository.JobDefinition, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeDefinitions) Delete(context.Context, string) error            { return nil }
func (f *fakeDefinitions) SetEnabled(context.Context, string, bool) error  { return nil }
func (f *fakeDefinitions) SetPaused(context.Context, string, bool) error   { return nil }
func (f *fakeDefinitions) ApplyFailurePolicy(context.Context, int64) error { return nil }
func (f *fakeDefinitions) ListUsages(context.Context, string) ([]repository.DefinitionUsage, error) {
	return nil, nil
}
func (f *fakeDefinitions) ListScheduledUsages(context.Context) ([]repository.ScheduledUsage, error) {
	return nil, nil
}
func (f *fakeDefinitions) ListScheduledParents(context.Context, string, string) ([]repository.ScheduledParent, error) {
	return nil, nil
}
func (f *fakeDefinitions) GetCronFireStatus(context.Context, string, time.Time) (*repository.CronFireStatus, error) {
	return nil, nil
}
func (f *fakeDefinitions) GetCronNextRun(context.Context, string) (*time.Time, error) {
	return nil, nil
}
func (f *fakeDefinitions) SetCronNextRun(context.Context, string, time.Time) error { return nil }

func TestParseInt64Param(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/runs/42", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("run_id", "42")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	got, err := parseInt64Param(req, "run_id")
	if err != nil {
		t.Fatalf("parseInt64Param error: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
}

func TestHealthz(t *testing.T) {
	h := NewHandler(&repository.Repos{})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	h.healthz(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got := rr.Body.String(); !bytes.Contains([]byte(got), []byte(`"status":"ok"`)) {
		t.Fatalf("unexpected response body: %s", got)
	}
}

func TestCreateNamespace(t *testing.T) {
	nsRepo := &fakeNamespaces{}
	h := NewHandler(&repository.Repos{Namespaces: nsRepo})
	rr := httptest.NewRecorder()
	body := bytes.NewBufferString(`{"name":"demo"}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/namespaces", body)

	h.createNamespace(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	if nsRepo.created == nil || nsRepo.created.Name != "demo" {
		t.Fatalf("expected namespace to be created, got %#v", nsRepo.created)
	}
}

func TestCreateDefinition_DefaultKindAndEnabled(t *testing.T) {
	defRepo := &fakeDefinitions{}
	h := NewHandler(&repository.Repos{Definitions: defRepo})
	rr := httptest.NewRecorder()
	payload := map[string]any{"namespace_id": "ns1", "name": "hello", "payload_template": map[string]any{"ok": true}}
	data, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/job-definitions", bytes.NewReader(data))

	h.createDefinition(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rr.Code, rr.Body.String())
	}
	if defRepo.created == nil {
		t.Fatal("expected definition to be created")
	}
	if defRepo.created.Kind != "cmd" {
		t.Fatalf("expected default kind cmd, got %q", defRepo.created.Kind)
	}
	if !defRepo.created.IsEnabled {
		t.Fatal("expected IsEnabled to default to true")
	}
}
