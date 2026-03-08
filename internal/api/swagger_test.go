package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/edkuperman/chronosched/internal/repository"
)

func TestSwaggerUIRoute(t *testing.T) {
	h := NewHTTPHandler(NewHandler(&repository.Repos{}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "/openapi/chronosched.yaml") {
		t.Fatalf("swagger page missing spec url: %s", rr.Body.String())
	}
}
