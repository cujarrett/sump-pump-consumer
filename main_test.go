package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	healthHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status=ok, got %q", body["status"])
	}
}

func TestNotFoundHandler(t *testing.T) {
	for _, path := range []string{"/", "/.env", "/.git/config"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		notFoundHandler(w, req)
		if w.Code != http.StatusNotFound {
			t.Errorf("path %q: expected 404, got %d", path, w.Code)
		}
		if w.Body.Len() != 0 {
			t.Errorf("path %q: expected empty body", path)
		}
	}
}
