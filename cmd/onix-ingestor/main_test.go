package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	healthz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, obtuve %d", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("respuesta no es JSON válido: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("esperaba status=ok, obtuve %v", body["status"])
	}
}

func TestReadyz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec := httptest.NewRecorder()

	readyz(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("esperaba 200, obtuve %d", rec.Code)
	}
}

func TestEnvFallback(t *testing.T) {
	if got := env("ONIX_NO_EXISTE_XYZ", "def"); got != "def" {
		t.Fatalf("esperaba fallback 'def', obtuve %q", got)
	}
	t.Setenv("ONIX_EXISTE_XYZ", "real")
	if got := env("ONIX_EXISTE_XYZ", "def"); got != "real" {
		t.Fatalf("esperaba 'real', obtuve %q", got)
	}
}
