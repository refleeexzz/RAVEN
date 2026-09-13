package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCORS(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	t.Run("preflight answers 204 with allow headers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/api/jobs", nil)
		rec := httptest.NewRecorder()
		cors(ok).ServeHTTP(rec, req)

		if rec.Code != http.StatusNoContent {
			t.Fatalf("preflight status = %d, want 204", rec.Code)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("Allow-Origin = %q, want *", got)
		}
		if got := rec.Header().Get("Access-Control-Allow-Headers"); got == "" {
			t.Error("Allow-Headers missing on preflight")
		}
	})

	t.Run("regular request gets headers and reaches handler", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
		rec := httptest.NewRecorder()
		cors(ok).ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
			t.Errorf("Allow-Origin = %q, want *", got)
		}
	})
}
