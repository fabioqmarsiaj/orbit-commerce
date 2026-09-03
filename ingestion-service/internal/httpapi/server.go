// Package httpapi exposes the ingestion-service's two HTTP endpoints:
// GET /health (liveness) and GET /stats (published-event counters).
// Deliberately built on net/http alone - for two simple GET endpoints, a
// routing framework like chi would be extra dependency weight with no
// real benefit (see docs/decisions.md, "Phase 8 - ingestion-service").
package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/fabioqmarsiaj/orbit-commerce/ingestion-service/internal/stats"
)

// NewServer builds an *http.Server listening on addr, serving GET /health
// and GET /stats from counters. The caller is responsible for actually
// running it (e.g. via ListenAndServe in a goroutine) and shutting it
// down.
func NewServer(addr string, counters *stats.Counters) *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(counters.Snapshot()); err != nil {
			// Encoding a plain struct of int64s/float64s into JSON cannot
			// realistically fail - this is defensive, not expected to
			// ever trigger in practice.
			http.Error(w, fmt.Sprintf("encoding stats: %v", err), http.StatusInternalServerError)
		}
	})

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}
