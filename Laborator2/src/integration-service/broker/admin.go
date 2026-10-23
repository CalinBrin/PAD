package broker

import (
	"encoding/json"
	"net/http"
)

// AdminHandler exposes operational endpoints (no state change, no costly work):
//
//	GET /healthz          liveness: the process runs (always 200)
//	GET /readyz           readiness: 200 when the broker accepts traffic,
//	                      503 when shut down or its storage is failing
//	GET /stats            queue depths, consumers, DLQ sizes
//	GET /dlq?queue=NAME   dead-lettered messages of a queue
func (b *Broker) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := b.Ready(); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "reason": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.Stats())
	})
	mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, b.DLQ(r.URL.Query().Get("queue")))
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
