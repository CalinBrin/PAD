package broker

import (
	"encoding/json"
	"net/http"
)

// AdminHandler exposes read-only operational endpoints:
//
//	GET /stats            queue depths, consumers, DLQ sizes
//	GET /dlq?queue=NAME   dead-lettered messages of a queue
func (b *Broker) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, b.Stats())
	})
	mux.HandleFunc("/dlq", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, b.DLQ(r.URL.Query().Get("queue")))
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
