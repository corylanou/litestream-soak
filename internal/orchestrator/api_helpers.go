package orchestrator

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

func readLimit(r *http.Request, fallback int) int {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return fallback
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		return fallback
	}
	return limit
}

func readBoolQuery(r *http.Request, key string) bool {
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get(key))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func respondError(w http.ResponseWriter, r *http.Request, status int, err error, msg string) {
	if msg == "" {
		msg = http.StatusText(status)
	}
	if err != nil {
		slog.Error("API request failed", "method", r.Method, "path", r.URL.Path, "status", status, "error", err)
	}
	http.Error(w, msg, status)
}

func writeAPIJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
