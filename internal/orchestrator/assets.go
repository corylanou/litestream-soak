package orchestrator

import (
	_ "embed"
	"net/http"
)

//go:embed assets/favicon.svg
var faviconSVG []byte

func (a *API) handleFavicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(faviconSVG)
}
