package orchestrator

import (
	"embed"
	"fmt"
	"hash/fnv"
	"io/fs"
	"net/http"
	"sort"
	"strings"
)

//go:embed assets/favicon.svg
var faviconSVG []byte

//go:embed assets
var assetsFS embed.FS

var assetVersion = computeAssetVersion()

func computeAssetVersion() string {
	entries, err := assetsFS.ReadDir("assets")
	if err != nil {
		return "dev"
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)

	hasher := fnv.New64a()
	for _, name := range names {
		body, err := assetsFS.ReadFile("assets/" + name)
		if err != nil {
			return "dev"
		}
		_, _ = hasher.Write([]byte(name))
		_, _ = hasher.Write(body)
	}
	return fmt.Sprintf("%016x", hasher.Sum64())
}

func (a *API) handleFavicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/svg+xml")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(faviconSVG)
}

func assetsHandler() http.Handler {
	sub, err := fs.Sub(assetsFS, "assets")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServerFS(sub)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "" || strings.HasSuffix(r.URL.Path, "/") {
			http.NotFound(w, r)
			return
		}
		if info, err := fs.Stat(sub, r.URL.Path); err != nil || info.IsDir() {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		fileServer.ServeHTTP(w, r)
	})
}
