package s3util

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
)

func TestDeletePrefixDeletesListedObjects(t *testing.T) {
	t.Parallel()

	deleted := make([]string, 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			t.Error("Authorization header is empty")
		}
		if r.Header.Get("X-Amz-Date") == "" {
			t.Error("X-Amz-Date header is empty")
		}
		if r.URL.Path != "/bucket" {
			t.Errorf("path=%q, want /bucket", r.URL.Path)
		}

		switch r.Method {
		case http.MethodGet:
			if got := r.URL.Query().Get("prefix"); got != "soak/worker/" {
				t.Errorf("prefix=%q, want soak/worker/", got)
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<ListBucketResult>
				<IsTruncated>false</IsTruncated>
				<Contents><Key>soak/worker/generation</Key></Contents>
				<Contents><Key>soak/worker/index/00000001.ltx</Key></Contents>
			</ListBucketResult>`))
		case http.MethodPost:
			if _, ok := r.URL.Query()["delete"]; !ok {
				t.Error("delete query parameter is missing")
			}
			if r.Header.Get("Content-MD5") == "" {
				t.Error("Content-MD5 header is empty")
			}
			var req deleteObjectsRequest
			if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode delete request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, object := range req.Objects {
				deleted = append(deleted, object.Key)
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<DeleteResult/>`))
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	defer server.Close()

	count, err := DeletePrefix(t.Context(), Config{
		Bucket:    "bucket",
		Endpoint:  server.URL,
		AccessKey: "access",
		SecretKey: "secret",
		Region:    "auto",
	}, "soak/worker")
	if err != nil {
		t.Fatalf("DeletePrefix() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("DeletePrefix() count = %d, want 2", count)
	}
	if got := strings.Join(deleted, ","); got != "soak/worker/generation,soak/worker/index/00000001.ltx" {
		t.Fatalf("deleted keys = %q", got)
	}
}

func TestDeletePrefixRequiresPrefix(t *testing.T) {
	t.Parallel()

	_, err := DeletePrefix(t.Context(), Config{
		Bucket:    "bucket",
		Endpoint:  "http://127.0.0.1",
		AccessKey: "access",
		SecretKey: "secret",
	}, "")
	if err == nil {
		t.Fatal("DeletePrefix() error = nil, want error")
	}
}

func newFakeS3Server(t *testing.T, allKeys []string) (*httptest.Server, func() []string) {
	t.Helper()
	var deletedKeys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bucket" {
			t.Errorf("path=%q, want /bucket", r.URL.Path)
		}
		switch r.Method {
		case http.MethodGet:
			requestedPrefix := r.URL.Query().Get("prefix")
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			var sb strings.Builder
			sb.WriteString(`<ListBucketResult><IsTruncated>false</IsTruncated>`)
			for _, key := range allKeys {
				if strings.HasPrefix(key, requestedPrefix) {
					sb.WriteString(`<Contents><Key>`)
					sb.WriteString(key)
					sb.WriteString(`</Key></Contents>`)
				}
			}
			sb.WriteString(`</ListBucketResult>`)
			_, _ = w.Write([]byte(sb.String()))
		case http.MethodPost:
			if _, ok := r.URL.Query()["delete"]; !ok {
				t.Error("delete query parameter is missing")
				return
			}
			var req deleteObjectsRequest
			if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode delete request: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			for _, object := range req.Objects {
				deletedKeys = append(deletedKeys, object.Key)
			}
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<DeleteResult/>`))
		default:
			t.Errorf("unexpected method %s", r.Method)
		}
	}))
	return server, func() []string {
		out := append([]string(nil), deletedKeys...)
		sort.Strings(out)
		return out
	}
}

func TestDeletePrefixIsolation(t *testing.T) {
	t.Parallel()

	cfg := func(endpoint string) Config {
		return Config{
			Bucket:    "bucket",
			Endpoint:  endpoint,
			AccessKey: "access",
			SecretKey: "secret",
			Region:    "auto",
		}
	}

	tests := []struct {
		name          string
		allKeys       []string
		deletePrefix  string
		wantDeleted   []string
		wantSurviving []string
		wantErr       bool
	}{
		{
			name: "sibling prefix not matched (short names)",
			allKeys: []string{
				"soak/a/file1",
				"soak/a/file2",
				"soak/ab/file1",
				"soak/ab/file2",
			},
			deletePrefix: "soak/a",
			wantDeleted: []string{
				"soak/a/file1",
				"soak/a/file2",
			},
			wantSurviving: []string{
				"soak/ab/file1",
				"soak/ab/file2",
			},
		},
		{
			name: "sibling prefix not matched (realistic worker names)",
			allKeys: []string{
				"soak/worker-main-gharchive/generation",
				"soak/worker-main-gharchive/index/00000001.ltx",
				"soak/worker-main-gharchive-mixed/generation",
				"soak/worker-main-gharchive-mixed/index/00000001.ltx",
			},
			deletePrefix: "soak/worker-main-gharchive",
			wantDeleted: []string{
				"soak/worker-main-gharchive/generation",
				"soak/worker-main-gharchive/index/00000001.ltx",
			},
			wantSurviving: []string{
				"soak/worker-main-gharchive-mixed/generation",
				"soak/worker-main-gharchive-mixed/index/00000001.ltx",
			},
		},
		{
			name: "trailing-slash input behaves identically to no trailing slash",
			allKeys: []string{
				"soak/worker/generation",
				"soak/worker/index/00000001.ltx",
				"soak/worker-extra/generation",
			},
			deletePrefix: "soak/worker/",
			wantDeleted: []string{
				"soak/worker/generation",
				"soak/worker/index/00000001.ltx",
			},
			wantSurviving: []string{
				"soak/worker-extra/generation",
			},
		},
		{
			name: "nested prefixes deleted",
			allKeys: []string{
				"soak/a/nested/deep/file1",
				"soak/a/nested/deep/file2",
				"soak/b/other",
			},
			deletePrefix: "soak/a",
			wantDeleted: []string{
				"soak/a/nested/deep/file1",
				"soak/a/nested/deep/file2",
			},
			wantSurviving: []string{
				"soak/b/other",
			},
		},
		{
			name:         "empty prefix rejected",
			deletePrefix: "",
			wantErr:      true,
		},
		{
			name:         "slash-only prefix rejected",
			deletePrefix: "/",
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server, getDeleted := newFakeS3Server(t, tc.allKeys)
			defer server.Close()

			_, err := DeletePrefix(t.Context(), cfg(server.URL), tc.deletePrefix)
			if tc.wantErr {
				if err == nil {
					t.Fatal("DeletePrefix() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("DeletePrefix() error = %v", err)
			}

			deleted := getDeleted()

			wantDeleted := append([]string(nil), tc.wantDeleted...)
			sort.Strings(wantDeleted)
			if got := strings.Join(deleted, ","); got != strings.Join(wantDeleted, ",") {
				t.Errorf("deleted keys = %q, want %q", got, strings.Join(wantDeleted, ","))
			}

			for _, surviving := range tc.wantSurviving {
				for _, d := range deleted {
					if d == surviving {
						t.Errorf("key %q should have survived but was deleted", surviving)
					}
				}
			}
		})
	}
}
