package s3util

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
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
			if got := r.URL.Query().Get("prefix"); got != "soak/worker" {
				t.Errorf("prefix=%q, want soak/worker", got)
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
				t.Fatalf("decode delete request: %v", err)
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
