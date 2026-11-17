package s3util

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/corylanou/litestream-soak/internal/s3util/s3fake"
)

func TestCopyObjectEncodesKeysAndSignsTheCopySource(t *testing.T) {
	const awkward = "profiles/2026-11-17T06:28:01Z heap+cpu=1 'q'.pprof.json"
	var path, copySource, signed, token string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path, copySource, signed, token = r.URL.EscapedPath(), r.Header.Get("X-Amz-Copy-Source"), r.Header.Get("Authorization"), r.Header.Get("X-Amz-Security-Token")
		_, _ = w.Write([]byte("<CopyObjectResult/>"))
	}))
	defer server.Close()
	client, err := NewClient(Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "access", SecretKey: "secret", SessionToken: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CopyObject(context.Background(), "soak/a/"+awkward, "soak/a-pre-upgrade/"+awkward); err != nil {
		t.Fatal(err)
	}
	const encoded = "profiles/2026-11-17T06%3A28%3A01Z%20heap%2Bcpu%3D1%20%27q%27.pprof.json"
	if path != "/bucket/soak/a-pre-upgrade/"+encoded {
		t.Fatalf("request path = %q", path)
	}
	if copySource != "/bucket/soak/a/"+encoded {
		t.Fatalf("copy source = %q", copySource)
	}
	if !strings.Contains(signed, "SignedHeaders=host;x-amz-content-sha256;x-amz-copy-source;x-amz-date;x-amz-security-token,") || token != "session" {
		t.Fatalf("authorization = %q, token %q; want the copy source and session token signed", signed, token)
	}
}

func TestCopyObjectReportsAFailureSentWithStatusOK(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<Error><Code>InternalError</Code><Message>try again</Message></Error>"))
	}))
	defer server.Close()
	client, err := NewClient(Config{Bucket: "bucket", Endpoint: server.URL, AccessKey: "access", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.CopyObject(context.Background(), "a", "b"); err == nil || !strings.Contains(err.Error(), "InternalError try again") {
		t.Fatalf("CopyObject() error = %v, want the error carried in the 200 response", err)
	}
}

func TestListCopyAndDeleteObjects(t *testing.T) {
	store := s3fake.New("bucket")
	defer store.Close()
	store.Put("soak/a/ltx/0000/1.ltx", []byte("one"), "")
	store.Put("soak/a/ltx/0009/2.ltx", []byte("snapshot"), "abcd-2")
	store.Put("soak/a-pre-upgrade/ltx/0000/1.ltx", []byte("one"), "")
	client, err := NewClient(Config{Bucket: "bucket", Endpoint: store.URL, AccessKey: "access", SecretKey: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	objects, err := client.ListObjects(context.Background(), "soak/a/")
	if err != nil {
		t.Fatal(err)
	}
	want := []Object{{Key: "soak/a/ltx/0000/1.ltx", Size: 3, ETag: "f97c5d29941bfb1b2fdab0874906ab82"}, {Key: "soak/a/ltx/0009/2.ltx", Size: 8, ETag: "abcd-2"}}
	if !slices.Equal(objects, want) {
		t.Fatalf("ListObjects() = %+v, want %+v: the trailing slash keeps the sibling prefix out", objects, want)
	}
	if err := client.CopyObject(context.Background(), "soak/a/ltx/0009/2.ltx", "soak/b/2.ltx"); err != nil {
		t.Fatal(err)
	}
	if body, ok := store.Get("soak/b/2.ltx"); !ok || string(body) != "snapshot" {
		t.Fatalf("copied object = %q, %v", body, ok)
	}
	if err := client.DeleteObjects(context.Background(), []string{"soak/b/2.ltx", "soak/a-pre-upgrade/ltx/0000/1.ltx"}); err != nil {
		t.Fatal(err)
	}
	if left := store.Keys(""); !slices.Equal(left, []string{"soak/a/ltx/0000/1.ltx", "soak/a/ltx/0009/2.ltx"}) {
		t.Fatalf("objects left = %v", left)
	}
	if _, err := client.ListObjects(context.Background(), " "); err == nil {
		t.Fatal("ListObjects() with an empty prefix succeeded")
	}
}
