package s3util

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestLiveCopyObject copies objects inside a real bucket. It runs only when
// S3UTIL_LIVE_ENDPOINT, S3UTIL_LIVE_BUCKET, S3UTIL_LIVE_ACCESS_KEY and
// S3UTIL_LIVE_SECRET_KEY are set, writes only under a random scratch prefix,
// and deletes everything it wrote.
func TestLiveCopyObject(t *testing.T) {
	cfg := Config{
		Endpoint:     os.Getenv("S3UTIL_LIVE_ENDPOINT"),
		Bucket:       os.Getenv("S3UTIL_LIVE_BUCKET"),
		AccessKey:    os.Getenv("S3UTIL_LIVE_ACCESS_KEY"),
		SecretKey:    os.Getenv("S3UTIL_LIVE_SECRET_KEY"),
		Region:       os.Getenv("S3UTIL_LIVE_REGION"),
		SessionToken: os.Getenv("S3UTIL_LIVE_SESSION_TOKEN"),
	}
	if cfg.Endpoint == "" || cfg.Bucket == "" || cfg.AccessKey == "" || cfg.SecretKey == "" {
		t.Skip("S3UTIL_LIVE_* not set")
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if os.Getenv("S3UTIL_LIVE_CREATE_BUCKET") != "" {
		req, err := client.newRequest(ctx, http.MethodPut, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.roundTrip(req, "create bucket"); err != nil && !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") {
			t.Fatal(err)
		}
	}

	var id [6]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	source := "soak/scratch-copy-check-" + hex.EncodeToString(id[:])
	destination := source + "-pre-upgrade"
	names := []string{
		"ltx/0000/0000000000000443-0000000000000443.ltx",
		"ltx/0009/0000000000000001-000000000000042f.ltx",
		"profiles/2026-11-17T06:28:01Z_verification-failed heap+cpu=1 'q'.pprof.json",
		"odd/a&b?c#d%25e@f,g;h~i!j(k)l*m.ltx",
	}
	written := map[string]int{}
	cleanup := func() {
		for _, prefix := range []string{source + "/", destination + "/"} {
			objects, err := client.ListObjects(context.Background(), prefix)
			if err != nil {
				t.Errorf("list %s for cleanup: %v", prefix, err)
				continue
			}
			keys := make([]string, 0, len(objects))
			for _, object := range objects {
				keys = append(keys, object.Key)
			}
			if err := client.DeleteObjects(context.Background(), keys); err != nil {
				t.Errorf("delete %d object(s) under %s: %v", len(keys), prefix, err)
			}
			if left, err := client.ListObjects(context.Background(), prefix); err != nil || len(left) != 0 {
				t.Errorf("%d object(s) left under %s, %v", len(left), prefix, err)
			}
		}
	}
	defer cleanup()

	for i, name := range names {
		body := bytes.Repeat([]byte{byte('a' + i)}, 1000+i)
		req, err := client.newObjectRequest(ctx, http.MethodPut, source+"/"+name)
		if err != nil {
			t.Fatal(err)
		}
		req.Body = http.NoBody
		req, err = http.NewRequestWithContext(ctx, http.MethodPut, req.URL.String(), bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		client.sign(req, body)
		if _, err := client.roundTrip(req, "put object"); err != nil {
			t.Fatalf("put %q: %v", name, err)
		}
		written[name] = len(body)
	}
	for _, name := range names {
		if err := client.CopyObject(ctx, source+"/"+name, destination+"/"+name); err != nil {
			t.Fatalf("CopyObject(%q) error = %v", name, err)
		}
	}
	relative := func(prefix string) map[string]Object {
		objects, err := client.ListObjects(ctx, prefix+"/")
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]Object{}
		for _, object := range objects {
			out[strings.TrimPrefix(object.Key, prefix+"/")] = Object{Size: object.Size, ETag: object.ETag}
		}
		return out
	}
	original, copied := relative(source), relative(destination)
	if len(original) != len(names) || len(copied) != len(names) {
		t.Fatalf("listed %d source and %d copied object(s), want %d each", len(original), len(copied), len(names))
	}
	for _, name := range slices.Sorted(func(yield func(string) bool) {
		for name := range written {
			if !yield(name) {
				return
			}
		}
	}) {
		if original[name].Size != int64(written[name]) || copied[name] != original[name] || original[name].ETag == "" {
			t.Fatalf("%q: source %+v, copy %+v, want %d bytes and the same ETag", name, original[name], copied[name], written[name])
		}
	}
	t.Logf("copied %d object(s), sizes and ETags match, scratch prefixes %s and %s", len(names), source, destination)
}
