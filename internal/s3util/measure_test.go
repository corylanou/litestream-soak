package s3util

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMeasurePrefix(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("prefix") != "fixture/" {
			t.Errorf("prefix=%s", r.URL.RawQuery)
		}
		if calls == 1 {
			_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>fixture/a</Key><Size>11</Size></Contents></ListBucketResult>`)
			return
		}
		if r.URL.Query().Get("continuation-token") != "next" {
			t.Error("missing token")
		}
		_, _ = fmt.Fprint(w, `<ListBucketResult><Contents><Key>fixture/b</Key><Size>23</Size></Contents></ListBucketResult>`)
	}))
	defer server.Close()
	client, err := NewClient(Config{Bucket: "test", Endpoint: server.URL, AccessKey: "example", SecretKey: "example"})
	if err != nil {
		t.Fatal(err)
	}
	size, err := client.MeasurePrefix(context.Background(), "fixture")
	if err != nil || size != 34 || calls != 2 {
		t.Fatalf("size=%d calls=%d err=%v", size, calls, err)
	}
}

func TestMeasurePrefixRejectsInvalidListing(t *testing.T) {
	for _, body := range []string{`broken`, `<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`, `<ListBucketResult><Contents><Key>other/a</Key><Size>1</Size></Contents></ListBucketResult>`, `<ListBucketResult><Contents><Key>fixture/a</Key><Size>-1</Size></Contents></ListBucketResult>`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = fmt.Fprint(w, body) }))
			defer server.Close()
			client, err := NewClient(Config{Bucket: "test", Endpoint: server.URL, AccessKey: "example", SecretKey: "example"})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.MeasurePrefix(context.Background(), "fixture"); err == nil {
				t.Fatal("accepted invalid listing")
			}
		})
	}
}
