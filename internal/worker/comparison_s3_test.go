package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestComparisonS3ObserverCountsRetriesAndBodies(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = io.Copy(io.Discard, r.Body)
		if calls == 1 {
			w.WriteHeader(503)
		}
		_, _ = io.WriteString(w, "reply")
	}))
	defer upstream.Close()
	observer, err := StartComparisonS3Observer(context.Background(), upstream.URL, "us-east-1", "example", "example-secret", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close(context.Background()) }()
	for i := 0; i < 2; i++ {
		request, err := http.NewRequest("PUT", observer.Endpoint()+"/bucket/key", strings.NewReader("body"))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	evidence := observer.Evidence()
	if evidence.Requests != 2 || evidence.UploadBytes != 8 || evidence.DownloadBytes != 10 || len(evidence.Failures) != 1 {
		t.Fatalf("evidence=%+v", evidence)
	}
}

func TestComparisonS3ObserverRetainsTruncatedResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "20")
		_, _ = io.WriteString(w, "short")
	}))
	defer upstream.Close()
	observer, err := StartComparisonS3Observer(context.Background(), upstream.URL, "us-east-1", "example", "example-secret", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close(context.Background()) }()
	request, err := http.NewRequest("GET", observer.Endpoint()+"/bucket/key", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	response, err := http.DefaultClient.Do(request)
	if err == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	if len(observer.Evidence().Failures) == 0 {
		t.Fatal("lost truncated body failure")
	}
}
