// Package s3fake is an in-memory S3 endpoint for tests: path-style listing,
// server-side copy and batch delete for one bucket.
package s3fake

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
)

type Server struct {
	*httptest.Server
	Bucket string

	mu      sync.Mutex
	objects map[string]object
	calls   []string
	// OnCopy runs before each copy with the decoded source and destination
	// keys. A non-nil error fails the copy with that message.
	OnCopy func(source, destination string) error
}

type object struct {
	body []byte
	etag string
	size int64
}

func New(bucket string) *Server {
	s := &Server{Bucket: bucket, objects: map[string]object{}}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// Put stores an object. A non-empty etag overrides the content MD5, as for
// an object that was uploaded in parts.
func (s *Server) Put(key string, body []byte, etag string) {
	if etag == "" {
		sum := md5.Sum(body)
		etag = hex.EncodeToString(sum[:])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = object{body: slices.Clone(body), etag: etag, size: int64(len(body))}
}

// SetSize makes listings report size for key, so a test can stand in for an
// object too large to hold in memory.
func (s *Server) SetSize(key string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.objects[key]
	o.size = size
	s.objects[key] = o
}

func (s *Server) Get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[key]
	return slices.Clone(o.body), ok
}

func (s *Server) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
}

func (s *Server) Keys(prefix string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var keys []string
	for key := range s.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	return keys
}

// Calls lists the mutating requests seen so far as "copy SRC DST" and
// "delete KEY".
func (s *Server) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	root := "/" + s.Bucket
	w.Header().Set("Content-Type", "application/xml")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == root:
		s.list(w, r.URL.Query().Get("prefix"))
	case r.Method == http.MethodPost && r.URL.Path == root:
		s.remove(w, r)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, root+"/") && r.Header.Get("X-Amz-Copy-Source") != "":
		s.copyObject(w, r, strings.TrimPrefix(r.URL.Path, root+"/"))
	default:
		http.Error(w, "<Error><Code>NotImplemented</Code></Error>", http.StatusNotImplemented)
	}
}

func (s *Server) list(w http.ResponseWriter, prefix string) {
	var b strings.Builder
	b.WriteString("<ListBucketResult><IsTruncated>false</IsTruncated>")
	for _, key := range s.Keys(prefix) {
		s.mu.Lock()
		o := s.objects[key]
		s.mu.Unlock()
		var escaped strings.Builder
		_ = xml.EscapeText(&escaped, []byte(key))
		fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>%d</Size><ETag>&quot;%s&quot;</ETag></Contents>", escaped.String(), o.size, o.etag)
	}
	b.WriteString("</ListBucketResult>")
	_, _ = io.WriteString(w, b.String())
}

func (s *Server) remove(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	body, _ := io.ReadAll(r.Body)
	if err := xml.Unmarshal(body, &request); err != nil {
		http.Error(w, "<Error><Code>MalformedXML</Code></Error>", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for _, o := range request.Objects {
		delete(s.objects, o.Key)
		s.calls = append(s.calls, "delete "+o.Key)
	}
	s.mu.Unlock()
	_, _ = io.WriteString(w, "<DeleteResult/>")
}

func (s *Server) copyObject(w http.ResponseWriter, r *http.Request, destination string) {
	raw := r.Header.Get("X-Amz-Copy-Source")
	decoded, err := url.PathUnescape(raw)
	source, ok := strings.CutPrefix(decoded, "/"+s.Bucket+"/")
	if err != nil || !ok || strings.ContainsAny(raw, " +'") {
		http.Error(w, "<Error><Code>SignatureDoesNotMatch</Code><Message>copy source is not percent-encoded</Message></Error>", http.StatusForbidden)
		return
	}
	if s.OnCopy != nil {
		if err := s.OnCopy(source, destination); err != nil {
			http.Error(w, "<Error><Code>InternalError</Code><Message>"+err.Error()+"</Message></Error>", http.StatusInternalServerError)
			return
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o, found := s.objects[source]
	if !found {
		http.Error(w, "<Error><Code>NoSuchKey</Code></Error>", http.StatusNotFound)
		return
	}
	if strings.Contains(o.etag, "-") {
		sum := md5.Sum(o.body)
		o.etag = hex.EncodeToString(sum[:])
	}
	s.objects[destination] = o
	s.calls = append(s.calls, "copy "+source+" "+destination)
	_, _ = io.WriteString(w, "<CopyObjectResult/>")
}
