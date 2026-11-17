package s3util

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Object is one entry of a bucket listing. ETag has its quotes removed.
type Object struct {
	Key  string
	Size int64
	ETag string
}

type listObjectsResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
		ETag string `xml:"ETag"`
	} `xml:"Contents"`
}

type s3ErrorResult struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// ListObjects returns every object whose key starts with prefix, which is
// used exactly as given.
func (c *Client) ListObjects(ctx context.Context, prefix string) ([]Object, error) {
	if strings.TrimSpace(prefix) == "" {
		return nil, fmt.Errorf("prefix is required")
	}
	var objects []Object
	continuationToken := ""
	for {
		query := url.Values{}
		query.Set("list-type", "2")
		query.Set("max-keys", "1000")
		query.Set("prefix", prefix)
		if continuationToken != "" {
			query.Set("continuation-token", continuationToken)
		}
		req, err := c.newRequest(ctx, http.MethodGet, query, nil)
		if err != nil {
			return nil, err
		}
		body, err := c.roundTrip(req, "list objects")
		if err != nil {
			return nil, err
		}
		var result listObjectsResult
		if err := xml.Unmarshal(body, &result); err != nil {
			return nil, fmt.Errorf("decode list objects response: %w", err)
		}
		for _, object := range result.Contents {
			if strings.TrimSpace(object.Key) != "" {
				objects = append(objects, Object{Key: object.Key, Size: object.Size, ETag: strings.Trim(object.ETag, `"`)})
			}
		}
		continuationToken = strings.TrimSpace(result.NextContinuationToken)
		if !result.IsTruncated || continuationToken == "" {
			return objects, nil
		}
	}
}

// CopyObject copies sourceKey to destinationKey inside the bucket without
// downloading it. Both keys are percent-encoded segment by segment, in the
// request path and in the copy-source header, so a key with spaces, plus
// signs, colons or other reserved characters signs and resolves correctly.
func (c *Client) CopyObject(ctx context.Context, sourceKey, destinationKey string) error {
	if sourceKey == "" || destinationKey == "" {
		return fmt.Errorf("source and destination keys are required")
	}
	req, err := c.newObjectRequest(ctx, http.MethodPut, destinationKey)
	if err != nil {
		return err
	}
	req.Header.Set("X-Amz-Copy-Source", "/"+awsPercentEncode(c.cfg.Bucket)+"/"+encodeKeyPath(sourceKey))
	c.sign(req, nil)
	body, err := c.roundTrip(req, "copy object")
	if err != nil {
		return fmt.Errorf("%w (source %q)", err, sourceKey)
	}
	// A copy can fail after the 200 status line has been sent.
	var failure s3ErrorResult
	if xml.Unmarshal(body, &failure) == nil && failure.Code != "" {
		return fmt.Errorf("copy object %q failed: %s %s", sourceKey, failure.Code, failure.Message)
	}
	return nil
}

// DeleteObjects removes exactly the given keys.
func (c *Client) DeleteObjects(ctx context.Context, keys []string) error {
	return c.deleteObjects(ctx, keys)
}

func (c *Client) newObjectRequest(ctx context.Context, method, key string) (*http.Request, error) {
	endpoint, err := url.Parse(c.cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	endpoint.Path = "/" + c.cfg.Bucket + "/" + key
	endpoint.RawPath = "/" + awsPercentEncode(c.cfg.Bucket) + "/" + encodeKeyPath(key)
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	return req, nil
}

func (c *Client) roundTrip(req *http.Request, what string) ([]byte, error) {
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", what, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", what, err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s failed: HTTP %d: %s", what, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}

func encodeKeyPath(key string) string {
	segments := strings.Split(key, "/")
	for i, segment := range segments {
		segments[i] = awsPercentEncode(segment)
	}
	return strings.Join(segments, "/")
}
