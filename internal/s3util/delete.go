package s3util

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const awsDateFormat = "20060102T150405Z"

type Config struct {
	Bucket    string
	Endpoint  string
	AccessKey string
	SecretKey string
	Region    string
}

type Client struct {
	cfg        Config
	httpClient *http.Client
}

func NewClient(cfg Config) (*Client, error) {
	cfg.Bucket = strings.TrimSpace(cfg.Bucket)
	cfg.Endpoint = strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	cfg.AccessKey = strings.TrimSpace(cfg.AccessKey)
	cfg.SecretKey = strings.TrimSpace(cfg.SecretKey)
	cfg.Region = strings.TrimSpace(cfg.Region)
	if cfg.Region == "" {
		cfg.Region = "auto"
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return nil, fmt.Errorf("access key and secret key are required")
	}
	if _, err := url.Parse(cfg.Endpoint); err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	return &Client{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: 2 * time.Minute,
		},
	}, nil
}

func DeletePrefix(ctx context.Context, cfg Config, prefix string) (int, error) {
	client, err := NewClient(cfg)
	if err != nil {
		return 0, err
	}
	return client.DeletePrefix(ctx, prefix)
}

func (c *Client) DeletePrefix(ctx context.Context, prefix string) (int, error) {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return 0, fmt.Errorf("prefix is required")
	}
	prefix = prefix + "/"

	deleted := 0
	continuationToken := ""
	for {
		keys, nextToken, err := c.listObjects(ctx, prefix, continuationToken)
		if err != nil {
			return deleted, err
		}
		if len(keys) > 0 {
			if err := c.deleteObjects(ctx, keys); err != nil {
				return deleted, err
			}
			deleted += len(keys)
		}
		if nextToken == "" {
			return deleted, nil
		}
		continuationToken = nextToken
	}
}

func (c *Client) listObjects(ctx context.Context, prefix, continuationToken string) ([]string, string, error) {
	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("max-keys", "1000")
	query.Set("prefix", prefix)
	if continuationToken != "" {
		query.Set("continuation-token", continuationToken)
	}

	req, err := c.newRequest(ctx, http.MethodGet, query, nil)
	if err != nil {
		return nil, "", err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("list objects: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("read list objects response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, "", fmt.Errorf("list objects failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result listBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, "", fmt.Errorf("decode list objects response: %w", err)
	}

	keys := make([]string, 0, len(result.Contents))
	for _, object := range result.Contents {
		if strings.TrimSpace(object.Key) != "" {
			keys = append(keys, object.Key)
		}
	}
	if !result.IsTruncated {
		return keys, "", nil
	}
	return keys, strings.TrimSpace(result.NextContinuationToken), nil
}

func (c *Client) deleteObjects(ctx context.Context, keys []string) error {
	for len(keys) > 0 {
		n := len(keys)
		if n > 1000 {
			n = 1000
		}
		batch := keys[:n]
		keys = keys[n:]

		body, err := xml.Marshal(deleteObjectsRequest{
			XMLNS:   "http://s3.amazonaws.com/doc/2006-03-01/",
			Quiet:   true,
			Objects: deleteObjects(batch),
		})
		if err != nil {
			return fmt.Errorf("encode delete objects request: %w", err)
		}

		query := url.Values{}
		query.Set("delete", "")
		req, err := c.newRequest(ctx, http.MethodPost, query, body)
		if err != nil {
			return err
		}
		sum := md5.Sum(body)
		req.Header.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
		c.sign(req, body)

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("delete objects: %w", err)
		}
		respBody, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read delete objects response: %w", readErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close delete objects response: %w", closeErr)
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("delete objects failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		}

		var result deleteResult
		if len(respBody) > 0 {
			if err := xml.Unmarshal(respBody, &result); err != nil {
				return fmt.Errorf("decode delete objects response: %w", err)
			}
			if len(result.Errors) > 0 {
				first := result.Errors[0]
				return fmt.Errorf("delete object %q failed: %s %s", first.Key, first.Code, first.Message)
			}
		}
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method string, query url.Values, body []byte) (*http.Request, error) {
	endpoint, err := url.Parse(c.cfg.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	endpoint.Path = "/" + awsPercentEncode(c.cfg.Bucket)
	endpoint.RawQuery = canonicalQuery(query)

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	c.sign(req, body)
	return req, nil
}

func (c *Client) sign(req *http.Request, body []byte) {
	now := time.Now().UTC()
	amzDate := now.Format(awsDateFormat)
	dateStamp := now.Format("20060102")
	payloadHash := sha256Hex(body)

	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	canonicalURI := req.URL.EscapedPath()
	canonicalQueryString := canonicalQuery(req.URL.Query())
	canonicalHeaders, signedHeaders := canonicalHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		canonicalQueryString,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := strings.Join([]string{dateStamp, c.cfg.Region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	signature := hex.EncodeToString(hmacSHA256(signingKey(c.cfg.SecretKey, dateStamp, c.cfg.Region), []byte(stringToSign)))
	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		c.cfg.AccessKey,
		credentialScope,
		signedHeaders,
		signature,
	))
}

func canonicalHeaders(req *http.Request) (string, string) {
	headers := map[string]string{
		"host":                 req.URL.Host,
		"x-amz-content-sha256": req.Header.Get("X-Amz-Content-Sha256"),
		"x-amz-date":           req.Header.Get("X-Amz-Date"),
	}
	if contentMD5 := req.Header.Get("Content-MD5"); contentMD5 != "" {
		headers["content-md5"] = contentMD5
	}

	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.TrimSpace(headers[name]))
		b.WriteByte('\n')
	}
	return b.String(), strings.Join(names, ";")
}

func canonicalQuery(values url.Values) string {
	if len(values) == 0 {
		return ""
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(values))
	for _, key := range keys {
		valueList := append([]string(nil), values[key]...)
		sort.Strings(valueList)
		if len(valueList) == 0 {
			parts = append(parts, awsPercentEncode(key)+"=")
			continue
		}
		for _, value := range valueList {
			parts = append(parts, awsPercentEncode(key)+"="+awsPercentEncode(value))
		}
	}
	return strings.Join(parts, "&")
}

func awsPercentEncode(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte("0123456789ABCDEF"[c>>4])
		b.WriteByte("0123456789ABCDEF"[c&0x0f])
	}
	return b.String()
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func signingKey(secretKey, dateStamp, region string) []byte {
	dateKey := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	dateRegionKey := hmacSHA256(dateKey, []byte(region))
	dateRegionServiceKey := hmacSHA256(dateRegionKey, []byte("s3"))
	return hmacSHA256(dateRegionServiceKey, []byte("aws4_request"))
}

func hmacSHA256(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(data)
	return mac.Sum(nil)
}

func deleteObjects(keys []string) []deleteObject {
	objects := make([]deleteObject, 0, len(keys))
	for _, key := range keys {
		objects = append(objects, deleteObject{Key: key})
	}
	return objects
}

type listBucketResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

type deleteObjectsRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	XMLNS   string         `xml:"xmlns,attr,omitempty"`
	Quiet   bool           `xml:"Quiet"`
	Objects []deleteObject `xml:"Object"`
}

type deleteObject struct {
	Key string `xml:"Key"`
}

type deleteResult struct {
	Errors []struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}
