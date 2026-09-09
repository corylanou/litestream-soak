package s3util

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
)

func (c *Client) MeasurePrefix(ctx context.Context, prefix string) (int64, error) {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return 0, fmt.Errorf("fixture prefix is required")
	}
	prefix += "/"
	var total int64
	token := ""
	seen := make(map[string]bool)
	for {
		query := url.Values{"list-type": {"2"}, "max-keys": {"1000"}, "prefix": {prefix}}
		if token != "" {
			query.Set("continuation-token", token)
		}
		req, err := c.newRequest(ctx, http.MethodGet, query, nil)
		if err != nil {
			return 0, err
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return 0, err
		}
		var listing struct {
			Truncated bool   `xml:"IsTruncated"`
			Next      string `xml:"NextContinuationToken"`
			Objects   []struct {
				Key  string `xml:"Key"`
				Size int64  `xml:"Size"`
			} `xml:"Contents"`
		}
		err = xml.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&listing)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return 0, fmt.Errorf("measure objects: HTTP %d", resp.StatusCode)
		}
		if err != nil {
			return 0, fmt.Errorf("decode object measurement: %w", err)
		}
		for _, object := range listing.Objects {
			if !strings.HasPrefix(object.Key, prefix) || object.Size < 0 || object.Size > math.MaxInt64-total {
				return 0, fmt.Errorf("invalid object measurement")
			}
			total += object.Size
		}
		if !listing.Truncated {
			return total, nil
		}
		if listing.Next == "" || seen[listing.Next] {
			return 0, fmt.Errorf("invalid object continuation token")
		}
		seen[listing.Next] = true
		token = listing.Next
	}
}
