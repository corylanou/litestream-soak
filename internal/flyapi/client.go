package flyapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.machines.dev/v1"

const (
	maxResponseBodyBytes = 8 << 20
	maxErrorBodyBytes    = 64 << 10
)

type Client struct {
	baseURL    string
	appName    string
	token      string
	httpClient *http.Client
}

type APIError struct {
	StatusCode int
	Body       string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.StatusCode, e.Body)
}

func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound
}

func NewClient(appName, token string) *Client {
	return &Client{
		baseURL: defaultBaseURL,
		appName: appName,
		token:   token,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func NewClientWithBaseURL(appName, token, baseURL string) *Client {
	client := NewClient(appName, token)
	if strings.TrimSpace(baseURL) != "" {
		client.baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	}
	return client
}

func (c *Client) AppName() string {
	return c.appName
}

func (c *Client) ForApp(appName string) *Client {
	if appName == "" || appName == c.appName {
		return c
	}
	return &Client{
		baseURL:    c.baseURL,
		appName:    appName,
		token:      c.token,
		httpClient: c.httpClient,
	}
}

func (c *Client) ListMachines(ctx context.Context) ([]Machine, error) {
	var machines []Machine
	if err := c.do(ctx, "GET", fmt.Sprintf("/apps/%s/machines", c.appName), nil, &machines); err != nil {
		return nil, fmt.Errorf("list machines: %w", err)
	}
	return machines, nil
}

func (c *Client) GetMachine(ctx context.Context, id string) (*Machine, error) {
	var machine Machine
	if err := c.do(ctx, "GET", fmt.Sprintf("/apps/%s/machines/%s", c.appName, id), nil, &machine); err != nil {
		if IsNotFound(err) {
			machines, listErr := c.ListMachines(ctx)
			if listErr == nil {
				for _, candidate := range machines {
					if candidate.ID == id {
						return &candidate, nil
					}
				}
			}
		}
		return nil, fmt.Errorf("get machine %s: %w", id, err)
	}
	return &machine, nil
}

func (c *Client) CreateMachine(ctx context.Context, req CreateMachineRequest) (*Machine, error) {
	var machine Machine
	if err := c.do(ctx, "POST", fmt.Sprintf("/apps/%s/machines", c.appName), req, &machine); err != nil {
		return nil, fmt.Errorf("create machine: %w", err)
	}
	return &machine, nil
}

func (c *Client) StopMachine(ctx context.Context, id string) error {
	if err := c.do(ctx, "POST", fmt.Sprintf("/apps/%s/machines/%s/stop", c.appName, id), nil, nil); err != nil {
		return fmt.Errorf("stop machine %s: %w", id, err)
	}
	return nil
}

func (c *Client) StartMachine(ctx context.Context, id string) error {
	if err := c.do(ctx, "POST", fmt.Sprintf("/apps/%s/machines/%s/start", c.appName, id), nil, nil); err != nil {
		return fmt.Errorf("start machine %s: %w", id, err)
	}
	return nil
}

func (c *Client) DestroyMachine(ctx context.Context, id string, force bool) error {
	path := fmt.Sprintf("/apps/%s/machines/%s", c.appName, id)
	if force {
		path += "?force=true"
	}
	if err := c.do(ctx, "DELETE", path, nil, nil); err != nil {
		return fmt.Errorf("destroy machine %s: %w", id, err)
	}
	return nil
}

func (c *Client) ListVolumes(ctx context.Context) ([]Volume, error) {
	var volumes []Volume
	if err := c.do(ctx, "GET", fmt.Sprintf("/apps/%s/volumes", c.appName), nil, &volumes); err != nil {
		return nil, fmt.Errorf("list volumes: %w", err)
	}
	return volumes, nil
}

func (c *Client) GetVolume(ctx context.Context, id string) (*Volume, error) {
	var volume Volume
	if err := c.do(ctx, "GET", fmt.Sprintf("/apps/%s/volumes/%s", c.appName, id), nil, &volume); err != nil {
		return nil, fmt.Errorf("get volume %s: %w", id, err)
	}
	return &volume, nil
}

func (c *Client) CreateVolume(ctx context.Context, req CreateVolumeRequest) (*Volume, error) {
	var volume Volume
	if err := c.do(ctx, "POST", fmt.Sprintf("/apps/%s/volumes", c.appName), req, &volume); err != nil {
		return nil, fmt.Errorf("create volume: %w", err)
	}
	return &volume, nil
}

func (c *Client) ForkVolume(ctx context.Context, sourceID string, name string) (*Volume, error) {
	req := CreateVolumeRequest{
		Name:      name,
		SourceID:  sourceID,
		Encrypted: true,
	}
	return c.CreateVolume(ctx, req)
}

func (c *Client) DestroyVolume(ctx context.Context, id string) error {
	if err := c.do(ctx, "DELETE", fmt.Sprintf("/apps/%s/volumes/%s", c.appName, id), nil, nil); err != nil {
		return fmt.Errorf("destroy volume %s: %w", id, err)
	}
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, body interface{}, result interface{}) error {
	return c.doAbsolute(ctx, method, c.baseURL+path, body, result)
}

func (c *Client) doAbsolute(ctx context.Context, method, endpoint string, body interface{}, result interface{}) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reqBody)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		body, err := readBoundedBody(resp.Body, maxErrorBodyBytes)
		if err != nil {
			return fmt.Errorf("read error response: %w", err)
		}
		return &APIError{StatusCode: resp.StatusCode, Body: body}
	}

	if result == nil {
		if err := discardBoundedBody(resp.Body, maxResponseBodyBytes); err != nil {
			return fmt.Errorf("read response: %w", err)
		}
		return nil
	}
	if err := decodeBoundedJSON(resp.Body, result, maxResponseBodyBytes); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func readBoundedBody(r io.Reader, limit int64) (string, error) {
	body, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return "", err
	}
	if int64(len(body)) > limit {
		return responseBodyTooLarge(limit), nil
	}
	return string(body), nil
}

func discardBoundedBody(r io.Reader, limit int64) error {
	n, err := io.Copy(io.Discard, io.LimitReader(r, limit+1))
	if err != nil {
		return err
	}
	if n > limit {
		return errors.New(responseBodyTooLarge(limit))
	}
	return nil
}

func decodeBoundedJSON(r io.Reader, result interface{}, limit int64) error {
	limited := &io.LimitedReader{R: r, N: limit + 1}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(result); err != nil {
		if limited.N == 0 {
			return errors.New(responseBodyTooLarge(limit))
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}

	var trailing json.RawMessage
	err := decoder.Decode(&trailing)
	if limited.N == 0 {
		return errors.New(responseBodyTooLarge(limit))
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New("response contains multiple JSON values")
}

func responseBodyTooLarge(limit int64) string {
	return fmt.Sprintf("response body exceeds %d-byte limit", limit)
}
