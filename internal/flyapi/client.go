package flyapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultBaseURL = "https://api.machines.dev/v1"
const defaultLogsBaseURL = "https://api.fly.io/api/v1"

type Client struct {
	baseURL    string
	appName    string
	token      string
	httpClient *http.Client
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
		if strings.Contains(err.Error(), "API error 404") {
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

func (c *Client) ListAppLogs(ctx context.Context, instanceID, nextToken string) (*AppLogsResponse, error) {
	values := url.Values{}
	if instanceID != "" {
		values.Set("instance", instanceID)
	}
	if nextToken != "" {
		values.Set("next_token", nextToken)
	}

	endpoint := fmt.Sprintf("%s/apps/%s/logs?%s", defaultLogsBaseURL, url.PathEscape(c.appName), values.Encode())

	var result AppLogsResponse
	if err := c.doAbsolute(ctx, "GET", endpoint, nil, &result); err != nil {
		return nil, fmt.Errorf("list app logs: %w", err)
	}
	return &result, nil
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
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
	}

	if result != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, result); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}

	return nil
}
