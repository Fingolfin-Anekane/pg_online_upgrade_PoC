package patroni

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// Client is the interface used by phases to interact with Patroni.
type Client interface {
	GetCluster(ctx context.Context) (*ClusterInfo, error)
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
}

type ClusterInfo struct {
	Members []Member `json:"members"`
	Paused  bool     `json:"pause"`
}

type Member struct {
	Name   string `json:"name"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	Role   string `json:"role"`
	State  string `json:"state"`
	APIURL string `json:"api_url"`
	Lag    int64  `json:"lag"`
}

// Leader returns the primary member, or nil if there is no leader.
func (c *ClusterInfo) Leader() *Member {
	for i := range c.Members {
		if c.Members[i].Role == "leader" {
			return &c.Members[i]
		}
	}
	return nil
}

// HTTPClient is the real Patroni REST client.
type HTTPClient struct {
	baseURL    string
	httpClient *http.Client
}

func NewHTTPClient(baseURL string) *HTTPClient {
	return &HTTPClient{baseURL: baseURL, httpClient: &http.Client{}}
}

func (c *HTTPClient) GetCluster(ctx context.Context) (*ClusterInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/cluster", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("patroni /cluster: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("patroni /cluster: HTTP %d", resp.StatusCode)
	}

	var info ClusterInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("patroni /cluster decode: %w", err)
	}
	return &info, nil
}

// Pause puts the cluster into maintenance mode. Patroni has no /pause endpoint;
// pause is a key in the dynamic configuration, toggled via PATCH /config (the
// same mechanism as `patronictl pause`).
func (c *HTTPClient) Pause(ctx context.Context) error {
	return c.patchConfig(ctx, map[string]any{"pause": true})
}

// Resume lifts maintenance mode by clearing the pause flag in the dynamic config.
func (c *HTTPClient) Resume(ctx context.Context) error {
	return c.patchConfig(ctx, map[string]any{"pause": false})
}

// patchConfig deep-merges patch into Patroni's dynamic configuration. PATCH is
// the unsafe-but-idempotent path Patroni exposes for config edits; it returns
// 200 with the merged config on success.
func (c *HTTPClient) patchConfig(ctx context.Context, patch map[string]any) error {
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.baseURL+"/config", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("patroni PATCH /config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("patroni PATCH /config: HTTP %d", resp.StatusCode)
	}
	return nil
}
