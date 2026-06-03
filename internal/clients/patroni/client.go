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
	Name   string   `json:"name"`
	Host   string   `json:"host"`
	Port   int      `json:"port"`
	Role   string   `json:"role"`
	State  string   `json:"state"`
	APIURL string   `json:"api_url"`
	Lag    LagBytes `json:"lag"`
}

// LagBytes is a member's replication lag in bytes. Patroni reports it as an
// integer, the string "unknown" when it cannot be determined, or null — so a
// plain int64 fails to decode. Non-numeric values decode to LagUnknown.
type LagBytes int64

// LagUnknown is the sentinel for a member whose lag Patroni could not report.
const LagUnknown LagBytes = -1

func (l *LagBytes) UnmarshalJSON(data []byte) error {
	s := string(data)
	switch {
	case s == "null":
		*l = LagUnknown
		return nil
	case len(s) > 0 && s[0] == '"': // a string such as "unknown"
		*l = LagUnknown
		return nil
	}
	var n int64
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*l = LagBytes(n)
	return nil
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
	username   string
	password   string
}

// Option configures an HTTPClient at construction time.
type Option func(*HTTPClient)

// WithBasicAuth sets the HTTP Basic credentials Patroni requires for unsafe REST
// methods (PATCH/PUT/POST) when restapi.authentication is enabled. Empty
// username leaves the client unauthenticated.
func WithBasicAuth(username, password string) Option {
	return func(c *HTTPClient) {
		c.username = username
		c.password = password
	}
}

func NewHTTPClient(baseURL string, opts ...Option) *HTTPClient {
	c := &HTTPClient{baseURL: baseURL, httpClient: &http.Client{}}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// authorize attaches Basic auth to req when credentials are configured. It is a
// no-op otherwise, so unauthenticated Patroni setups keep working.
func (c *HTTPClient) authorize(req *http.Request) {
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
}

func (c *HTTPClient) GetCluster(ctx context.Context) (*ClusterInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/cluster", nil)
	if err != nil {
		return nil, err
	}
	c.authorize(req) // harmless for GET; required if the operator protects all endpoints
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
	c.authorize(req)
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
