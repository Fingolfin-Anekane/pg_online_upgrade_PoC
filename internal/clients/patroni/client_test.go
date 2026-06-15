package patroni_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCluster_ReturnsLeaderAndMembers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/cluster", r.URL.Path)
		json.NewEncoder(w).Encode(map[string]any{
			"pause": false,
			"members": []map[string]any{
				{"name": "n0", "host": "n0.internal", "port": 5432, "role": "leader", "state": "running", "lag": 0},
				{"name": "n1", "host": "n1.internal", "port": 5432, "role": "replica", "state": "running", "lag": 100},
			},
		})
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	cluster, err := c.GetCluster(context.Background())
	require.NoError(t, err)

	assert.False(t, cluster.Paused)
	assert.Len(t, cluster.Members, 2)

	leader := cluster.Leader()
	require.NotNil(t, leader)
	assert.Equal(t, "n0.internal", leader.Host)
	assert.Equal(t, "leader", leader.Role)
}

func TestGetCluster_ToleratesNonNumericLag(t *testing.T) {
	// Patroni reports lag as the string "unknown" when it cannot be determined,
	// and as null for some members; neither must break decoding.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"members": []map[string]any{
				{"name": "n0", "host": "n0", "role": "leader", "lag": 0},
				{"name": "n1", "host": "n1", "role": "replica", "lag": "unknown"},
				{"name": "n2", "host": "n2", "role": "replica", "lag": nil},
				{"name": "n3", "host": "n3", "role": "replica", "lag": 4096},
			},
		})
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	cluster, err := c.GetCluster(context.Background())
	require.NoError(t, err)
	require.Len(t, cluster.Members, 4)
	assert.Equal(t, "n0", cluster.Leader().Name)
	assert.Equal(t, int64(4096), int64(cluster.Members[3].Lag))
}

func TestGetCluster_NoLeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"pause":   false,
			"members": []map[string]any{},
		})
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	cluster, err := c.GetCluster(context.Background())
	require.NoError(t, err)
	assert.Nil(t, cluster.Leader())
}

// Patroni has no /pause endpoint: maintenance mode is toggled via the dynamic
// config (PATCH /config {"pause": true|false}), the same path patronictl uses.
func TestNodePaused_ReadsAppliedPauseFromPatroniEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/patroni", r.URL.Path)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"state":"running","role":"replica","pause":true}`))
	}))
	defer srv.Close()

	paused, err := patroni.NewHTTPClient(srv.URL).NodePaused(context.Background())
	require.NoError(t, err)
	assert.True(t, paused)
}

func TestNodePaused_FalseWhenFieldAbsent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"state":"running","role":"replica"}`)) // not yet applied
	}))
	defer srv.Close()

	paused, err := patroni.NewHTTPClient(srv.URL).NodePaused(context.Background())
	require.NoError(t, err)
	assert.False(t, paused)
}

func TestPause_PatchesConfigPauseTrue(t *testing.T) {
	var called bool
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/config", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	err := c.Pause(context.Background())
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, true, body["pause"])
}

func TestResume_PatchesConfigPauseFalse(t *testing.T) {
	var called bool
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/config", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	err := c.Resume(context.Background())
	require.NoError(t, err)
	assert.True(t, called)
	assert.Equal(t, false, body["pause"])
}

func TestPause_SendsBasicAuthWhenConfigured(t *testing.T) {
	var gotUser, gotPass string
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, hadAuth = r.BasicAuth()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL, patroni.WithBasicAuth("admin", "s3cret"))
	require.NoError(t, c.Pause(context.Background()))
	assert.True(t, hadAuth, "expected Authorization header on PATCH /config")
	assert.Equal(t, "admin", gotUser)
	assert.Equal(t, "s3cret", gotPass)
}

func TestPause_NoAuthHeaderWhenUnconfigured(t *testing.T) {
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _, hadAuth = r.BasicAuth()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL) // no creds
	require.NoError(t, c.Pause(context.Background()))
	assert.False(t, hadAuth, "no Authorization header expected when creds are unset")
}

func TestSetStandbyCluster(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/config", r.URL.Path)
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	require.NoError(t, c.SetStandbyCluster(context.Background(), "prod-primary", 5432, "shadow_phys"))

	sc, ok := gotBody["standby_cluster"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod-primary", sc["host"])
	assert.Equal(t, float64(5432), sc["port"])
	assert.Equal(t, "shadow_phys", sc["primary_slot_name"])
}

func TestClearStandbyCluster(t *testing.T) {
	var gotBody map[string]any
	var hasKey bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPatch, r.Method)
		assert.Equal(t, "/config", r.URL.Path)
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_, hasKey = gotBody["standby_cluster"]
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	require.NoError(t, c.ClearStandbyCluster(context.Background()))

	require.True(t, hasKey, "standby_cluster key must be present so Patroni clears it")
	assert.Nil(t, gotBody["standby_cluster"])
}

func TestGetCluster_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	_, err := c.GetCluster(context.Background())
	assert.ErrorContains(t, err, "500")
}
