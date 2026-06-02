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

func TestPause_SendsPutRequest(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/pause", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	err := c.Pause(context.Background())
	require.NoError(t, err)
	assert.True(t, called)
}

func TestResume_SendsPutRequest(t *testing.T) {
	var called bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		assert.Equal(t, "PUT", r.Method)
		assert.Equal(t, "/resume", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := patroni.NewHTTPClient(srv.URL)
	err := c.Resume(context.Background())
	require.NoError(t, err)
	assert.True(t, called)
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
