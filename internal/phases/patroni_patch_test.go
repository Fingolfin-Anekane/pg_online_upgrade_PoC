package phases

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// scopeDataBinConfig decodes a patched document back into the four fields under
// test, so assertions don't depend on byte-for-byte formatting.
func scopeDataBinConfig(t *testing.T, b []byte) (scope, dataDir, binDir, configDir string) {
	t.Helper()
	var doc struct {
		Scope      string `yaml:"scope"`
		Postgresql struct {
			DataDir   string `yaml:"data_dir"`
			BinDir    string `yaml:"bin_dir"`
			ConfigDir string `yaml:"config_dir"`
		} `yaml:"postgresql"`
	}
	require.NoError(t, yaml.Unmarshal(b, &doc))
	return doc.Scope, doc.Postgresql.DataDir, doc.Postgresql.BinDir, doc.Postgresql.ConfigDir
}

func TestPatchPatroniConfig_RewritesExistingKeys(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /usr/lib/postgresql/13/bin\n  config_dir: /etc/postgresql/13/main\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/usr/lib/postgresql/17/bin", "/etc/postgresql/17/main")
	require.NoError(t, err)
	scope, data, bin, cfg := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", bin)
	assert.Equal(t, "/etc/postgresql/17/main", cfg)
}

func TestPatchPatroniConfig_LeavesConfigDirWhenEmpty(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b\n  config_dir: /etc/postgresql/13/main\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/usr/lib/postgresql/17/bin", "")
	require.NoError(t, err)
	_, _, _, cfg := scopeDataBinConfig(t, out)
	assert.Equal(t, "/etc/postgresql/13/main", cfg) // untouched
}

func TestPatchPatroniConfig_InsertsMissingKeys(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n" // no bin_dir, no config_dir
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/usr/lib/postgresql/17/bin", "/etc/postgresql/17/main")
	require.NoError(t, err)
	scope, data, bin, cfg := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/usr/lib/postgresql/17/bin", bin)
	assert.Equal(t, "/etc/postgresql/17/main", cfg)
}

func TestPatchPatroniConfig_CreatesPostgresqlMappingWhenAbsent(t *testing.T) {
	in := "scope: prod\nrestapi:\n  listen: 0.0.0.0:8008\n" // no postgresql mapping at all
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "")
	require.NoError(t, err)
	scope, data, bin, _ := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/b17", bin)
}

func TestPatchPatroniConfig_PreservesCommentsAndOtherKeys(t *testing.T) {
	in := "# top comment\nscope: prod\nnamespace: /service/  # keep me\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "")
	require.NoError(t, err)
	s := string(out)
	assert.Contains(t, s, "# top comment")
	assert.Contains(t, s, "keep me")
	assert.Contains(t, s, "namespace: /service/")
}

func TestPatchPatroniConfig_Idempotent(t *testing.T) {
	in := "scope: prod\npostgresql:\n  data_dir: /data/pg13\n  bin_dir: /b13\n"
	once, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "/c17")
	require.NoError(t, err)
	twice, err := patchPatroniConfig(once, "prod-17", "/data/pg17", "/b17", "/c17")
	require.NoError(t, err)
	s1, d1, b1, c1 := scopeDataBinConfig(t, once)
	s2, d2, b2, c2 := scopeDataBinConfig(t, twice)
	assert.Equal(t, []string{s1, d1, b1, c1}, []string{s2, d2, b2, c2})
}

func TestPatchPatroniConfig_RejectsMalformedYAML(t *testing.T) {
	_, err := patchPatroniConfig([]byte("scope: [unclosed\n"), "prod-17", "/d", "/b", "")
	require.Error(t, err)
}

func TestPatchPatroniConfig_ReplacesNonMappingPostgresql(t *testing.T) {
	// A null/scalar postgresql node must not silently swallow the writes.
	in := "scope: prod\npostgresql: ~\n"
	out, err := patchPatroniConfig([]byte(in), "prod-17", "/data/pg17", "/b17", "")
	require.NoError(t, err)
	scope, data, bin, _ := scopeDataBinConfig(t, out)
	assert.Equal(t, "prod-17", scope)
	assert.Equal(t, "/data/pg17", data)
	assert.Equal(t, "/b17", bin)
}
