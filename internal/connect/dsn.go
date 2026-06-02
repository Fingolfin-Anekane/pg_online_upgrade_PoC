// Package connect derives per-host connection strings from a single credential
// template, so the orchestrator can reach both the primary (discovered at
// runtime) and the local N1 node using one configured superuser_dsn.
package connect

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// DSNForHost parses template (keyword or URL form) and returns a keyword DSN
// with Host replaced by host and all other connection parameters preserved.
func DSNForHost(template, host string) (string, error) {
	cfg, err := pgconn.ParseConfig(template)
	if err != nil {
		return "", fmt.Errorf("connect: parse dsn: %w", err)
	}

	parts := map[string]string{
		"host":   host,
		"port":   fmt.Sprintf("%d", cfg.Port),
		"user":   cfg.User,
		"dbname": cfg.Database,
	}
	if cfg.Password != "" {
		parts["password"] = cfg.Password
	}
	for k, v := range cfg.RuntimeParams {
		parts[k] = v
	}

	keys := make([]string, 0, len(parts))
	for k := range parts {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		if parts[k] == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte(' ')
		}
		v := parts[k]
		if strings.ContainsAny(v, " \t\\'") {
			v = strings.ReplaceAll(v, `\`, `\\`)
			v = strings.ReplaceAll(v, `'`, `\'`)
			v = "'" + v + "'"
		}
		fmt.Fprintf(&b, "%s=%s", k, v)
	}
	return b.String(), nil
}
