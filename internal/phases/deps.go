// Package phases implements the pg-upgrade FSM phases (Prepare, Isolate, Drain,
// Upgrade) as runner.Phase values built from a shared Deps.
package phases

import (
	"context"

	"github.com/dmbabuev/pg-upgrade/internal/clients/patroni"
	pg "github.com/dmbabuev/pg-upgrade/internal/clients/pg"
	"github.com/dmbabuev/pg-upgrade/internal/clients/pgbin"
	"github.com/dmbabuev/pg-upgrade/internal/config"
	"github.com/dmbabuev/pg-upgrade/internal/runner"
	"github.com/dmbabuev/pg-upgrade/internal/slotdrain"
	"github.com/dmbabuev/pg-upgrade/internal/state"
)

// Deps is the dependency set shared by all phase steps.
type Deps struct {
	Cfg     config.Config
	Mgr     *state.Manager
	Patroni patroni.Client
	Tools   pgbin.PGTools

	// N1 is the local node's PG client (PG10 before upgrade).
	N1 pg.Client

	// Primary returns a client to the current primary. It is resolved lazily
	// because the primary host is only known after DiscoverTopology.
	Primary func(ctx context.Context) (pg.Client, error)

	// Drain runs the slot drain (injected for testability).
	Drain func(ctx context.Context, cfg slotdrain.Config) (*slotdrain.Report, error)

	// PG17 returns a client to the upgraded PG17 on N1 (new cluster primary).
	PG17 func(ctx context.Context) (pg.Client, error)

	// NewPatroni is the new cluster's Patroni REST client (separate from Patroni,
	// which manages the old, paused cluster).
	NewPatroni patroni.Client

	// Shadow returns a pg client to the shadow cluster's leader (PG13 standby
	// during provision, PG17 after upgrade). Resolved lazily.
	Shadow func(ctx context.Context) (pg.Client, error)

	// WriteSignal persists the DSN-swap signal file (injected for testability).
	WriteSignal func(path string, data []byte) error

	// Log receives parameter-level progress lines emitted from within steps. It
	// may be nil (tests construct Deps without it); use logf, which nil-guards.
	Log runner.Logger
}

// logf emits an indented detail line under the current step. It is a no-op when
// no logger was injected, so steps can call it unconditionally.
func (d Deps) logf(format string, args ...any) {
	if d.Log != nil {
		d.Log.Detail(format, args...)
	}
}
