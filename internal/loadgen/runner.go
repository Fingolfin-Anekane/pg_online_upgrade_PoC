package loadgen

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/dmbabuev/pg-upgrade/internal/loadcfg"
)

// throttle returns a tick channel for the given rate (ops/sec); 0 = no throttle.
func throttle(rate int) (<-chan time.Time, func()) {
	if rate <= 0 {
		ch := make(chan time.Time)
		close(ch) // always ready
		return ch, func() {}
	}
	tk := time.NewTicker(time.Second / time.Duration(rate))
	return tk.C, tk.Stop
}

// appendLoop runs append ops until ctx is cancelled, returning the op count.
func appendLoop(ctx context.Context, sel *Selector, w *Writer, m *Metrics, writerID, rate int) int64 {
	tick, stop := throttle(rate)
	defer stop()
	var seq, count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		if rate > 0 {
			select {
			case <-ctx.Done():
				return count
			case <-tick:
			}
		}
		pool, dsn, phase := sel.Active()
		seq++
		status, class := appendOnce(ctx, pool, w, dsn, phase, writerID, seq, "append")
		recordOutcome(m, "append", dsn, status, class)
		count++
	}
}

func rywLoop(ctx context.Context, sel *Selector, w *Writer, m *Metrics, writerID, rate int) int64 {
	tick, stop := throttle(rate)
	defer stop()
	var seq, count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		if rate > 0 {
			select {
			case <-ctx.Done():
				return count
			case <-tick:
			}
		}
		pool, dsn, phase := sel.Active()
		seq++
		status, class, _ := rywOnce(ctx, pool, w, dsn, phase, writerID, seq)
		recordOutcome(m, "ryw", dsn, status, class)
		count++
	}
}

func transferLoop(ctx context.Context, sel *Selector, m *Metrics, accounts, rate int) int64 {
	tick, stop := throttle(rate)
	defer stop()
	var count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		if rate > 0 {
			select {
			case <-ctx.Done():
				return count
			case <-tick:
			}
		}
		pool, dsn, _ := sel.Active()
		from := 1 + int(count)%accounts
		to := 1 + int(count+1)%accounts
		if from == to {
			to = 1 + (to % accounts)
		}
		status, class := transferOnce(ctx, pool, from, to, 1)
		recordOutcome(m, "transfer", dsn, status, class)
		count++
	}
}

func longTxnLoop(ctx context.Context, sel *Selector, w *Writer, m *Metrics, writerID int, batchSize int, hold time.Duration) int64 {
	var startSeq, count int64
	for {
		select {
		case <-ctx.Done():
			return count
		default:
		}
		pool, dsn, phase := sel.Active()
		status, class := longTxnOnce(ctx, pool, w, dsn, phase, writerID, startSeq+1, int64(batchSize), hold)
		recordOutcome(m, "long-txn", dsn, status, class)
		startSeq += int64(batchSize)
		count++
	}
}

// recordOutcome feeds a finished op into the live metrics. class is the error
// bucket ("" for acked) computed by the worker via errorClass.
func recordOutcome(m *Metrics, kind, dsn, status, class string) {
	now := time.Now()
	if status == StatusAcked {
		m.RecordCommit(kind, dsn, now)
		return
	}
	m.RecordError(class, now)
}

// reportLoop periodically prints a non-authoritative live snapshot to stdout.
func reportLoop(ctx context.Context, m *Metrics, every time.Duration) {
	tk := time.NewTicker(every)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			commits, errs := m.Snapshot()
			fmt.Fprintf(os.Stdout, "-- live: commits=%v errors=%v --\n", commits, errs)
		}
	}
}

// Run executes the full workload: opens pools, starts all workers, installs the
// SIGHUP swap and shutdown, and blocks until ctx is done or duration elapses.
func Run(ctx context.Context, cfg loadcfg.Config, openPool func(ctx context.Context, dsn string) (Pool, func(), error)) (*Metrics, error) {
	poolA, closeA, err := openPool(ctx, cfg.DSNA)
	if err != nil {
		return nil, fmt.Errorf("run open dsn_a: %w", err)
	}
	defer closeA()
	poolB, closeB, err := openPool(ctx, cfg.DSNB)
	if err != nil {
		return nil, fmt.Errorf("run open dsn_b: %w", err)
	}
	defer closeB()

	sel := NewSelector(poolA, poolB)
	w, err := NewWriter(cfg.IntentLog, os.Stdout)
	if err != nil {
		return nil, err
	}
	defer w.Close()
	m := NewMetrics()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if cfg.Duration > 0 {
		t := time.AfterFunc(time.Duration(cfg.Duration), cancel)
		defer t.Stop()
	}

	// SIGHUP -> swap; SIGINT/SIGTERM -> shutdown.
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	sigDone := make(chan struct{})
	defer close(sigDone) // stop the signal goroutine when Run returns
	go func() {
		for {
			select {
			case <-sigDone:
				return
			case s := <-sigCh:
				if s == syscall.SIGHUP {
					m.RecordSwitch(time.Now()) // mark switched before flipping the pool
					sel.Switch()
					fmt.Fprintln(os.Stdout, "== SIGHUP: switched DSN A -> B ==")
				} else {
					cancel()
					return
				}
			}
		}
	}()

	// Live throughput/error snapshot every 5s (observation, not a verdict).
	go reportLoop(runCtx, m, 5*time.Second)

	var wg sync.WaitGroup
	start := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); fn() }() }

	// writerID ranges are disjoint so the unique (writer_id, client_seq)
	// constraint never collides across workers: append [1000,2000),
	// ryw [2000,3000), long-txn [3000,4000). Supports up to 999 workers per
	// type; higher counts are not prevented.
	for i := 0; i < cfg.Workers.Append.Count; i++ {
		id := 1000 + i
		start(func() { appendLoop(runCtx, sel, w, m, id, cfg.Workers.Append.Rate) })
	}
	for i := 0; i < cfg.Workers.RYW.Count; i++ {
		id := 2000 + i
		start(func() { rywLoop(runCtx, sel, w, m, id, cfg.Workers.RYW.Rate) })
	}
	for i := 0; i < cfg.Workers.Transfer.Count; i++ {
		start(func() { transferLoop(runCtx, sel, m, cfg.Accounts, cfg.Workers.Transfer.Rate) })
	}
	for i := 0; i < cfg.Workers.LongTxn.Count; i++ {
		id := 3000 + i
		start(func() {
			longTxnLoop(runCtx, sel, w, m, id, cfg.Workers.LongTxn.BatchSize, time.Duration(cfg.Workers.LongTxn.TxnDuration))
		})
	}

	wg.Wait()
	return m, nil
}
