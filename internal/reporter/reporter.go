package reporter

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Reporter struct {
	events  chan Event
	metrics chan MetricSnapshot
	done    chan struct{}
	wg      sync.WaitGroup
	out     io.Writer
}

func New() *Reporter {
	return &Reporter{
		events:  make(chan Event, 64),
		metrics: make(chan MetricSnapshot, 4),
		done:    make(chan struct{}),
		out:     os.Stdout,
	}
}

func (r *Reporter) Start() {
	r.wg.Add(1)
	go r.loop()
}

func (r *Reporter) Stop() {
	close(r.done)
	r.wg.Wait()
}

func (r *Reporter) Send(e Event) {
	select {
	case r.events <- e:
	default:
	}
}

func (r *Reporter) SendMetric(m MetricSnapshot) {
	select {
	case r.metrics <- m:
	default:
	}
}

func (r *Reporter) loop() {
	defer r.wg.Done()
	var lastMetric MetricSnapshot

	for {
		select {
		case e := <-r.events:
			r.renderEvent(e)
		case m := <-r.metrics:
			lastMetric = m
			r.renderMetrics(lastMetric)
		case <-r.done:
			// drain remaining events
			for {
				select {
				case e := <-r.events:
					r.renderEvent(e)
				default:
					return
				}
			}
		}
	}
}

func (r *Reporter) renderEvent(e Event) {
	var symbol string
	switch e.Type {
	case EventStepDone:
		symbol = "✓"
	case EventStepSkipped:
		symbol = "↷"
	case EventStepFailed:
		symbol = "✗"
	case EventStepStart:
		symbol = "⟳"
	case EventPhaseStart:
		fmt.Fprintf(r.out, "\n▶ %s\n", e.Phase)
		return
	case EventPhaseComplete:
		fmt.Fprintf(r.out, "✓ %-12s %s\n", e.Phase, e.At.Format("15:04:05"))
		return
	case EventCheckpoint:
		fmt.Fprintf(r.out, "\n── checkpoint ──────────────────────────────────\n%s\n", e.Message)
		return
	default:
		symbol = " "
	}
	ts := e.At.Format("15:04:05")
	if e.Message != "" {
		fmt.Fprintf(r.out, "  %s %-30s  %s  %s\n", symbol, e.Step, ts, e.Message)
	} else {
		fmt.Fprintf(r.out, "  %s %-30s  %s\n", symbol, e.Step, ts)
	}
}

func (r *Reporter) renderMetrics(m MetricSnapshot) {
	// Overwrite last two lines using ANSI escape: move up 2 lines, clear to end
	fmt.Fprintf(r.out, "\033[2A\033[J")
	if m.SlotLagBytes != nil {
		fmt.Fprintf(r.out, "  slot lag: %s\n", formatBytes(*m.SlotLagBytes))
	} else if m.SubLagMs != nil {
		fmt.Fprintf(r.out, "  sub lag:  %dms\n", *m.SubLagMs)
	} else {
		fmt.Fprintln(r.out)
	}
	fmt.Fprintf(r.out, "  %s\n", m.ClusterState)
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// PrintHeader prints the upgrade banner. Call once before Start().
func (r *Reporter) PrintHeader(clusterName, fromVersion, toVersion string) {
	fmt.Fprintf(r.out, "\n[pg-upgrade] %s  PG%s→PG%s  started: %s\n\n",
		clusterName, fromVersion, toVersion, time.Now().Format("2006-01-02 15:04:05"))
}
