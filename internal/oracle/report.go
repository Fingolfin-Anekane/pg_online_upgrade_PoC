package oracle

import (
	"fmt"
	"io"
)

// Render writes a human-readable verdict to w and returns nil. The verdict line
// is PASS when f has no findings, FAIL otherwise.
func Render(w io.Writer, f *Findings) error {
	p := func(format string, a ...any) { fmt.Fprintf(w, format+"\n", a...) }

	p("=== loadtool verify ===")
	p("events: %d LOST, %d DUP, %d PHANTOM, %d PARTIAL", len(f.Lost), len(f.Dup), len(f.Phantom), len(f.Partial))
	if len(f.Lost) > 0 {
		p("  LOST ops: %v", f.Lost)
	}
	if len(f.Dup) > 0 {
		p("  DUP ops: %v", f.Dup)
	}
	if len(f.Phantom) > 0 {
		p("  PHANTOM ops: %v", f.Phantom)
	}
	if len(f.Partial) > 0 {
		p("  PARTIAL ops: %v", f.Partial)
	}
	p("sequence: %d duplicate id%s; last_value=%d max(id)=%d%s",
		f.DupID, ifThen(f.DupID > 0, "  <-- DUPLICATE ID", ""),
		f.LastValue, f.MaxID, ifThen(f.SeqBehind, "  <-- SEQUENCE BEHIND", ""))
	p("accounts: sum expected=%d actual=%d%s",
		f.SumExpected, f.SumActual, ifThen(f.SumExpected != f.SumActual, "  <-- sum mismatch", ""))

	if f.Failed() {
		p("VERDICT: FAIL")
	} else {
		p("VERDICT: PASS")
	}
	return nil
}

func ifThen(cond bool, yes, no string) string {
	if cond {
		return yes
	}
	return no
}
