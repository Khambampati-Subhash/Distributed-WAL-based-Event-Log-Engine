package main

import (
	"fmt"
	"strings"
	"time"
)

// renderHTML builds a self-contained dashboard (inline CSS, no external assets)
// comparing the algorithms across the collected metrics.
func renderHTML(cfg config, results []result) string {
	var b strings.Builder

	b.WriteString(`<!doctype html><html lang="en"><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1">`)
	b.WriteString(`<title>Checksum Benchmark Dashboard</title>`)
	b.WriteString("<style>" + css + "</style></head><body>")

	b.WriteString(`<div class="wrap">`)
	b.WriteString(`<h1>Checksum Benchmark Dashboard</h1>`)
	b.WriteString(`<p class="sub">CRC32C vs SHA-256 in the WAL event-log engine — pure cost and end-to-end load.</p>`)

	// Run configuration.
	b.WriteString(`<div class="cfg">`)
	cfgItem(&b, "Messages", fmt.Sprintf("%d", cfg.messages))
	cfgItem(&b, "Producers", fmt.Sprintf("%d", cfg.producers))
	cfgItem(&b, "Consumers", fmt.Sprintf("%d", cfg.consumers))
	cfgItem(&b, "Payload", fmt.Sprintf("%d B", cfg.payload))
	cfgItem(&b, "Consumer sleep", cfg.consumerSleep.String())
	cfgItem(&b, "Micro iters", fmt.Sprintf("%d", cfg.microIters))
	cfgItem(&b, "Generated", time.Now().Format("2006-01-02 15:04:05"))
	b.WriteString(`</div>`)

	b.WriteString(`<div class="note"><strong>Read this first:</strong> the WAL fsyncs on every append, and an fsync (~ms) dwarfs a checksum (~ns–µs). So the <em>end-to-end produce</em> numbers are dominated by fsync, not the checksum — the algorithm difference lives in <em>Phase&nbsp;1 (pure checksum)</em> below. That contrast is the whole point.</div>`)

	// Phase 1 — pure checksum.
	b.WriteString(`<h2>Phase 1 — pure checksum cost <span class="tag">isolated CPU + allocations</span></h2>`)
	b.WriteString(`<div class="grid">`)
	chart(&b, "Time per checksum (ns/op) — lower is better", results, func(r result) float64 { return r.pureNsPerOp }, func(v float64) string { return fmt.Sprintf("%.1f ns", v) })
	chart(&b, "Throughput (MB/s) — higher is better", results, func(r result) float64 { return r.pureMBps }, func(v float64) string { return fmt.Sprintf("%.0f MB/s", v) })
	chart(&b, "Allocations per checksum — lower is better", results, func(r result) float64 { return r.pureAllocsPerOp }, func(v float64) string { return fmt.Sprintf("%.2f", v) })
	chart(&b, "Bytes allocated per checksum — lower is better", results, func(r result) float64 { return r.pureBytesPerOp }, func(v float64) string { return fmt.Sprintf("%.0f B", v) })
	b.WriteString(`</div>`)

	// Phase 2 — end-to-end.
	b.WriteString(`<h2>Phase 2 — end-to-end load <span class="tag">3 producers · 2 consumers · fsync-bound</span></h2>`)
	b.WriteString(`<div class="grid">`)
	chart(&b, "Produce throughput (msg/s) — higher is better", results, func(r result) float64 { return r.produceThroughput }, func(v float64) string { return fmt.Sprintf("%.0f/s", v) })
	chart(&b, "Produce latency p99 — lower is better", results, func(r result) float64 { return float64(r.producePctl.p99) }, func(v float64) string { return fmtDur(time.Duration(v)) })
	chart(&b, "End-to-end latency p99 — lower is better", results, func(r result) float64 { return float64(r.e2ePctl.p99) }, func(v float64) string { return fmtDur(time.Duration(v)) })
	chart(&b, "Peak heap during run — lower is better", results, func(r result) float64 { return float64(r.peakHeapBytes) }, func(v float64) string { return fmtBytes(uint64(v)) })
	b.WriteString(`</div>`)

	// Full numbers table.
	b.WriteString(`<h2>All numbers</h2>`)
	b.WriteString(`<div class="tablewrap"><table><thead><tr><th>Metric</th>`)
	for _, r := range results {
		b.WriteString("<th>" + r.name + "</th>")
	}
	b.WriteString(`</tr></thead><tbody>`)
	row(&b, "Checksum size", results, func(r result) string { return fmt.Sprintf("%d B", r.checksumSize) })
	row(&b, "Pure — ns/op", results, func(r result) string { return fmt.Sprintf("%.1f", r.pureNsPerOp) })
	row(&b, "Pure — MB/s", results, func(r result) string { return fmt.Sprintf("%.0f", r.pureMBps) })
	row(&b, "Pure — allocs/op", results, func(r result) string { return fmt.Sprintf("%.2f", r.pureAllocsPerOp) })
	row(&b, "Pure — bytes/op", results, func(r result) string { return fmt.Sprintf("%.0f", r.pureBytesPerOp) })
	row(&b, "Produce throughput (msg/s)", results, func(r result) string { return fmt.Sprintf("%.0f", r.produceThroughput) })
	row(&b, "Produce p50", results, func(r result) string { return fmtDur(r.producePctl.p50) })
	row(&b, "Produce p95", results, func(r result) string { return fmtDur(r.producePctl.p95) })
	row(&b, "Produce p99", results, func(r result) string { return fmtDur(r.producePctl.p99) })
	row(&b, "E2E p50", results, func(r result) string { return fmtDur(r.e2ePctl.p50) })
	row(&b, "E2E p95", results, func(r result) string { return fmtDur(r.e2ePctl.p95) })
	row(&b, "E2E p99", results, func(r result) string { return fmtDur(r.e2ePctl.p99) })
	row(&b, "Total run time", results, func(r result) string { return fmtDur(r.totalDur) })
	row(&b, "Total allocated", results, func(r result) string { return fmtBytes(r.totalAllocBytes) })
	row(&b, "Peak heap", results, func(r result) string { return fmtBytes(r.peakHeapBytes) })
	b.WriteString(`</tbody></table></div>`)

	b.WriteString(`<p class="foot">Generated by <code>cmd/benchmark</code>. Re-run with more <code>-messages</code> for a heavier load; raise <code>-consumer-sleep</code> to simulate slow processing (watch e2e latency climb as backlog builds).</p>`)
	b.WriteString(`</div></body></html>`)
	return b.String()
}

func cfgItem(b *strings.Builder, k, v string) {
	fmt.Fprintf(b, `<div class="cfgitem"><span class="k">%s</span><span class="v">%s</span></div>`, k, v)
}

// chart renders a titled panel with one horizontal bar per algorithm, scaled to
// the largest value so the two are visually comparable.
func chart(b *strings.Builder, title string, results []result, val func(result) float64, fmtVal func(float64) string) {
	var max float64
	for _, r := range results {
		if v := val(r); v > max {
			max = v
		}
	}
	fmt.Fprintf(b, `<div class="panel"><div class="ptitle">%s</div>`, title)
	for i, r := range results {
		v := val(r)
		pct := 2.0
		if max > 0 {
			pct = 2 + 98*v/max
		}
		fmt.Fprintf(b, `<div class="barrow"><span class="blabel">%s</span><div class="btrack"><div class="bfill c%d" style="width:%.1f%%"></div></div><span class="bval">%s</span></div>`,
			r.name, i, pct, fmtVal(v))
	}
	b.WriteString(`</div>`)
}

func row(b *strings.Builder, label string, results []result, cell func(result) string) {
	b.WriteString("<tr><td>" + label + "</td>")
	for _, r := range results {
		b.WriteString("<td>" + cell(r) + "</td>")
	}
	b.WriteString("</tr>")
}

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%d ns", d.Nanoseconds())
	case d < time.Millisecond:
		return fmt.Sprintf("%.2f µs", float64(d.Nanoseconds())/1e3)
	case d < time.Second:
		return fmt.Sprintf("%.2f ms", float64(d.Nanoseconds())/1e6)
	default:
		return fmt.Sprintf("%.2f s", d.Seconds())
	}
}

func fmtBytes(n uint64) string {
	f := float64(n)
	switch {
	case f < 1024:
		return fmt.Sprintf("%d B", n)
	case f < 1<<20:
		return fmt.Sprintf("%.1f KB", f/1024)
	case f < 1<<30:
		return fmt.Sprintf("%.1f MB", f/(1<<20))
	default:
		return fmt.Sprintf("%.2f GB", f/(1<<30))
	}
}

const css = `
:root{--bg:#f6f7f9;--card:#fff;--ink:#1a1d21;--muted:#5b6470;--line:#e5e8ec;--c0:#3b82f6;--c1:#8b5cf6;--accent:#0ea5e9;--note:#fff7ed;--noteline:#fdba74}
@media(prefers-color-scheme:dark){:root{--bg:#0f1216;--card:#171b21;--ink:#e8ebef;--muted:#9aa4b2;--line:#252b33;--c0:#60a5fa;--c1:#a78bfa;--note:#1f1a12;--noteline:#7c5b25}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
.wrap{max-width:960px;margin:0 auto;padding:32px 20px 64px}
h1{font-size:26px;margin:0 0 4px}.sub{color:var(--muted);margin:0 0 20px}
h2{font-size:18px;margin:32px 0 14px;display:flex;align-items:center;gap:10px}
.tag{font-size:11px;font-weight:600;color:var(--muted);background:var(--line);padding:3px 8px;border-radius:20px}
.cfg{display:flex;flex-wrap:wrap;gap:8px}
.cfgitem{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:8px 12px;display:flex;flex-direction:column}
.cfgitem .k{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.cfgitem .v{font-weight:600}
.note{margin:18px 0 0;background:var(--note);border:1px solid var(--noteline);border-radius:10px;padding:12px 14px;font-size:14px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:640px){.grid{grid-template-columns:1fr}}
.panel{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.ptitle{font-size:13px;font-weight:600;color:var(--muted);margin-bottom:12px}
.barrow{display:flex;align-items:center;gap:10px;margin:8px 0}
.blabel{width:64px;font-size:12px;font-weight:600}
.btrack{flex:1;background:var(--line);border-radius:6px;height:20px;overflow:hidden}
.bfill{height:100%;border-radius:6px}.bfill.c0{background:var(--c0)}.bfill.c1{background:var(--c1)}
.bval{width:96px;text-align:right;font-variant-numeric:tabular-nums;font-size:13px;font-weight:600}
.tablewrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px}
table{border-collapse:collapse;width:100%;background:var(--card)}
th,td{padding:10px 14px;text-align:left;border-bottom:1px solid var(--line);font-variant-numeric:tabular-nums}
th{font-size:12px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted)}
tbody tr:last-child td{border-bottom:none}
td:first-child{color:var(--muted)}
.foot{color:var(--muted);font-size:13px;margin-top:24px}
code{background:var(--line);padding:1px 5px;border-radius:4px;font-size:.9em}
`
