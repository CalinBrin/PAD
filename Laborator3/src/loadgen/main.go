// Command loadgen sends concurrent requests through the proxy and writes the
// measurements to files (evidence for the cache / no cache and failure
// comparisons):
//
//	loadgen -label cache-on-healthy -duration 20s -c 16 -out docs/evidence/results
//	loadgen -summarize docs/evidence/results      (writes summary.md from every *.json)
//
// Each run writes <out>/<label>.json and appends one row to <out>/summary.csv.
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
	"pad-lab3/src/proxy/proxy"
)

// Result is the content of <label>.json.
type Result struct {
	Label         string           `json:"label"`
	Scenario      string           `json:"scenario"`
	Started       time.Time        `json:"started"`
	DurationS     float64          `json:"duration_s"`
	Concurrency   int              `json:"concurrency"`
	IDs           int              `json:"ids"`
	WriteRatio    float64          `json:"write_ratio"`
	Distribution  string           `json:"distribution"`
	Format        string           `json:"format"`
	CacheEnabled  *bool            `json:"cache_enabled,omitempty"`
	Algorithm     string           `json:"algorithm,omitempty"`
	Requests      int64            `json:"requests"`
	OK            int64            `json:"ok"`
	Errors        int64            `json:"errors"`
	Statuses      map[string]int64 `json:"statuses"`
	ThroughputRPS float64          `json:"throughput_rps"`
	LatencyMs     Latency          `json:"latency_ms"`
	ReadLatencyMs Latency          `json:"read_latency_ms"`
	XCache        map[string]int64 `json:"x_cache"`
	ByBackend     map[string]int64 `json:"answered_by_backend"`
	ClientConns   struct {
		New    int64 `json:"new"`
		Reused int64 `json:"reused"`
	} `json:"client_connections"`
	Proxy *ProxyDelta `json:"proxy_delta,omitempty"`
}

type Latency struct {
	Mean, P50, P90, P95, P99, Max float64
}

// ProxyDelta: what the proxy counted during the run (from /proxy/stats).
type ProxyDelta struct {
	UpstreamRequests int64            `json:"upstream_requests"`
	UpstreamPerReq   float64          `json:"upstream_requests_per_client_request"`
	Retries          int64            `json:"retries"`
	UpstreamErrors   int64            `json:"upstream_errors"`
	Hits             int64            `json:"cache_hits"`
	Misses           int64            `json:"cache_misses"`
	UpConnsNew       int64            `json:"upstream_conns_new"`
	UpConnsReused    int64            `json:"upstream_conns_reused"`
	BackendServed    map[string]int64 `json:"backend_served"`
	BackendsAfter    any              `json:"backends_after"`
}

type sample struct {
	d     time.Duration
	write bool
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	return sorted[i]
}

func summarize(ds []float64) Latency {
	if len(ds) == 0 {
		return Latency{}
	}
	sort.Float64s(ds)
	sum := 0.0
	for _, d := range ds {
		sum += d
	}
	r := func(v float64) float64 { return math.Round(v*100) / 100 }
	return Latency{r(sum / float64(len(ds))), r(percentile(ds, 50)), r(percentile(ds, 90)),
		r(percentile(ds, 95)), r(percentile(ds, 99)), r(ds[len(ds)-1])}
}

func proxyStats(admin string) (*proxy.StatsView, error) {
	if admin == "" {
		return nil, nil
	}
	resp, err := http.Get(admin + "/proxy/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var v proxy.StatsView
	return &v, json.NewDecoder(resp.Body).Decode(&v)
}

func main() {
	base := flag.String("url", "http://localhost:8080", "proxy URL")
	admin := flag.String("admin", "http://localhost:8081", "proxy admin URL ('' to skip the proxy counters)")
	dur := flag.Duration("duration", 20*time.Second, "duration of the run")
	conc := flag.Int("c", 16, "concurrent clients (each keeps its connection alive)")
	ids := flag.Int("ids", 50, "number of employees read")
	prefix := flag.String("prefix", "bench", "id prefix of the employees")
	seed := flag.Bool("seed", false, "PUT the employees before the run")
	writeRatio := flag.Float64("write-ratio", 0.02, "share of PUT requests (they invalidate the cache)")
	dist := flag.String("dist", "zipf", "zipf (a few hot employees) or uniform")
	format := flag.String("format", "json", "json or xml")
	label := flag.String("label", "run", "name of the result file")
	scenario := flag.String("scenario", "", "free text describing the scenario")
	out := flag.String("out", "docs/evidence/results", "output directory")
	sum := flag.String("summarize", "", "write summary.md from the *.json of this directory and exit")
	flag.Parse()

	if *sum != "" {
		if err := writeSummary(*sum); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	f := httpx.JSON
	if *format == "xml" {
		f = httpx.XML
	}
	client := httpx.NewClient(*base, f, 10*time.Second)
	client.HTTP.Transport.(*http.Transport).MaxIdleConnsPerHost = *conc
	client.From = "loadgen"
	idOf := func(i int) string { return fmt.Sprintf("%s-%03d", *prefix, i+1) }
	if *seed {
		for i := 0; i < *ids; i++ {
			e := model.Employee{ID: idOf(i), Name: "Bench " + strconv.Itoa(i+1), Department: "Bench", Position: "Load", Salary: 1000}
			resp, err := client.Do(context.Background(), http.MethodPut, "/employees/"+e.ID, e, nil)
			if err != nil || resp.Status >= 300 {
				fmt.Fprintln(os.Stderr, "seed failed:", err, resp)
				os.Exit(1)
			}
		}
	}

	before, err := proxyStats(*admin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proxy stats unavailable:", err)
	}
	res := Result{Label: *label, Scenario: *scenario, Started: time.Now().UTC(), Concurrency: *conc, IDs: *ids,
		WriteRatio: *writeRatio, Distribution: *dist, Format: string(f),
		Statuses: map[string]int64{}, XCache: map[string]int64{}, ByBackend: map[string]int64{}}
	if before != nil {
		en := before.Cache.Enabled
		res.CacheEnabled = &en
		res.Algorithm = before.Upstream.Algorithm
	}
	var mu sync.Mutex
	var connNew, connReused atomic.Int64
	trace := &httptrace.ClientTrace{GotConn: func(i httptrace.GotConnInfo) {
		if i.Reused {
			connReused.Add(1)
		} else {
			connNew.Add(1)
		}
	}}
	deadline := time.Now().Add(*dur)
	samples := make([][]sample, *conc)
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w)+1, 42))
			zipf := rand.NewZipf(rng, 1.2, 1, uint64(*ids-1))
			for n := 0; time.Now().Before(deadline); n++ {
				var i int
				if *dist == "uniform" {
					i = rng.IntN(*ids)
				} else {
					i = int(zipf.Uint64())
				}
				ctx := httptrace.WithClientTrace(logx.WithCorrelationID(context.Background(),
					fmt.Sprintf("%s-%d-%d", *label, w, n)), trace)
				write := rng.Float64() < *writeRatio
				t0 := time.Now()
				var resp *httpx.Response
				var err error
				if write {
					e := model.Employee{ID: idOf(i), Name: "Bench " + strconv.Itoa(i+1), Department: "Bench",
						Position: "Load", Salary: int64(1000 + rng.IntN(100000))}
					resp, err = client.Do(ctx, http.MethodPut, "/employees/"+e.ID, e, nil)
				} else {
					resp, err = client.Do(ctx, http.MethodGet, "/employees/"+idOf(i), nil, nil)
				}
				d := time.Since(t0)
				mu.Lock()
				res.Requests++
				samples[w] = append(samples[w], sample{d, write})
				if err != nil {
					res.Errors++
					res.Statuses["transport_error"]++
				} else {
					res.Statuses[strconv.Itoa(resp.Status)]++
					if resp.Status < 400 {
						res.OK++
					} else {
						res.Errors++
					}
					if v := resp.Header.Get("X-Cache"); v != "" {
						res.XCache[v]++
					}
					if v := resp.Header.Get("X-Upstream"); v != "" {
						res.ByBackend[v]++
					}
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	res.DurationS = math.Round(elapsed.Seconds()*100) / 100
	res.ThroughputRPS = math.Round(float64(res.Requests)/elapsed.Seconds()*10) / 10
	var all, reads []float64
	for _, ws := range samples {
		for _, s := range ws {
			ms := float64(s.d.Microseconds()) / 1000
			all = append(all, ms)
			if !s.write {
				reads = append(reads, ms)
			}
		}
	}
	res.LatencyMs, res.ReadLatencyMs = summarize(all), summarize(reads)
	res.ClientConns.New, res.ClientConns.Reused = connNew.Load(), connReused.Load()

	if after, err := proxyStats(*admin); err == nil && after != nil && before != nil {
		d := &ProxyDelta{
			UpstreamRequests: after.Upstream.Requests - before.Upstream.Requests,
			Retries:          after.Upstream.Retries - before.Upstream.Retries,
			UpstreamErrors:   after.Upstream.Errors - before.Upstream.Errors,
			Hits:             after.Cache.Hits - before.Cache.Hits,
			Misses:           after.Cache.Misses - before.Cache.Misses,
			UpConnsNew:       after.Upstream.ConnsNew - before.Upstream.ConnsNew,
			UpConnsReused:    after.Upstream.ConnsReused - before.Upstream.ConnsReused,
			BackendServed:    map[string]int64{},
			BackendsAfter:    after.Upstream.Backends,
		}
		prev := map[string]int64{}
		for _, b := range before.Upstream.Backends {
			prev[b.Name] = b.Served
		}
		for _, b := range after.Upstream.Backends {
			d.BackendServed[b.Name] = b.Served - prev[b.Name]
		}
		if res.Requests > 0 {
			d.UpstreamPerReq = math.Round(float64(d.UpstreamRequests)/float64(res.Requests)*1000) / 1000
		}
		res.Proxy = d
	}

	if err := os.MkdirAll(*out, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(filepath.Join(*out, *label+".json"), append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := appendCSV(filepath.Join(*out, "summary.csv"), res); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	fmt.Printf("%-28s req=%d ok=%d err=%d rps=%.1f p50=%.2fms p95=%.2fms p99=%.2fms hit=%d miss=%d upstream/req=%s\n",
		res.Label, res.Requests, res.OK, res.Errors, res.ThroughputRPS, res.LatencyMs.P50, res.LatencyMs.P95,
		res.LatencyMs.P99, res.XCache["HIT"], res.XCache["MISS"], upPerReq(res))
}

func upPerReq(r Result) string {
	if r.Proxy == nil {
		return "-"
	}
	return strconv.FormatFloat(r.Proxy.UpstreamPerReq, 'f', 3, 64)
}

var csvHeader = []string{"label", "scenario", "cache_enabled", "algorithm", "concurrency", "duration_s", "requests", "ok", "errors",
	"throughput_rps", "p50_ms", "p95_ms", "p99_ms", "max_ms", "hits", "misses", "upstream_requests", "upstream_per_request", "retries", "upstream_errors"}

func row(r Result) []string {
	ce, up, upr, rt, ue := "", "", "", "", ""
	if r.CacheEnabled != nil {
		ce = strconv.FormatBool(*r.CacheEnabled)
	}
	if r.Proxy != nil {
		up, upr = strconv.FormatInt(r.Proxy.UpstreamRequests, 10), strconv.FormatFloat(r.Proxy.UpstreamPerReq, 'f', 3, 64)
		rt, ue = strconv.FormatInt(r.Proxy.Retries, 10), strconv.FormatInt(r.Proxy.UpstreamErrors, 10)
	}
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }
	return []string{r.Label, r.Scenario, ce, r.Algorithm, strconv.Itoa(r.Concurrency), f(r.DurationS),
		strconv.FormatInt(r.Requests, 10), strconv.FormatInt(r.OK, 10), strconv.FormatInt(r.Errors, 10),
		f(r.ThroughputRPS), f(r.LatencyMs.P50), f(r.LatencyMs.P95), f(r.LatencyMs.P99), f(r.LatencyMs.Max),
		strconv.FormatInt(r.XCache["HIT"], 10), strconv.FormatInt(r.XCache["MISS"], 10), up, upr, rt, ue}
}

func appendCSV(path string, r Result) error {
	_, statErr := os.Stat(path)
	fh, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer fh.Close()
	w := csv.NewWriter(fh)
	if statErr != nil {
		_ = w.Write(csvHeader)
	}
	_ = w.Write(row(r))
	w.Flush()
	return w.Error()
}

// writeSummary renders every result of dir as a markdown table.
func writeSummary(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		return err
	}
	sort.Strings(files)
	var b strings.Builder
	b.WriteString("# Rezultatele măsurătorilor\n\nGenerat de `loadgen -summarize` din fișierele `*.json` ale acestui director.\n\n")
	b.WriteString("| Rulare | Scenariu | Cache | Algoritm | Cereri | Erori | RPS | p50 ms | p95 ms | p99 ms | HIT | MISS | Cereri la DW / cerere client | Retry | Distribuția pe DW |\n")
	b.WriteString("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|\n")
	for _, fn := range files {
		raw, err := os.ReadFile(fn)
		if err != nil {
			return err
		}
		var r Result
		if err := json.Unmarshal(raw, &r); err != nil {
			continue
		}
		ce := "-"
		if r.CacheEnabled != nil {
			ce = map[bool]string{true: "da", false: "nu"}[*r.CacheEnabled]
		}
		up, rt, distr := "-", "-", "-"
		if r.Proxy != nil {
			up = strconv.FormatFloat(r.Proxy.UpstreamPerReq, 'f', 3, 64)
			rt = strconv.FormatInt(r.Proxy.Retries, 10)
			names := make([]string, 0, len(r.Proxy.BackendServed))
			for n := range r.Proxy.BackendServed {
				names = append(names, n)
			}
			sort.Strings(names)
			parts := []string{}
			for _, n := range names {
				parts = append(parts, fmt.Sprintf("%s:%d", n, r.Proxy.BackendServed[n]))
			}
			distr = strings.Join(parts, " ")
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s | %d | %d | %.1f | %.2f | %.2f | %.2f | %d | %d | %s | %s | %s |\n",
			r.Label, r.Scenario, ce, r.Algorithm, r.Requests, r.Errors, r.ThroughputRPS, r.LatencyMs.P50,
			r.LatencyMs.P95, r.LatencyMs.P99, r.XCache["HIT"], r.XCache["MISS"], up, rt, distr)
	}
	return os.WriteFile(filepath.Join(dir, "summary.md"), []byte(b.String()), 0o644)
}
