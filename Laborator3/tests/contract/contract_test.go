// Contract tests: the HTTP API of contracts/http-api.md, checked on a
// warehouse replica directly and through the proxy (the proxy must be
// transparent: same statuses, bodies and headers).
package contract

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"
	"testing"
	"time"

	"pad-lab3/src/common/model"
	"pad-lab3/tests/testutil"
)

const empJSON = `{"id":"e-1","name":"Ana Rusu","department":"IT","position":"Engineer","email":"ana@example.md","salary":12000}`
const empXML = `<employee id="e-2"><name>Ion Lungu</name><department>HR</department><position>Analyst</position><email>ion@example.md</email><salary>9000</salary></employee>`

// Each subtest has its own system (empty database).
func TestContract(t *testing.T) {
	t.Run("dw", func(t *testing.T) {
		contract(t, testutil.Start(t, testutil.Options{Replicas: 1}).DWs[0].Server.URL)
	})
	t.Run("proxy", func(t *testing.T) {
		contract(t, testutil.Start(t, testutil.Options{Replicas: 2}).Server.URL)
	})
}

func contract(t *testing.T, base string) {
	J := map[string]string{"Content-Type": "application/json", "Accept": "application/json", "From": "src-json@test"}
	X := map[string]string{"Content-Type": "application/xml", "Accept": "application/xml", "From": "src-xml@test"}

	// PUT creates: 201, Location, ETag, Cache-Control: no-store
	r := testutil.Raw(t, base, "PUT", "/employees/e-1", empJSON, J)
	if r.Status != 201 || r.Header.Get("Location") != "/employees/e-1" || r.Header.Get("ETag") != `"v1-json"` ||
		r.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("PUT create: %d %v %s", r.Status, r.Header, r.Body)
	}
	// the same PUT again: 200, no change, same version
	r = testutil.Raw(t, base, "PUT", "/employees/e-1", empJSON, J)
	if r.Status != 200 || r.Header.Get("X-Changed") != "false" || testutil.Employee(t, r).Version != 1 {
		t.Fatalf("idempotent PUT: %d %s", r.Status, r.Body)
	}
	// a PUT in XML (the XML source node)
	r = testutil.Raw(t, base, "PUT", "/employees/e-2", empXML, X)
	if r.Status != 201 || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/xml") {
		t.Fatalf("PUT xml: %d %s", r.Status, r.Body)
	}

	// GET JSON: the fields of the contract (golden)
	r = testutil.Raw(t, base, "GET", "/employees/e-1", "", map[string]string{"Accept": "application/json"})
	var m map[string]any
	if err := json.Unmarshal(r.Body, &m); err != nil || r.Status != 200 {
		t.Fatalf("GET json: %d %s", r.Status, r.Body)
	}
	for _, k := range []string{"id", "name", "department", "position", "email", "salary", "version", "updated_at", "updated_by"} {
		if _, ok := m[k]; !ok {
			t.Errorf("json field %q missing: %s", k, r.Body)
		}
	}
	if len(m) != 9 {
		t.Errorf("json has %d fields, the contract has 9: %s", len(m), r.Body)
	}
	if m["updated_by"] != "src-json@test" {
		t.Errorf("updated_by must come from the From header: %v", m["updated_by"])
	}
	if !strings.Contains(r.Header.Get("Cache-Control"), "max-age=") || r.Header.Get("Vary") != "Accept" || r.Header.Get("Last-Modified") == "" {
		t.Errorf("GET cache headers: %v", r.Header)
	}

	// GET XML of the same resource
	r = testutil.Raw(t, base, "GET", "/employees/e-1", "", map[string]string{"Accept": "application/xml"})
	var x struct {
		XMLName xml.Name `xml:"employee"`
		ID      string   `xml:"id,attr"`
		Name    string   `xml:"name"`
		Salary  int64    `xml:"salary"`
		Version int64    `xml:"version"`
	}
	if err := xml.Unmarshal(r.Body, &x); err != nil || x.ID != "e-1" || x.Salary != 12000 || x.Version != 1 {
		t.Fatalf("GET xml: %v %s", err, r.Body)
	}
	if r.Header.Get("ETag") != `"v1-xml"` {
		t.Errorf("xml etag: %s", r.Header.Get("ETag"))
	}
	// ?format= wins over Accept
	r = testutil.Raw(t, base, "GET", "/employees/e-1?format=xml", "", map[string]string{"Accept": "application/json"})
	if !strings.HasPrefix(string(r.Body), "<?xml") {
		t.Fatalf("format=xml: %s", r.Body)
	}

	// conditional GET
	r = testutil.Raw(t, base, "GET", "/employees/e-1", "", map[string]string{"If-None-Match": `"v1-json"`})
	if r.Status != 304 || len(r.Body) != 0 {
		t.Fatalf("If-None-Match: %d", r.Status)
	}

	// errors, in the requested format
	r = testutil.Raw(t, base, "GET", "/employees/missing", "", map[string]string{"Accept": "application/xml"})
	if r.Status != 404 || !strings.Contains(string(r.Body), "<code>not_found</code>") {
		t.Fatalf("404 xml: %d %s", r.Status, r.Body)
	}
	r = testutil.Raw(t, base, "GET", "/employees/missing", "", nil)
	var e model.Error
	if json.Unmarshal(r.Body, &e) != nil || e.Code != "not_found" || e.CorrelationID == "" {
		t.Fatalf("404 json: %s", r.Body)
	}
	for _, c := range []struct {
		method, path, body string
		hdr                map[string]string
		status             int
		code               string
	}{
		{"GET", "/employees/e-1", "", map[string]string{"Accept": "text/html"}, 406, "not_acceptable"},
		{"PUT", "/employees/e-9", "name: x", map[string]string{"Content-Type": "text/yaml"}, 415, "unsupported_media_type"},
		{"PUT", "/employees/e-9", `{"id":"e-9"}`, J, 400, "invalid"},
		{"PUT", "/employees/e-9", `{"id":"other","name":"x"}`, J, 400, "invalid"},
		{"PUT", "/employees/e-9", `{"name":"x","unknown":1}`, J, 400, "invalid"},
		{"PUT", "/employees/e-1", empJSON, map[string]string{"Content-Type": "application/json", "If-Match": `"v7-json"`}, 412, "version_mismatch"},
		{"GET", "/employees?limit=500", "", nil, 400, "invalid"},
		{"POST", "/employees/missing", `{"salary":1}`, J, 404, "not_found"},
		{"POST", "/employees/e-1", `{}`, J, 400, "invalid"},
		{"GET", "/updates/employees?since=yesterday", "", nil, 400, "invalid"},
	} {
		r := testutil.Raw(t, base, c.method, c.path, c.body, c.hdr)
		var e model.Error
		_ = json.Unmarshal(r.Body, &e)
		if r.Status != c.status || e.Code != c.code {
			t.Errorf("%s %s: got %d %q, want %d %q (%s)", c.method, c.path, r.Status, e.Code, c.status, c.code, r.Body)
		}
	}

	// POST: a client modification, with Idempotency-Key
	h := map[string]string{"Content-Type": "application/json", "Idempotency-Key": "k-" + base[len(base)-5:], "From": "client@test"}
	r1 := testutil.Raw(t, base, "POST", "/employees/e-1", `{"salary":15000}`, h)
	r2 := testutil.Raw(t, base, "POST", "/employees/e-1", `{"salary":15000}`, h)
	if r1.Status != 200 || testutil.Employee(t, r1).Version != 2 || testutil.Employee(t, r1).Salary != 15000 {
		t.Fatalf("POST: %d %s", r1.Status, r1.Body)
	}
	if r2.Header.Get("Idempotent-Replayed") != "true" || testutil.Employee(t, r2).Version != 2 {
		t.Fatalf("POST replay: %v %s", r2.Header, r2.Body)
	}
	r = testutil.Raw(t, base, "POST", "/employees/e-1", `{"salary":1}`, h)
	if r.Status != 422 {
		t.Fatalf("idempotency key reused for another body: %d", r.Status)
	}

	// list, with the aliases of the statement
	r = testutil.Raw(t, base, "GET", "/employees/?offset=1&limit=4", "", nil)
	var page model.EmployeeList
	if err := json.Unmarshal(r.Body, &page); err != nil || page.Total != 2 || page.Offset != 1 || len(page.Items) != 1 || page.Items[0].ID != "e-2" {
		t.Fatalf("list: %s", r.Body)
	}
	r = testutil.Raw(t, base, "GET", "/employees?format=xml", "", nil)
	if !strings.Contains(string(r.Body), `<employees offset="0" limit="10" total="2">`) {
		t.Fatalf("list xml: %s", r.Body)
	}
	r = testutil.Raw(t, base, "GET", "/employee/?id=e-2", "", nil)
	if r.Status != 200 || testutil.Employee(t, r).ID != "e-2" {
		t.Fatalf("GET /employee/?id=: %d %s", r.Status, r.Body)
	}
	r = testutil.Raw(t, base, "PUT", "/employee/", `{"id":"e-3","name":"Olga"}`, J)
	if r.Status != 201 {
		t.Fatalf("PUT /employee/ with the id in the body: %d %s", r.Status, r.Body)
	}

	// change feed: cursor, then If-Modified-Since
	r = testutil.Raw(t, base, "GET", "/updates/employees", "", nil)
	var feed model.ChangeFeed
	if err := json.Unmarshal(r.Body, &feed); err != nil || len(feed.Items) != 3 || feed.Cursor == "" ||
		r.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("feed: %s", r.Body)
	}
	r = testutil.Raw(t, base, "GET", "/update/employees/", "", map[string]string{
		"If-Modified-Since": time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)})
	if r.Status != 304 {
		t.Fatalf("feed with If-Modified-Since in the future: %d %s", r.Status, r.Body)
	}
}

// The proxy adds its own headers and never changes the warehouse answer.
func TestProxyHeaders(t *testing.T) {
	c := testutil.Start(t, testutil.Options{Replicas: 2})
	c.Seed(t, "p-1", 10)
	r := testutil.Raw(t, c.Server.URL, "GET", "/employees/p-1", "", map[string]string{"X-Correlation-ID": "contract-1"})
	if r.Header.Get("X-Correlation-ID") != "contract-1" || r.Header.Get("X-Cache") != "MISS" ||
		r.Header.Get("X-Upstream") == "" || r.Header.Get("Via") != "1.1 pad-proxy" || r.Header.Get("X-Served-By") == "" {
		t.Fatalf("proxy headers: %v", r.Header)
	}
	if len(r.Header.Values("X-Correlation-ID")) != 1 {
		t.Fatalf("duplicated correlation header: %v", r.Header.Values("X-Correlation-ID"))
	}
	r = testutil.Raw(t, c.Server.URL, "GET", "/employees/p-1", "", nil)
	if r.Header.Get("X-Cache") != "HIT" || r.Header.Get("Age") == "" {
		t.Fatalf("second GET must be a HIT: %v", r.Header)
	}
}
