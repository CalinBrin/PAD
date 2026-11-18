// Command client is a small CLI for the data consumers (through the proxy).
//
//	client [-url http://localhost:8080] [-format json|xml] [-cid id] <command>
//	  get <id>                      GET /employees/<id>
//	  list [offset] [limit]         GET /employees?offset=&limit=
//	  put <id> name=.. salary=..    PUT /employees/<id>
//	  post <id> field=value ...     POST /employees/<id> (patch, with an Idempotency-Key)
//	  updates [cursor]              GET /updates/employees?since=
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"pad-lab3/src/common/httpx"
	"pad-lab3/src/common/logx"
	"pad-lab3/src/common/model"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: client [-url URL] [-format json|xml] [-cid correlation-id] [-key idempotency-key]
  get <id> | list [offset] [limit] | put <id> k=v... | post <id> k=v... | updates [cursor]
  fields: name department position email salary`)
	os.Exit(2)
}

func fields(args []string) (map[string]string, error) {
	m := map[string]string{}
	for _, a := range args {
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			return nil, fmt.Errorf("expected field=value, got %q", a)
		}
		m[k] = v
	}
	return m, nil
}

func main() {
	base := flag.String("url", logx.Env("PROXY_URL", "http://localhost:8080"), "proxy URL")
	format := flag.String("format", "json", "json or xml")
	cid := flag.String("cid", "", "X-Correlation-ID")
	key := flag.String("key", "", "Idempotency-Key for post (default: random)")
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	f := httpx.JSON
	if *format == "xml" {
		f = httpx.XML
	}
	c := httpx.NewClient(*base, f, 10*time.Second)
	c.From = "cli@" + func() string { h, _ := os.Hostname(); return h }()
	ctx := context.Background()
	if *cid == "" {
		*cid = "cli-" + logx.NewID()[:8]
	}
	ctx = logx.WithCorrelationID(ctx, *cid)

	var resp *httpx.Response
	var err error
	switch args[0] {
	case "get":
		if len(args) != 2 {
			usage()
		}
		resp, err = c.Do(ctx, http.MethodGet, "/employees/"+args[1], nil, nil)
	case "list":
		q := "/employees"
		if len(args) > 1 {
			q += "?offset=" + args[1]
			if len(args) > 2 {
				q += "&limit=" + args[2]
			}
		}
		resp, err = c.Do(ctx, http.MethodGet, q, nil, nil)
	case "put":
		if len(args) < 2 {
			usage()
		}
		m, ferr := fields(args[2:])
		if ferr != nil {
			fmt.Fprintln(os.Stderr, ferr)
			os.Exit(2)
		}
		e := model.Employee{ID: args[1], Name: m["name"], Department: m["department"], Position: m["position"], Email: m["email"]}
		e.Salary, _ = strconv.ParseInt(m["salary"], 10, 64)
		resp, err = c.Do(ctx, http.MethodPut, "/employees/"+args[1], e, nil)
	case "post":
		if len(args) < 3 {
			usage()
		}
		m, ferr := fields(args[2:])
		if ferr != nil {
			fmt.Fprintln(os.Stderr, ferr)
			os.Exit(2)
		}
		var p model.EmployeePatch
		for k, v := range m {
			v := v
			switch k {
			case "name":
				p.Name = &v
			case "department":
				p.Department = &v
			case "position":
				p.Position = &v
			case "email":
				p.Email = &v
			case "salary":
				n, perr := strconv.ParseInt(v, 10, 64)
				if perr != nil {
					fmt.Fprintln(os.Stderr, "salary must be an integer")
					os.Exit(2)
				}
				p.Salary = &n
			default:
				fmt.Fprintln(os.Stderr, "unknown field", k)
				os.Exit(2)
			}
		}
		if *key == "" {
			*key = "cli-" + logx.NewID()
		}
		resp, err = c.Do(ctx, http.MethodPost, "/employees/"+args[1], p, map[string]string{"Idempotency-Key": *key})
	case "updates":
		q := "/updates/employees"
		if len(args) > 1 {
			q += "?since=" + args[1]
		}
		resp, err = c.Do(ctx, http.MethodGet, q, nil, nil)
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Printf("HTTP %d\n", resp.Status)
	for _, h := range []string{"X-Correlation-ID", "X-Cache", "X-Upstream", "X-Served-By", "X-Attempts", "ETag", "Age", "X-Changed", "Idempotent-Replayed", "X-Outcome"} {
		if v := resp.Header.Get(h); v != "" {
			fmt.Printf("%s: %s\n", h, v)
		}
	}
	fmt.Println()
	fmt.Print(string(resp.Body))
	if resp.Status >= 400 {
		os.Exit(1)
	}
}
