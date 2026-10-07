// Command order-service is the producer. It runs an HTTP API, or, with -load,
// publishes N orders from P concurrent publishers and exits.
package main

import (
	"flag"
	"log/slog"
	"net/http"
	"os"
	"sync"

	"pad-lab1/src/api"
	"pad-lab1/src/order-service/service"
)

func main() {
	brokerAddr := flag.String("broker", "localhost:7000", "broker address")
	listen := flag.String("listen", ":8080", "HTTP listen address")
	load := flag.Int("load", 0, "if > 0: publish this many orders and exit")
	publishers := flag.Int("publishers", 3, "concurrent publishers in -load mode")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("component", "order-service")

	if *load > 0 {
		runLoad(*brokerAddr, *load, *publishers, log)
		return
	}
	svc := service.New(*brokerAddr, log)
	defer svc.Close()
	log.Info("order-service listening", "addr", *listen, "broker", *brokerAddr)
	if err := http.ListenAndServe(*listen, api.NewHandler(svc)); err != nil {
		log.Error("http server stopped", "error", err)
		os.Exit(1)
	}
}

// runLoad uses one service (= one broker connection) per publisher goroutine.
func runLoad(addr string, total, publishers int, log *slog.Logger) {
	var wg sync.WaitGroup
	jobs := make(chan int)
	failed := 0
	var mu sync.Mutex
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			svc := service.New(addr, log.With("publisher", p))
			defer svc.Close()
			for i := range jobs {
				if _, err := svc.CreateOrder("customer-"+string(rune('A'+i%26)), float64(10+i), ""); err != nil {
					mu.Lock()
					failed++
					mu.Unlock()
				}
			}
		}(p)
	}
	for i := 0; i < total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	log.Info("load finished", "published", total-failed, "failed", failed)
	if failed > 0 {
		os.Exit(1)
	}
}
