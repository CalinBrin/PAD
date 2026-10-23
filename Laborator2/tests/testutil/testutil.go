// Package testutil starts the real gRPC servers (broker, payment) on local
// TCP ports and offers small helpers shared by the test packages.
package testutil

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	brokerv1 "pad-lab2/gen/broker/v1"
	ordersv1 "pad-lab2/gen/orders/v1"
	paymentv1 "pad-lab2/gen/payment/v1"
	"pad-lab2/src/integration-service/broker"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/integration-service/rpcx"
	"pad-lab2/src/integration-service/server"
	"pad-lab2/src/payment-service/payment"
)

const Token = "test-token"

// FastConfig returns broker timings suitable for tests.
func FastConfig(dir string) broker.Config {
	return broker.Config{DataDir: dir, MaxAttempts: 3, AckTimeout: 300 * time.Millisecond, RetryBase: 10 * time.Millisecond}
}

// FastPolicy is a publish/charge policy with short timings.
var FastPolicy = rpcx.Policy{MaxAttempts: 4, BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond, PerAttemptTimeout: 300 * time.Millisecond}

// Broker is a broker served over gRPC on a local TCP port.
type Broker struct {
	*broker.Broker
	Addr string
	gs   *grpc.Server
	once sync.Once
}

// Stop shuts the broker down (like a process exit): sessions end, the log stays.
func (b *Broker) Stop() {
	b.once.Do(func() {
		b.Broker.Close()
		b.gs.Stop()
	})
}

// StartBroker starts a broker on addr ("127.0.0.1:0" for a free port).
func StartBroker(t testing.TB, cfg broker.Config, addr string, extra ...grpc.UnaryServerInterceptor) *Broker {
	t.Helper()
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatalf("broker.New: %v", err)
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		b.Close()
		t.Fatalf("listen: %v", err)
	}
	gs, _ := server.New(b, cfg.Logger, extra...)
	go gs.Serve(ln)
	tb := &Broker{Broker: b, Addr: ln.Addr().String(), gs: gs}
	t.Cleanup(tb.Stop)
	return tb
}

// FreeAddr returns a local address that is free right now.
func FreeAddr(t testing.TB) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

// Payment is the payment service served over gRPC on a local TCP port.
type Payment struct {
	*payment.Service
	Addr string
	gs   *grpc.Server
	once sync.Once
}

func (p *Payment) Stop() { p.once.Do(func() { p.gs.Stop(); p.Service.Close() }) }

func StartPayment(t testing.TB, cfg payment.Config, addr string) *Payment {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = Token
	}
	svc, err := payment.New(cfg)
	if err != nil {
		t.Fatalf("payment.New: %v", err)
	}
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := svc.GRPCServer()
	go gs.Serve(ln)
	p := &Payment{Service: svc, Addr: ln.Addr().String(), gs: gs}
	t.Cleanup(p.Stop)
	return p
}

// Dial opens a client connection (closed at test end).
func Dial(t testing.TB, addr string, extra ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()
	conn, err := client.Dial(addr, extra...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// PaymentClient returns an authenticated payment client.
func PaymentClient(t testing.TB, addr string) paymentv1.PaymentClient {
	return paymentv1.NewPaymentClient(Dial(t, addr, grpc.WithPerRPCCredentials(payment.TokenCredentials(Token))))
}

// Order returns a valid OrderCreated envelope with a fresh message ID.
func Order(t testing.TB, correlationID string, amountCents int64, currency string) *brokerv1.Envelope {
	t.Helper()
	id := rpcx.NewID()
	env, err := envelope.NewOrderCreated(id, correlationID, time.Now(), &ordersv1.OrderCreated{
		OrderId: "ord-" + id[:8], CustomerId: "c1", AmountCents: amountCents, Currency: currency})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// Publish publishes env with FastPolicy and fails the test on error.
func Publish(t testing.TB, conn grpc.ClientConnInterface, env *brokerv1.Envelope) *brokerv1.PublishResponse {
	t.Helper()
	res, err := client.NewPublisher(conn, FastPolicy, nil).Publish(context.Background(), envelope.TopicOrders, env)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	return res
}

// Subscribe starts a subscriber of the orders topic (stopped at test end).
func Subscribe(t testing.TB, conn grpc.ClientConnInterface, group, name string, h client.Handler) *client.Subscription {
	t.Helper()
	s := client.Start(context.Background(), conn, client.Options{Topic: envelope.TopicOrders, Group: group, Name: name}, h)
	t.Cleanup(s.Close)
	return s
}

// WaitFor polls cond until it is true or the timeout expires.
func WaitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", what)
}

// Idle reports whether a queue has nothing pending, in flight or retrying.
func Idle(b *Broker, queue string) bool {
	s := b.Stats()[queue]
	return s.Pending == 0 && s.Inflight == 0 && s.Retrying == 0
}

// Recorder collects delivered message IDs in a thread-safe way.
type Recorder struct {
	mu    sync.Mutex
	ids   []string
	count map[string]int
}

func NewRecorder() *Recorder { return &Recorder{count: map[string]int{}} }

func (r *Recorder) Add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
	r.count[id]++
}

func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ids)
}

func (r *Recorder) Distinct() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.count)
}

func (r *Recorder) Count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count[id]
}

func (r *Recorder) IDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

// Ack is a handler that records and acknowledges.
func (r *Recorder) Ack(ctx context.Context, d client.Delivery) error {
	r.Add(d.Message.MessageId)
	return nil
}
