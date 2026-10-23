package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	brokerv1 "pad-lab2/gen/broker/v1"
	ordersv1 "pad-lab2/gen/orders/v1"
	paymentv1 "pad-lab2/gen/payment/v1"
	"pad-lab2/src/api"
	"pad-lab2/src/integration-service/client"
	"pad-lab2/src/integration-service/envelope"
	"pad-lab2/src/order-service/service"
	"pad-lab2/src/payment-service/payment"
	"pad-lab2/tests/testutil"
)

// Golden table of the wire contract: renumbering or retyping a field breaks
// every deployed client, so such a change must fail this test (and go to v2).
func TestProtoFieldNumbersAndTypesAreStable(t *testing.T) {
	golden := map[protoreflect.Message]map[string]string{
		(&brokerv1.Envelope{}).ProtoReflect(): {"message_id": "1 string", "correlation_id": "2 string", "message_type": "3 string",
			"schema_version": "4 int32", "occurred_at": "5 string", "payload": "6 bytes"},
		(&brokerv1.PublishRequest{}).ProtoReflect():  {"topic": "1 string", "message": "2 message"},
		(&brokerv1.PublishResponse{}).ProtoReflect(): {"message_id": "1 string", "routed_queues": "2 int32", "duplicate": "3 bool"},
		(&brokerv1.ConsumeRequest{}).ProtoReflect():  {"subscribe": "1 message", "ack": "2 message", "nack": "3 message"},
		(&brokerv1.Subscribe{}).ProtoReflect():       {"topic": "1 string", "group": "2 string", "consumer_name": "3 string", "prefetch": "4 int32"},
		(&brokerv1.Ack{}).ProtoReflect():             {"delivery_tag": "1 uint64"},
		(&brokerv1.Nack{}).ProtoReflect():            {"delivery_tag": "1 uint64", "reason": "2 string", "permanent": "3 bool"},
		(&brokerv1.Delivery{}).ProtoReflect():        {"delivery_tag": "1 uint64", "message": "2 message", "attempt": "3 int32", "redelivered": "4 bool"},
		(&ordersv1.OrderCreated{}).ProtoReflect():    {"order_id": "1 string", "customer_id": "2 string", "amount_cents": "3 int64", "currency": "4 string"},
		(&paymentv1.ChargeRequest{}).ProtoReflect():  {"idempotency_key": "1 string", "order_id": "2 string", "amount_cents": "3 int64", "currency": "4 string"},
		(&paymentv1.ChargeResponse{}).ProtoReflect(): {"charge_id": "1 string", "status": "2 enum", "replayed": "3 bool", "decline_reason": "4 string"},
	}
	for m, fields := range golden {
		d := m.Descriptor()
		if d.Fields().Len() != len(fields) {
			t.Errorf("%s: %d fields, contract lists %d", d.FullName(), d.Fields().Len(), len(fields))
		}
		for name, want := range fields {
			f := d.Fields().ByName(protoreflect.Name(name))
			if f == nil {
				t.Errorf("%s.%s removed", d.FullName(), name)
				continue
			}
			if got := fmt.Sprintf("%d %s", f.Number(), f.Kind()); got != want {
				t.Errorf("%s.%s is %q, contract says %q", d.FullName(), name, got, want)
			}
		}
	}
	if paymentv1.ChargeStatus_CHARGE_STATUS_APPROVED != 1 || paymentv1.ChargeStatus_CHARGE_STATUS_DECLINED != 2 {
		t.Error("ChargeStatus values changed")
	}
}

// orderCreatedV1 is the schema_version 1 descriptor of OrderCreated (no
// currency field): what an old consumer, built before v2, knows.
func orderCreatedV1(t *testing.T) protoreflect.MessageDescriptor {
	str, i64 := descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_TYPE_INT64
	opt := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	field := func(name string, n int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: proto.String(name), Number: proto.Int32(n), Type: typ.Enum(), Label: opt.Enum(), JsonName: proto.String(name)}
	}
	fd, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("old/orders_v1_schema1.proto"), Package: proto.String("old"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("OrderCreated"), Field: []*descriptorpb.FieldDescriptorProto{
			field("order_id", 1, str), field("customer_id", 2, str), field("amount_cents", 3, i64)}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return fd.Messages().Get(0)
}

// Forward compatibility: a payload written by the current producer (v2, with
// currency) is read without error by a consumer that only knows v1.
func TestNewerPayloadIsReadByOlderConsumer(t *testing.T) {
	b, _ := proto.Marshal(&ordersv1.OrderCreated{OrderId: "o1", CustomerId: "c1", AmountCents: 250, Currency: "EUR"})
	old := dynamicpb.NewMessage(orderCreatedV1(t))
	if err := proto.Unmarshal(b, old); err != nil {
		t.Fatalf("old consumer cannot read the new payload: %v", err)
	}
	d := old.Descriptor()
	if old.Get(d.Fields().ByName("order_id")).String() != "o1" || old.Get(d.Fields().ByName("amount_cents")).Int() != 250 {
		t.Fatal("known fields must keep their values")
	}
	if len(old.GetUnknown()) == 0 {
		t.Fatal("the new field must be kept as unknown, not rejected")
	}
}

// Backward compatibility: a v1 payload (no currency) is read by the current
// consumer, which applies the documented default.
func TestOlderPayloadIsReadByNewerConsumer(t *testing.T) {
	old := dynamicpb.NewMessage(orderCreatedV1(t))
	d := old.Descriptor()
	old.Set(d.Fields().ByName("order_id"), protoreflect.ValueOfString("o1"))
	old.Set(d.Fields().ByName("customer_id"), protoreflect.ValueOfString("c1"))
	old.Set(d.Fields().ByName("amount_cents"), protoreflect.ValueOfInt64(99))
	b, _ := proto.Marshal(old)
	o, err := envelope.DecodeOrderCreated(&brokerv1.Envelope{MessageType: envelope.TypeOrderCreated, SchemaVersion: 1, Payload: b})
	if err != nil {
		t.Fatal(err)
	}
	if o.OrderId != "o1" || o.AmountCents != 99 || o.Currency != envelope.DefaultCurrency {
		t.Fatalf("unexpected decode of a v1 payload: %+v", o)
	}
}

// A schema version newer than the consumer supports is a permanent error.
func TestUnsupportedSchemaVersionIsAPermanentError(t *testing.T) {
	for _, v := range []int32{0, envelope.OrderCreatedSchema + 1} {
		_, err := envelope.DecodeOrderCreated(&brokerv1.Envelope{MessageType: envelope.TypeOrderCreated, SchemaVersion: v})
		if !errors.Is(err, envelope.ErrUnsupported) {
			t.Errorf("schema_version %d: want ErrUnsupported, got %v", v, err)
		}
	}
	if _, err := envelope.DecodeOrderCreated(&brokerv1.Envelope{MessageType: "orders.v1.Unknown", SchemaVersion: 1}); !errors.Is(err, envelope.ErrUnsupported) {
		t.Error("an unknown message_type must be unsupported")
	}
}

// Error semantics of Publish (contracts/errors.md), over a real gRPC server.
func TestPublishErrorSemantics(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	c := brokerv1.NewBrokerClient(testutil.Dial(t, b.Addr))
	ctx := context.Background()

	bad := testutil.Order(t, "c", 1, "MDL")
	bad.CorrelationId = ""
	if _, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: bad}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("invalid envelope: want InvalidArgument, got %v", err)
	}
	if _, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: testutil.Order(t, "c", 1, "MDL")}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("topic without queue: want FailedPrecondition, got %v", err)
	}
	b.Declare(envelope.TopicOrders, "inventory")
	m := testutil.Order(t, "c", 1, "MDL")
	if r, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: m}); err != nil || r.Duplicate || r.RoutedQueues != 1 {
		t.Fatalf("first publish: %+v %v", r, err)
	}
	if r, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: m}); err != nil || !r.Duplicate || r.RoutedQueues != 1 {
		t.Errorf("same message again: want duplicate=true, got %+v %v", r, err)
	}
	changed := proto.Clone(m).(*brokerv1.Envelope)
	changed.Payload = append([]byte(nil), changed.Payload...)
	changed.Payload[len(changed.Payload)-1] ^= 1
	if _, err := c.Publish(ctx, &brokerv1.PublishRequest{Topic: envelope.TopicOrders, Message: changed}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("same id, other content: want AlreadyExists, got %v", err)
	}
	big, _ := c.Consume(ctx)
	big.Send(&brokerv1.ConsumeRequest{Request: &brokerv1.ConsumeRequest_Subscribe{Subscribe: &brokerv1.Subscribe{Topic: "orders", Group: "g", Prefetch: 5000}}})
	if _, err := big.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("prefetch over the limit: want InvalidArgument, got %v", err)
	}
	stream, _ := c.Consume(ctx)
	stream.Send(&brokerv1.ConsumeRequest{Request: &brokerv1.ConsumeRequest_Ack{Ack: &brokerv1.Ack{DeliveryTag: 1}}})
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Errorf("consume without subscribe: want InvalidArgument, got %v", err)
	}
}

// Error semantics of the payment service.
func TestChargeErrorSemantics(t *testing.T) {
	p := testutil.StartPayment(t, payment.Config{DataDir: t.TempDir(), LimitCents: 1000}, "")
	c := testutil.PaymentClient(t, p.Addr)
	ctx := context.Background()
	req := &paymentv1.ChargeRequest{IdempotencyKey: "k1", OrderId: "o1", AmountCents: 500, Currency: "MDL"}

	if _, err := c.GetCharge(ctx, &paymentv1.GetChargeRequest{IdempotencyKey: "k1"}); status.Code(err) != codes.NotFound {
		t.Errorf("GetCharge before Charge: want NotFound, got %v", err)
	}
	r1, err := c.Charge(ctx, req)
	if err != nil || r1.Status != paymentv1.ChargeStatus_CHARGE_STATUS_APPROVED || r1.Replayed {
		t.Fatalf("first charge: %+v %v", r1, err)
	}
	r2, err := c.Charge(ctx, req)
	if err != nil || !r2.Replayed || r2.ChargeId != r1.ChargeId {
		t.Errorf("repeated charge must replay the first result, got %+v %v", r2, err)
	}
	if g, err := c.GetCharge(ctx, &paymentv1.GetChargeRequest{IdempotencyKey: "k1"}); err != nil || g.ChargeId != r1.ChargeId {
		t.Errorf("GetCharge after Charge: %+v %v", g, err)
	}
	other := proto.Clone(req).(*paymentv1.ChargeRequest)
	other.AmountCents = 600
	if _, err := c.Charge(ctx, other); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("key reused with other amount: want FailedPrecondition, got %v", err)
	}
	if _, err := c.Charge(ctx, &paymentv1.ChargeRequest{IdempotencyKey: "k2", OrderId: "o2", AmountCents: 0, Currency: "MDL"}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("amount 0: want InvalidArgument, got %v", err)
	}
	if r, err := c.Charge(ctx, &paymentv1.ChargeRequest{IdempotencyKey: "k3", OrderId: "o3", AmountCents: 5000, Currency: "MDL"}); err != nil || r.Status != paymentv1.ChargeStatus_CHARGE_STATUS_DECLINED {
		t.Errorf("over the limit: want a DECLINED result (not an error), got %+v %v", r, err)
	}
	anon := paymentv1.NewPaymentClient(testutil.Dial(t, p.Addr))
	if _, err := anon.Charge(ctx, req); status.Code(err) != codes.Unauthenticated {
		t.Errorf("no token: want Unauthenticated, got %v", err)
	}
	if n := len(p.Charges()); n != 2 {
		t.Errorf("exactly 2 charges must have been executed (k1, k3), got %d", n)
	}
}

// HTTP API contract (contracts/http-api.md).
func TestHTTPAPIContract(t *testing.T) {
	b := testutil.StartBroker(t, testutil.FastConfig(t.TempDir()), "")
	b.Declare(envelope.TopicOrders, "inventory")
	svc, err := service.New(t.TempDir(), client.NewPublisher(testutil.Dial(t, b.Addr), testutil.FastPolicy, nil), nil)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(api.NewHandler(svc, func() string { return "ready" }, nil))
	defer srv.Close()

	post := func(key, body string) (*http.Response, map[string]any) {
		req, _ := http.NewRequest("POST", srv.URL+"/orders", bytes.NewBufferString(body))
		if key != "" {
			req.Header.Set(api.IdempotencyHeader, key)
		}
		req.Header.Set(api.CorrelationHeader, "corr-contract")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var out map[string]any
		json.NewDecoder(res.Body).Decode(&out)
		return res, out
	}
	if res, _ := post("", `{"customer_id":"c1","amount_cents":100}`); res.StatusCode != 400 {
		t.Errorf("missing Idempotency-Key: want 400, got %d", res.StatusCode)
	}
	if res, _ := post("k1", `{"customer_id":"c1","amount_cents":0}`); res.StatusCode != 400 {
		t.Errorf("amount 0: want 400, got %d", res.StatusCode)
	}
	if res, _ := post("k1", `{"customer_id":"c1","amount_cents":10,"currency":"eur"}`); res.StatusCode != 400 {
		t.Errorf("bad currency: want 400, got %d", res.StatusCode)
	}
	res, body := post("k1", `{"customer_id":"c1","amount_cents":100,"currency":"EUR","future_field":true}`)
	if res.StatusCode != 202 || body["status"] != "published" || body["order_id"] == "" || res.Header.Get(api.CorrelationHeader) != "corr-contract" {
		t.Fatalf("create: want 202 published with correlation header, got %d %v", res.StatusCode, body)
	}
	res2, body2 := post("k1", `{"customer_id":"c1","amount_cents":100,"currency":"EUR"}`)
	if res2.StatusCode != 200 || res2.Header.Get(api.ReplayedHeader) != "true" || body2["order_id"] != body["order_id"] || body2["message_id"] != body["message_id"] {
		t.Errorf("repeat: want 200 + Idempotent-Replayed and the same order, got %d %v", res2.StatusCode, body2)
	}
	if res, _ := post("k1", `{"customer_id":"c1","amount_cents":999}`); res.StatusCode != 409 {
		t.Errorf("key reused with another body: want 409, got %d", res.StatusCode)
	}
	get, err := http.Get(srv.URL + "/orders/" + body["order_id"].(string))
	if err != nil || get.StatusCode != 200 {
		t.Errorf("GET /orders/{id}: %v %v", get, err)
	}
	if get, _ := http.Get(srv.URL + "/orders/nope"); get.StatusCode != 404 {
		t.Errorf("unknown order: want 404, got %d", get.StatusCode)
	}
	if s := b.Stats()["orders.inventory"]; s.Pending != 1 {
		t.Errorf("exactly one event must be in the queue, got %+v", s)
	}
}
