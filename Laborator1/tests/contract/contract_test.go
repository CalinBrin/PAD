package contract

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	"pad-lab1/src/integration-service/protocol"
)

type schema struct {
	Required   []string                  `json:"required"`
	Properties map[string]map[string]any `json:"properties"`
}

func loadSchema(t *testing.T, path string) schema {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var s schema
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("%s is not valid JSON: %v", path, err)
	}
	return s
}

func keysOf(m map[string]any) []string {
	var k []string
	for key := range m {
		k = append(k, key)
	}
	sort.Strings(k)
	return k
}

// The serialized envelope must contain exactly the fields of the contract.
func TestEnvelopeMatchesContract(t *testing.T) {
	s := loadSchema(t, "../../contracts/message-envelope.v1.schema.json")
	m, err := protocol.NewMessage(protocol.TypeOrderCreated, "corr", protocol.OrderCreated{OrderID: "o", CustomerID: "c", Amount: 1})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(m)
	var doc map[string]any
	json.Unmarshal(raw, &doc)

	for _, req := range s.Required {
		if _, ok := doc[req]; !ok {
			t.Errorf("required field %q missing from serialized message", req)
		}
	}
	for k := range doc {
		if _, ok := s.Properties[k]; !ok {
			t.Errorf("field %q is not part of the contract", k)
		}
	}
	if v, _ := doc["schema_version"].(float64); int(v) != 1 {
		t.Errorf("schema_version must be 1, got %v", doc["schema_version"])
	}
}

func TestOrderCreatedPayloadMatchesContract(t *testing.T) {
	s := loadSchema(t, "../../contracts/order-created.v1.schema.json")
	raw, _ := json.Marshal(protocol.OrderCreated{OrderID: "o", CustomerID: "c", Amount: 1})
	var doc map[string]any
	json.Unmarshal(raw, &doc)
	for _, req := range s.Required {
		if _, ok := doc[req]; !ok {
			t.Errorf("required payload field %q missing", req)
		}
	}
	if len(doc) != len(s.Properties) {
		t.Errorf("payload fields %v differ from contract fields", keysOf(doc))
	}
}

// Golden test of the wire format: renaming a JSON field breaks every client.
func TestWireFramesGolden(t *testing.T) {
	pub, _ := json.Marshal(protocol.Frame{Type: protocol.FramePublish, Topic: "orders"})
	if string(pub) != `{"type":"publish","topic":"orders"}` {
		t.Errorf("publish frame changed: %s", pub)
	}
	del, _ := json.Marshal(protocol.Frame{Type: protocol.FrameDeliver, Attempt: 2, Redelivered: true})
	if string(del) != `{"type":"deliver","attempt":2,"redelivered":true}` {
		t.Errorf("deliver frame changed: %s", del)
	}
	ack, _ := json.Marshal(protocol.Frame{Type: protocol.FrameAck, ID: "m1"})
	if string(ack) != `{"type":"ack","id":"m1"}` {
		t.Errorf("ack frame changed: %s", ack)
	}
}

// Consumers must refuse a schema version they do not know.
func TestUnknownSchemaVersionIsRejected(t *testing.T) {
	m, _ := protocol.NewMessage("T", "c", map[string]int{})
	m.SchemaVersion = protocol.SchemaVersion + 1
	if m.Validate() == nil {
		t.Fatal("future schema version must be rejected")
	}
}
