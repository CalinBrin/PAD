// Package protocol defines the message envelope and the wire frames shared by
// the broker, the producer and the consumers. The wire format is one JSON
// object per line over TCP.
package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	// SchemaVersion is the current version of the message envelope + payload.
	SchemaVersion = 1

	TopicOrders      = "orders"
	TypeOrderCreated = "OrderCreated"
)

// Frame types.
const (
	FramePublish   = "publish"   // client -> broker
	FramePubAck    = "puback"    // broker -> client (message is persisted)
	FrameSubscribe = "subscribe" // client -> broker
	FrameSubAck    = "suback"    // broker -> client
	FrameDeliver   = "deliver"   // broker -> consumer
	FrameAck       = "ack"       // consumer -> broker (processing confirmed)
	FrameNack      = "nack"      // consumer -> broker (processing failed)
	FrameError     = "error"     // broker -> client
)

// Message is the envelope carried by the broker.
type Message struct {
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Timestamp     time.Time       `json:"timestamp"`
	CorrelationID string          `json:"correlation_id"`
	SchemaVersion int             `json:"schema_version"`
	Payload       json.RawMessage `json:"payload"`
}

// OrderCreated is the payload of Type == "OrderCreated", schema version 1.
type OrderCreated struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
}

// Frame is the single wire structure; unused fields are omitted.
type Frame struct {
	Type        string   `json:"type"`
	Topic       string   `json:"topic,omitempty"`
	Group       string   `json:"group,omitempty"`
	Name        string   `json:"name,omitempty"`
	Prefetch    int      `json:"prefetch,omitempty"`
	ID          string   `json:"id,omitempty"`
	Message     *Message `json:"message,omitempty"`
	Attempt     int      `json:"attempt,omitempty"`
	Redelivered bool     `json:"redelivered,omitempty"`
	Routed      int      `json:"routed,omitempty"`
	Reason      string   `json:"reason,omitempty"`
}

// NewID returns a random 128-bit hex identifier.
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err) // crypto/rand failing is unrecoverable
	}
	return hex.EncodeToString(b)
}

// NewMessage builds a valid envelope. An empty correlationID is replaced by a
// fresh one.
func NewMessage(msgType, correlationID string, payload any) (Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("marshal payload: %w", err)
	}
	if correlationID == "" {
		correlationID = NewID()
	}
	m := Message{
		ID:            NewID(),
		Type:          msgType,
		Timestamp:     time.Now().UTC(),
		CorrelationID: correlationID,
		SchemaVersion: SchemaVersion,
		Payload:       raw,
	}
	return m, m.Validate()
}

// Validate checks the mandatory envelope fields.
func (m Message) Validate() error {
	switch {
	case m.ID == "":
		return errors.New("message id is required")
	case m.Type == "":
		return errors.New("message type is required")
	case m.CorrelationID == "":
		return errors.New("correlation_id is required")
	case m.Timestamp.IsZero():
		return errors.New("timestamp is required")
	case m.SchemaVersion < 1 || m.SchemaVersion > SchemaVersion:
		return fmt.Errorf("unsupported schema_version %d", m.SchemaVersion)
	case !json.Valid(m.Payload):
		return errors.New("payload must be valid JSON")
	}
	return nil
}
