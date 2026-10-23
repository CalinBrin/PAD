// Package envelope builds, validates and decodes the messages exchanged through
// the broker. It is the code side of contracts/proto and contracts/versioning.md.
package envelope

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	brokerv1 "pad-lab2/gen/broker/v1"
	ordersv1 "pad-lab2/gen/orders/v1"
)

const (
	TopicOrders = "orders"

	TypeOrderCreated = "orders.v1.OrderCreated"
	// OrderCreatedSchema is the schema version this code writes.
	// 1: order_id, customer_id, amount_cents. 2: + currency.
	OrderCreatedSchema = 2
	// OrderCreatedMinSchema is the oldest version this code still reads.
	OrderCreatedMinSchema = 1

	DefaultCurrency = "MDL"

	TimeLayout = "2006-01-02T15:04:05.000Z"
)

// ErrUnsupported marks a message this code cannot process, whatever the number
// of retries: unknown type, unknown schema version or unreadable payload.
// Consumers treat it as a permanent error (straight to the DLQ).
var ErrUnsupported = errors.New("unsupported message")

// DeriveID returns a stable identifier derived from a request key: the same
// request always produces the same message ID, so a retried publish is
// recognised as a duplicate by the broker.
func DeriveID(namespace, key string) string {
	sum := sha256.Sum256([]byte(namespace + "\x00" + key))
	return hex.EncodeToString(sum[:16])
}

// FormatTime returns t in the contract format (RFC 3339, UTC, milliseconds).
func FormatTime(t time.Time) string { return t.UTC().Format(TimeLayout) }

// NewOrderCreated builds the envelope of an OrderCreated event. The payload is
// encoded deterministically, so the same order always gives the same bytes.
func NewOrderCreated(messageID, correlationID string, occurredAt time.Time, o *ordersv1.OrderCreated) (*brokerv1.Envelope, error) {
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(o)
	if err != nil {
		return nil, err
	}
	env := &brokerv1.Envelope{
		MessageId:     messageID,
		CorrelationId: correlationID,
		MessageType:   TypeOrderCreated,
		SchemaVersion: OrderCreatedSchema,
		OccurredAt:    FormatTime(occurredAt),
		Payload:       payload,
	}
	return env, Validate(env)
}

// Validate checks the mandatory envelope fields.
func Validate(e *brokerv1.Envelope) error {
	switch {
	case e == nil:
		return errors.New("message is required")
	case strings.TrimSpace(e.MessageId) == "":
		return errors.New("message_id is required")
	case len(e.MessageId) > 128:
		return errors.New("message_id is longer than 128 characters")
	case strings.TrimSpace(e.CorrelationId) == "":
		return errors.New("correlation_id is required")
	case e.MessageType == "":
		return errors.New("message_type is required")
	case e.SchemaVersion < 1:
		return errors.New("schema_version must be >= 1")
	}
	t, err := time.Parse(time.RFC3339Nano, e.OccurredAt)
	if err != nil {
		return fmt.Errorf("occurred_at %q is not RFC 3339", e.OccurredAt)
	}
	if _, off := t.Zone(); off != 0 || !strings.HasSuffix(e.OccurredAt, "Z") {
		return fmt.Errorf("occurred_at %q must be in UTC (Z suffix)", e.OccurredAt)
	}
	return nil
}

// DecodeOrderCreated reads the payload of an OrderCreated envelope written with
// any supported schema version. A v1 payload has no currency: DefaultCurrency.
// Unknown fields written by a newer producer are ignored (forward compatible).
func DecodeOrderCreated(e *brokerv1.Envelope) (*ordersv1.OrderCreated, error) {
	if e.MessageType != TypeOrderCreated {
		return nil, fmt.Errorf("%w: message_type %q", ErrUnsupported, e.MessageType)
	}
	if e.SchemaVersion < OrderCreatedMinSchema || e.SchemaVersion > OrderCreatedSchema {
		return nil, fmt.Errorf("%w: %s schema_version %d (supported %d..%d)", ErrUnsupported, e.MessageType, e.SchemaVersion, OrderCreatedMinSchema, OrderCreatedSchema)
	}
	var o ordersv1.OrderCreated
	if err := proto.Unmarshal(e.Payload, &o); err != nil {
		return nil, fmt.Errorf("%w: payload: %v", ErrUnsupported, err)
	}
	if o.OrderId == "" {
		return nil, fmt.Errorf("%w: order_id is empty", ErrUnsupported)
	}
	if e.SchemaVersion < 2 || o.Currency == "" {
		o.Currency = DefaultCurrency
	}
	return &o, nil
}
