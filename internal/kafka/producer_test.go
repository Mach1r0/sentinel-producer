package kafka

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
	kafkago "github.com/segmentio/kafka-go"
)

func TestNewProducerRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "empty broker",
			config: Config{Topic: "security.events", BatchSize: 50},
		},
		{
			name:   "empty topic",
			config: Config{Broker: "localhost:9092", BatchSize: 50},
		},
		{
			name:   "zero batch size",
			config: Config{Broker: "localhost:9092", Topic: "security.events"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewProducer(test.config); err == nil {
				t.Fatal("expected an invalid producer configuration error")
			}
		})
	}
}

func TestRecordCompletionUpdatesMetrics(t *testing.T) {
	producer := &Producer{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	producer.recordCompletion(make([]kafkago.Message, 3), nil)
	producer.recordCompletion(
		make([]kafkago.Message, 2),
		errors.New("delivery failed"),
	)

	metrics := producer.metrics.snapshot()
	if metrics.Published != 3 {
		t.Errorf("expected 3 published messages, got %d", metrics.Published)
	}
	if metrics.Failed != 2 {
		t.Errorf("expected 2 failed messages, got %d", metrics.Failed)
	}
	if metrics.Batches != 2 {
		t.Errorf("expected 2 batches, got %d", metrics.Batches)
	}
}

func TestNewMessage(t *testing.T) {
	timestamp := time.Date(2026, time.August, 5, 12, 30, 0, 0, time.UTC)
	securityEvent := event.Event{
		Timestamp: timestamp,
		EventType: "auth_failed",
		SourceIP:  "10.0.0.15",
		Severity:  "high",
		Raw:       json.RawMessage("{\"user\":\"admin\",\"attempts\":5}"),
	}

	message, err := newMessage(securityEvent)
	if err != nil {
		t.Fatalf("newMessage returned an unexpected error: %v", err)
	}

	if got := string(message.Key); got != securityEvent.SourceIP {
		t.Errorf("expected Kafka key %q, got %q", securityEvent.SourceIP, got)
	}

	if !message.Time.Equal(timestamp) {
		t.Errorf("expected timestamp %s, got %s", timestamp, message.Time)
	}

	if !json.Valid(message.Value) {
		t.Fatalf("expected valid message JSON, got %q", message.Value)
	}

	var decoded event.Event
	if err := json.Unmarshal(message.Value, &decoded); err != nil {
		t.Fatalf("decode Kafka message: %v", err)
	}

	if decoded.EventType != securityEvent.EventType {
		t.Errorf("expected event type %q, got %q", securityEvent.EventType, decoded.EventType)
	}

	if decoded.SourceIP != securityEvent.SourceIP {
		t.Errorf("expected source IP %q, got %q", securityEvent.SourceIP, decoded.SourceIP)
	}

	if decoded.Severity != securityEvent.Severity {
		t.Errorf("expected severity %q, got %q", securityEvent.Severity, decoded.Severity)
	}

	if !json.Valid(decoded.Raw) {
		t.Errorf("expected valid decoded raw JSON, got %q", decoded.Raw)
	}
}

func TestNewMessageRejectsInvalidRawJSON(t *testing.T) {
	securityEvent := event.Event{
		Timestamp: time.Now().UTC(),
		EventType: "auth_failed",
		SourceIP:  "10.0.0.15",
		Severity:  "high",
		Raw:       json.RawMessage("{\"invalid\""),
	}

	if _, err := newMessage(securityEvent); err == nil {
		t.Fatal("expected an error for invalid raw JSON")
	}
}
