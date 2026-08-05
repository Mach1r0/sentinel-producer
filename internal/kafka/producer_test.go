package kafka

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
)

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
