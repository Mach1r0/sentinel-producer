package event

import (
	"encoding/json"
	"net"
	"testing"
)

func TestGenerateAuthFailed(t *testing.T) {
	securityEvent := generateAuthFailed()

	assertValidEvent(t, securityEvent)

	if securityEvent.EventType != "auth_failed" {
		t.Fatalf("expected event type auth_failed, got %q", securityEvent.EventType)
	}

	if securityEvent.Severity != "medium" && securityEvent.Severity != "high" {
		t.Fatalf("expected medium or high severity, got %q", securityEvent.Severity)
	}

	var raw struct {
		User     string `json:"user"`
		Attempts int    `json:"attempts"`
		Reason   string `json:"reason"`
	}

	if err := json.Unmarshal(securityEvent.Raw, &raw); err != nil {
		t.Fatalf("decode auth_failed raw data: %v", err)
	}

	if raw.User == "" {
		t.Error("expected a non-empty user")
	}

	if raw.Attempts < 1 || raw.Attempts > 5 {
		t.Errorf("expected attempts between 1 and 5, got %d", raw.Attempts)
	}

	if raw.Reason == "" {
		t.Error("expected a non-empty failure reason")
	}
}

func TestGenerateProcessCreated(t *testing.T) {
	securityEvent := generateProcessCreated()

	assertValidEvent(t, securityEvent)

	if securityEvent.EventType != "process_created" {
		t.Fatalf("expected event type process_created, got %q", securityEvent.EventType)
	}

	if securityEvent.Severity != "low" {
		t.Fatalf("expected low severity, got %q", securityEvent.Severity)
	}

	var raw struct {
		User        string `json:"user"`
		ProcessName string `json:"process_name"`
		PID         int    `json:"pid"`
		Command     string `json:"command"`
	}

	if err := json.Unmarshal(securityEvent.Raw, &raw); err != nil {
		t.Fatalf("decode process_created raw data: %v", err)
	}

	if raw.User == "" {
		t.Error("expected a non-empty user")
	}

	if raw.ProcessName == "" {
		t.Error("expected a non-empty process name")
	}

	if raw.PID < 100 || raw.PID > 30099 {
		t.Errorf("expected PID between 100 and 30099, got %d", raw.PID)
	}

	if raw.Command == "" {
		t.Error("expected a non-empty command")
	}
}

func TestGenerateReturnsSupportedEvent(t *testing.T) {
	generator := NewGenerator()

	for range 100 {
		securityEvent := generator.Generate()
		assertValidEvent(t, securityEvent)

		switch securityEvent.EventType {
		case "auth_failed", "process_created":
		default:
			t.Fatalf("unsupported event type %q", securityEvent.EventType)
		}
	}
}

func assertValidEvent(t *testing.T, securityEvent Event) {
	t.Helper()

	if securityEvent.Timestamp.IsZero() {
		t.Error("expected a non-zero timestamp")
	}

	if net.ParseIP(securityEvent.SourceIP) == nil {
		t.Errorf("expected a valid source IP, got %q", securityEvent.SourceIP)
	}

	if securityEvent.EventType == "" {
		t.Error("expected a non-empty event type")
	}

	if securityEvent.Severity == "" {
		t.Error("expected a non-empty severity")
	}

	if !json.Valid(securityEvent.Raw) {
		t.Errorf("expected valid raw JSON, got %q", securityEvent.Raw)
	}
}

func TestGenerateReturnsUniqueIDs(t *testing.T) {
	generator := NewGenerator()
	seen := make(map[string]struct{})

	for range 1000 {
		securityEvent := generator.Generate()

		if securityEvent.ID == "" {
			t.Error("expected a non-empty event ID")
		}

		if err := securityEvent.Validate(); err != nil {
			t.Errorf("expected a valid event: %v", err)
		}

		if _, exists := seen[securityEvent.ID]; exists {
			t.Fatalf("duplicate event ID %q", securityEvent.ID)
		}

		seen[securityEvent.ID] = struct{}{}
	}
}
