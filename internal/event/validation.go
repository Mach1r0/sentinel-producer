package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

func (e Event) Validate() error {
	if e.ID == "" {
		return errors.New("event ID is required")
	}

	if e.Timestamp.IsZero() {
		return errors.New("timestamp is required")
	}

	switch e.EventType {
	case "auth_failed", "process_created":
	default:
		return fmt.Errorf("unsupported event type %q", e.EventType)
	}

	if net.ParseIP(e.SourceIP) == nil {
		return fmt.Errorf("invalid source IP %q", e.SourceIP)
	}

	switch e.Severity {
	case "low", "medium", "high", "critical":
	default:
		return fmt.Errorf("invalid severity %q", e.Severity)
	}

	if !json.Valid(e.Raw) {
		return errors.New("raw must contain valid JSON")
	}

	return nil
}
