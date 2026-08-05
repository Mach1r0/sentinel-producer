package event

import (
	"encoding/json"
	"time"
)

type Event struct {
	Timestamp time.Time       `json:"timestamp"`
	EventType string          `json:"event_type"`
	SourceIP  string          `json:"source_ip"`
	Severity  string          `json:"severity"`
	Raw       json.RawMessage `json:"raw"`
}
