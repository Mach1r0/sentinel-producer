package event

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID        string          `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	EventType string          `json:"event_type"`
	SourceIP  string          `json:"source_ip"`
	Severity  string          `json:"severity"`
	Raw       json.RawMessage `json:"raw"`
}

func newEventID() string {
	value := make([]byte, 16)

	if _, err := rand.Read(value); err != nil {
		panic(fmt.Sprintf("generate event ID: %v", err))
	}

	return hex.EncodeToString(value)
}

return Event{
	ID:        newEventID(),
	Timestamp: time.Now().UTC(),
	EventType: "process_created",
	SourceIP:  randomValue(sourceIPs),
	Severity:  "low",
	Raw:       raw,
}