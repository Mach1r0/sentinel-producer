package event

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
