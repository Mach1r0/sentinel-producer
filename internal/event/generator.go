package event

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
)

var (
	sourceIPs = []string{
		"10.0.0.10",
		"10.0.0.11",
		"10.0.0.12",
		"192.168.1.20",
		"192.168.1.21",
	}

	users = []string{
		"admin",
		"alice",
		"bob",
		"service-account",
	}

	processes = []string{
		"bash",
		"curl",
		"python",
		"ssh",
		"nmap",
	}
)

type Generator struct{}

func NewGenerator() *Generator {
	return &Generator{}
}

func (g *Generator) Generate() Event {
	if rand.IntN(2) == 0 {
		return generateAuthFailed()
	}

	return generateProcessCreated()
}

func generateAuthFailed() Event {
	attempts := rand.IntN(5) + 1

	raw := mustMarshalRaw(map[string]any{
		"user":     randomValue(users),
		"attempts": attempts,
		"reason":   "invalid credentials",
	})

	severity := "medium"
	if attempts >= 4 {
		severity = "high"
	}

	return Event{
		Timestamp: time.Now().UTC(),
		EventType: "auth_failed",
		SourceIP:  randomValue(sourceIPs),
		Severity:  severity,
		Raw:       raw,
	}
}

func generateProcessCreated() Event {
	process := randomValue(processes)

	raw := mustMarshalRaw(map[string]any{
		"user":         randomValue(users),
		"process_name": process,
		"pid":          rand.IntN(30000) + 100,
		"command":      fmt.Sprintf("/usr/bin/%s", process),
	})

	return Event{
		Timestamp: time.Now().UTC(),
		EventType: "process_created",
		SourceIP:  randomValue(sourceIPs),
		Severity:  "low",
		Raw:       raw,
	}
}

func randomValue(values []string) string {
	return values[rand.IntN(len(values))]
}

func mustMarshalRaw(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("marshal generated event data: %v", err))
	}

	return raw
}
