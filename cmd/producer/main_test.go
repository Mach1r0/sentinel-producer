package main

import "testing"

func TestParseConfigDefaults(t *testing.T) {
	cfg, err := parseConfig(nil, emptyEnvironment)
	if err != nil {
		t.Fatalf("parseConfig returned an unexpected error: %v", err)
	}

	if cfg.broker != defaultBroker {
		t.Errorf("expected broker %q, got %q", defaultBroker, cfg.broker)
	}
	if cfg.topic != defaultTopic {
		t.Errorf("expected topic %q, got %q", defaultTopic, cfg.topic)
	}
	if cfg.rate != defaultRate {
		t.Errorf("expected rate %d, got %d", defaultRate, cfg.rate)
	}
	if cfg.batchSize != defaultBatchSize {
		t.Errorf("expected batch size %d, got %d", defaultBatchSize, cfg.batchSize)
	}
	if cfg.source != defaultSource {
		t.Errorf("expected source %q, got %q", defaultSource, cfg.source)
	}
}

func TestParseConfigFromEnvironment(t *testing.T) {
	environment := map[string]string{
		"KAFKA_BROKER":     "kafka:29092",
		"KAFKA_TOPIC":      "custom.events",
		"EVENT_RATE":       "25",
		"KAFKA_BATCH_SIZE": "100",
		"EVENT_SOURCE":     "simulated",
	}

	cfg, err := parseConfig(nil, mapEnvironment(environment))
	if err != nil {
		t.Fatalf("parseConfig returned an unexpected error: %v", err)
	}

	if cfg.broker != "kafka:29092" {
		t.Errorf("expected environment broker, got %q", cfg.broker)
	}
	if cfg.topic != "custom.events" {
		t.Errorf("expected environment topic, got %q", cfg.topic)
	}
	if cfg.rate != 25 {
		t.Errorf("expected environment rate 25, got %d", cfg.rate)
	}
	if cfg.batchSize != 100 {
		t.Errorf("expected environment batch size 100, got %d", cfg.batchSize)
	}
}

func TestFlagsOverrideEnvironment(t *testing.T) {
	environment := map[string]string{
		"KAFKA_BROKER": "environment:9092",
		"EVENT_RATE":   "25",
	}

	cfg, err := parseConfig(
		[]string{"-broker", "flag:9092", "-rate", "50"},
		mapEnvironment(environment),
	)
	if err != nil {
		t.Fatalf("parseConfig returned an unexpected error: %v", err)
	}

	if cfg.broker != "flag:9092" {
		t.Errorf("expected flag broker, got %q", cfg.broker)
	}
	if cfg.rate != 50 {
		t.Errorf("expected flag rate 50, got %d", cfg.rate)
	}
}

func TestParseConfigRejectsInvalidEnvironmentInteger(t *testing.T) {
	environment := map[string]string{"EVENT_RATE": "fast"}

	if _, err := parseConfig(nil, mapEnvironment(environment)); err == nil {
		t.Fatal("expected an error for an invalid EVENT_RATE")
	}
}

func TestParseConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "zero rate", args: []string{"-rate", "0"}},
		{name: "zero batch size", args: []string{"-batch-size", "0"}},
		{name: "unsupported source", args: []string{"-source", "sniffer"}},
		{name: "unexpected argument", args: []string{"unexpected"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseConfig(test.args, emptyEnvironment); err == nil {
				t.Fatal("expected an invalid configuration error")
			}
		})
	}
}

func emptyEnvironment(string) (string, bool) {
	return "", false
}

func mapEnvironment(values map[string]string) envLookup {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
