//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
	kafkago "github.com/segmentio/kafka-go"
)

func TestProducerPublishesEvent(t *testing.T) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	topic := fmt.Sprintf(
		"security.events.integration.%d",
		time.Now().UnixNano(),
	)

	createTestTopic(t, broker, topic)

	timestamp := time.Now().UTC()
	expected := event.Event{
		Timestamp: timestamp,
		EventType: "auth_failed",
		SourceIP:  "10.0.0.99",
		Severity:  "high",
		Raw: json.RawMessage(
			`{"user":"integration-test","attempts":5}`,
		),
	}

	producer := NewProducer(Config{
		Broker:    broker,
		Topic:     topic,
		BatchSize: 10,
	})

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	if err := producer.Send(ctx, expected); err != nil {
		_ = producer.Close()
		t.Fatalf("send event: %v", err)
	}

	if err := producer.Close(); err != nil {
		t.Fatalf("close producer: %v", err)
	}

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     fmt.Sprintf("integration-%d", time.Now().UnixNano()),
		StartOffset: kafkago.FirstOffset,
		MinBytes:    1,
		MaxBytes:    1e6,
		MaxWait:     500 * time.Millisecond,
	})
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Logf("close Kafka reader: %v", err)
		}
	})

	message, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("read Kafka message: %v", err)
	}

	if got := string(message.Key); got != expected.SourceIP {
		t.Errorf(
			"expected Kafka key %q, got %q",
			expected.SourceIP,
			got,
		)
	}

	var received event.Event
	if err := json.Unmarshal(message.Value, &received); err != nil {
		t.Fatalf("decode received event: %v", err)
	}

	if received.EventType != expected.EventType {
		t.Errorf(
			"expected event type %q, got %q",
			expected.EventType,
			received.EventType,
		)
	}

	if received.SourceIP != expected.SourceIP {
		t.Errorf(
			"expected source IP %q, got %q",
			expected.SourceIP,
			received.SourceIP,
		)
	}

	if received.Severity != expected.Severity {
		t.Errorf(
			"expected severity %q, got %q",
			expected.Severity,
			received.Severity,
		)
	}

	if !received.Timestamp.Equal(expected.Timestamp) {
		t.Errorf(
			"expected timestamp %s, got %s",
			expected.Timestamp,
			received.Timestamp,
		)
	}

	if !json.Valid(received.Raw) {
		t.Errorf("received invalid raw JSON: %q", received.Raw)
	}
}

func createTestTopic(t *testing.T, broker, topic string) {
	t.Helper()

	client := &kafkago.Client{
		Addr:    kafkago.TCP(broker),
		Timeout: 10 * time.Second,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	response, err := client.CreateTopics(
		ctx,
		&kafkago.CreateTopicsRequest{
			Topics: []kafkago.TopicConfig{
				{
					Topic:             topic,
					NumPartitions:     1,
					ReplicationFactor: 1,
				},
			},
		},
	)
	if err != nil {
		t.Fatalf("create Kafka topic: %v", err)
	}

	if topicErr := response.Errors[topic]; topicErr != nil {
		t.Fatalf("create Kafka topic %q: %v", topic, topicErr)
	}

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()

		_, err := client.DeleteTopics(
			cleanupCtx,
			&kafkago.DeleteTopicsRequest{
				Topics: []string{topic},
			},
		)
		if err != nil {
			t.Logf("delete Kafka topic %q: %v", topic, err)
		}
	})
}