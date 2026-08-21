package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
	kafkago "github.com/segmentio/kafka-go"
)

type Config struct {
	Broker    string
	Topic     string
	BatchSize int
	Logger    *slog.Logger
}

type Producer struct {
	writer  *kafkago.Writer
	logger  *slog.Logger
	metrics counters
}

func NewProducer(cfg Config) (*Producer, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	producer := &Producer{logger: logger}
	producer.writer = &kafkago.Writer{
		Addr:     kafkago.TCP(cfg.Broker),
		Topic:    cfg.Topic,
		Balancer: &kafkago.Hash{},

		BatchSize:    cfg.BatchSize,
		BatchTimeout: 500 * time.Millisecond,

		RequiredAcks: kafkago.RequireAll,
		MaxAttempts:  10,

		WriteBackoffMin: 100 * time.Millisecond,
		WriteBackoffMax: 2 * time.Second,
		WriteTimeout:    10 * time.Second,

		Async:                  true,
		AllowAutoTopicCreation: false,
		Completion:             producer.recordCompletion,
	}

	return producer, nil
}

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.Broker) == "" {
		return errors.New("kafka broker is required")
	}

	if strings.TrimSpace(cfg.Topic) == "" {
		return errors.New("kafka topic is required")
	}

	if cfg.BatchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero: %d", cfg.BatchSize)
	}

	return nil
}

func (p *Producer) recordCompletion(messages []kafkago.Message, err error) {
	p.metrics.batches.Add(1)

	if err != nil {
		p.metrics.failed.Add(uint64(len(messages)))
		p.logger.Error(
			"Kafka batch publication failed",
			"messages", len(messages),
			"error", err,
		)
		return
	}

	p.metrics.published.Add(uint64(len(messages)))
}

func (p *Producer) Send(ctx context.Context, securityEvent event.Event) error {
	message, err := newMessage(securityEvent)
	if err != nil {
		p.metrics.failed.Add(1)
		return err
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
		p.metrics.failed.Add(1)
		return fmt.Errorf("write Kafka message: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}

	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close Kafka writer: %w", err)
	}

	return nil
}

func (p *Producer) Metrics() Metrics {
	if p == nil {
		return Metrics{}
	}

	if p.writer != nil {
		stats := p.writer.Stats()
		if stats.Retries > 0 {
			p.metrics.retries.Add(uint64(stats.Retries))
		}
	}

	return p.metrics.snapshot()
}

func newMessage(securityEvent event.Event) (kafkago.Message, error) {
	value, err := json.Marshal(securityEvent)
	if err != nil {
		return kafkago.Message{}, fmt.Errorf("serialize security event: %w", err)
	}

	return kafkago.Message{
		Key:   []byte(securityEvent.SourceIP),
		Value: value,
		Time:  securityEvent.Timestamp,
	}, nil
}
