package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
	kafkago "github.com/segmentio/kafka-go"
)

type Config struct {
	Broker    string
	Topic     string
	BatchSize int
}

type Producer struct {
	writer *kafkago.Writer
}

func NewProducer(cfg Config) *Producer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 50
	}

	writer := &kafkago.Writer{
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

		Completion: func(messages []kafkago.Message, err error) {
			if err != nil {
				log.Printf(
					"Kafka publication failed messages=%d error=%v",
					len(messages),
					err,
				)
			}
		},
	}

	return &Producer{writer: writer}
}

func (p *Producer) Send(ctx context.Context, securityEvent event.Event) error {
	message, err := newMessage(securityEvent)
	if err != nil {
		return err
	}

	if err := p.writer.WriteMessages(ctx, message); err != nil {
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
