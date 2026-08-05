package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
	"github.com/Mach1r0/sentinel-producer/internal/kafka"
)

type config struct {
	broker    string
	topic     string
	rate      int
	batchSize int
	source    string
}

func main() {
	cfg := parseFlags()

	if cfg.rate <= 0 {
		log.Fatalf("invalid rate %d: rate must be greater than zero", cfg.rate)
	}

	if cfg.batchSize <= 0 {
		log.Fatalf("invalid batch size %d: batch size must be greater than zero", cfg.batchSize)
	}

	if cfg.source != "simulated" {
		log.Fatalf("unsupported source %q: currently only simulated is available", cfg.source)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	generator := event.NewGenerator()
	producer := kafka.NewProducer(kafka.Config{
		Broker:    cfg.broker,
		Topic:     cfg.topic,
		BatchSize: cfg.batchSize,
	})

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("failed to close producer: %v", err)
		}
	}()

	interval := time.Second / time.Duration(cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf(
		"producer started broker=%s topic=%s rate=%d batch_size=%d source=%s",
		cfg.broker,
		cfg.topic,
		cfg.rate,
		cfg.batchSize,
		cfg.source,
	)

	for {
		select {
		case <-ctx.Done():
			log.Println("shutting down producer")
			return

		case <-ticker.C:
			securityEvent := generator.Generate()

			if err := producer.Send(ctx, securityEvent); err != nil {
				log.Printf("failed to send event: %v", err)
			}
		}
	}
}

func parseFlags() config {
	var cfg config

	flag.StringVar(&cfg.broker, "broker", "localhost:9092", "Kafka broker address")
	flag.StringVar(&cfg.topic, "topic", "security.events", "Kafka topic")
	flag.IntVar(&cfg.rate, "rate", 10, "Events generated per second")
	flag.IntVar(&cfg.batchSize, "batch-size", 50, "Kafka batch size")
	flag.StringVar(&cfg.source, "source", "simulated", "Event source")
	flag.Parse()

	return cfg
}
