package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Mach1r0/sentinel-producer/internal/event"
	"github.com/Mach1r0/sentinel-producer/internal/kafka"
)

const (
	defaultBroker    = "localhost:9092"
	defaultTopic     = "security.events"
	defaultRate      = 10
	defaultBatchSize = 50
	defaultSource    = "simulated"
)

type config struct {
	broker    string
	topic     string
	rate      int
	batchSize int
	source    string
}

type envLookup func(string) (string, bool)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := parseConfig(os.Args[1:], os.LookupEnv)
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	generator := event.NewGenerator()
	producer, err := kafka.NewProducer(kafka.Config{
		Broker:    cfg.broker,
		Topic:     cfg.topic,
		BatchSize: cfg.batchSize,
		Logger:    logger,
	})
	if err != nil {
		logger.Error("failed to create producer", "error", err)
		os.Exit(1)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			logger.Error("failed to close producer", "error", err)
		}

		metrics := producer.Metrics()
		logger.Info(
			"producer stopped",
			"published", metrics.Published,
			"failed", metrics.Failed,
			"batches", metrics.Batches,
			"retries", metrics.Retries,
		)
	}()

	interval := time.Second / time.Duration(cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Info(
		"producer started",
		"broker", cfg.broker,
		"topic", cfg.topic,
		"rate", cfg.rate,
		"batch_size", cfg.batchSize,
		"source", cfg.source,
	)

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down producer")
			return

		case <-ticker.C:
			securityEvent := generator.Generate()

			if err := producer.Send(ctx, securityEvent); err != nil {
				logger.Error("failed to send event", "error", err)
			}
		}
	}
}

func parseConfig(args []string, lookup envLookup) (config, error) {
	rate, err := envInt(lookup, "EVENT_RATE", defaultRate)
	if err != nil {
		return config{}, err
	}

	batchSize, err := envInt(lookup, "KAFKA_BATCH_SIZE", defaultBatchSize)
	if err != nil {
		return config{}, err
	}

	cfg := config{
		broker:    envString(lookup, "KAFKA_BROKER", defaultBroker),
		topic:     envString(lookup, "KAFKA_TOPIC", defaultTopic),
		rate:      rate,
		batchSize: batchSize,
		source:    envString(lookup, "EVENT_SOURCE", defaultSource),
	}

	flags := flag.NewFlagSet("sentinel-producer", flag.ContinueOnError)
	flags.StringVar(&cfg.broker, "broker", cfg.broker, "Kafka broker address")
	flags.StringVar(&cfg.topic, "topic", cfg.topic, "Kafka topic")
	flags.IntVar(&cfg.rate, "rate", cfg.rate, "Events generated per second")
	flags.IntVar(&cfg.batchSize, "batch-size", cfg.batchSize, "Kafka batch size")
	flags.StringVar(&cfg.source, "source", cfg.source, "Event source")

	if err := flags.Parse(args); err != nil {
		return config{}, err
	}

	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(flags.Args(), " "))
	}

	if err := cfg.validate(); err != nil {
		return config{}, err
	}

	return cfg, nil
}

func (cfg config) validate() error {
	if cfg.rate <= 0 {
		return fmt.Errorf("rate must be greater than zero: %d", cfg.rate)
	}

	if cfg.batchSize <= 0 {
		return fmt.Errorf("batch size must be greater than zero: %d", cfg.batchSize)
	}

	if cfg.source != "simulated" {
		return fmt.Errorf(
			"unsupported source %q: currently only simulated is available",
			cfg.source,
		)
	}

	return nil
}

func envString(lookup envLookup, name, fallback string) string {
	if value, ok := lookup(name); ok {
		return value
	}

	return fallback
}

func envInt(lookup envLookup, name string, fallback int) (int, error) {
	value, ok := lookup(name)
	if !ok {
		return fallback, nil
	}

	number, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", name, value, err)
	}

	return number, nil
}
