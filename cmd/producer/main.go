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
	broker string 
	topic string
	rate int 
	batchSize int 
	source string 
}

func main() {
	cfg := ParseFlags()

	if cfg.rate <= 0 {
		log.Fatalf("Invalid rate: %d. Rate must be greater than 0.", cfg.rate)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM
	)

	defer stop()

	generator := event.NewGenerator()

	producer := kafka.NewProducer(kafka.Config{
		Broker: cfg.broker, 
		Topic: cfg.topic,
		BatchSize: cfg.batchSize, 
	})

	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Failed to close producer: %v", err)
		}
	}()

	interval := time.Second / time.Duration(cfg.rate)
    ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.Printf("Producing events to topic %s at a rate of %d events per second", cfg.topic, cfg.rate)
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down producer...")
			return
		
		case <-ticker.C:
			securityEVent := generator.Generate()
		}

		if err := producer.Send(securityEVent); err != nil {
			log.Printf("Failed to send event: %v", err)
		}
	}

	
	func parseFlags() config {
		var cfg config

		flag.StringVar(&cfg.broker, "broker", "localhost:9092", "Kafka broker address")
		flag.StringVar(&cfg.topic, "topic", "security-events", "Kafka topic to produce events to")
		flag.IntVar(&cfg.rate, "rate", 10, "Number of events to produce per second")
		flag.IntVar(&cfg.batchSize, "batch-size", 100, "Number of events to send in a single batch")
		flag.StringVar(&cfg.source, "source", "default-source", "Source of the security events")
		
		flag.Parse()

		return cfg
	}
}