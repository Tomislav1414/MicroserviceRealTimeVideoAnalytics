package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := ensureTopicWithRetry(ctx, cfg); err != nil {
		if ctx.Err() != nil {
			log.Println("shutting down before topic was ready")
			return
		}
		log.Fatalf("ensure topic: %v", err)
	}

	producer := NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.Cameras, cfg.KafkaPartitions)
	defer producer.Close()

	var wg sync.WaitGroup
	for _, camID := range cfg.Cameras {
		rtspURL := fmt.Sprintf("rtsp://%s:%s/%s", cfg.MediaMTXHost, cfg.MediaMTXPort, camID)
		wg.Add(1)
		go func(camID, rtspURL string) {
			defer wg.Done()
			log.Printf("[%s] starting, source=%s", camID, rtspURL)
			RunCamera(ctx, camID, rtspURL, producer)
			log.Printf("[%s] stopped", camID)
		}(camID, rtspURL)
	}

	wg.Wait()
	log.Println("all camera workers stopped, exiting")
}

// ensureTopicWithRetry retries topic creation until it succeeds or ctx is
// cancelled — infra and grabber are separate compose stacks with no startup
// ordering guarantee, so Kafka may not be reachable yet on first attempt.
func ensureTopicWithRetry(ctx context.Context, cfg Config) error {
	for {
		err := EnsureTopic(ctx, cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaPartitions)
		if err == nil {
			log.Printf("topic %q ready (%d partitions)", cfg.KafkaTopic, cfg.KafkaPartitions)
			return nil
		}

		log.Printf("waiting for kafka to create topic %q: %v", cfg.KafkaTopic, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(reconnectDelay):
		}
	}
}
