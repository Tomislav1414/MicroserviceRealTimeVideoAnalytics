package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	inputPartitions, err := readPartitionCountWithRetry(ctx, cfg)
	if err != nil {
		if ctx.Err() != nil {
			log.Println("shutting down before input topic was ready")
			return
		}
		log.Fatalf("read input partitions: %v", err)
	}

	if err := ensureTopicWithRetry(ctx, cfg.KafkaBrokers, cfg.OutputTopic, inputPartitions); err != nil {
		if ctx.Err() != nil {
			log.Println("shutting down before output topic was ready")
			return
		}
		log.Fatalf("ensure output topic: %v", err)
	}
	log.Printf("output topic %q ready (%d partitions, mirrors %q)", cfg.OutputTopic, inputPartitions, cfg.InputTopic)

	producer := NewProducer(cfg.KafkaBrokers, cfg.OutputTopic)
	defer producer.Close()

	group, err := kafka.NewConsumerGroup(kafka.ConsumerGroupConfig{
		ID:      cfg.GroupID,
		Brokers: cfg.KafkaBrokers,
		Topics:  []string{cfg.InputTopic},
	})
	if err != nil {
		log.Fatalf("create consumer group: %v", err)
	}
	defer group.Close()

	for {
		gen, err := group.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Println("shutting down")
				return
			}
			log.Printf("consumer group error: %v (retrying)", err)
			continue
		}

		for _, assignment := range gen.Assignments[cfg.InputTopic] {
			partition, offset := assignment.ID, assignment.Offset
			log.Printf("[partition %d] assigned, starting at offset %d", partition, offset)
			gen.Start(func(ctx context.Context) {
				runPartitionWorker(ctx, gen, cfg, partition, offset, producer)
			})
		}
	}
}

func readPartitionCountWithRetry(ctx context.Context, cfg Config) (int, error) {
	for {
		n, err := ReadPartitionCount(cfg.KafkaBrokers, cfg.InputTopic)
		if err == nil {
			return n, nil
		}
		log.Printf("waiting for input topic %q: %v", cfg.InputTopic, err)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func ensureTopicWithRetry(ctx context.Context, brokers []string, topic string, partitions int) error {
	for {
		err := EnsureTopic(ctx, brokers, topic, partitions)
		if err == nil {
			return nil
		}
		log.Printf("waiting for kafka to create topic %q: %v", topic, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}
