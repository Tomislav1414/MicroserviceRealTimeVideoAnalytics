package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
			// Each input partition (= one camera) maps to the identically
			// numbered output partition, so decoded frames keep the same
			// per-camera ordering guarantee the raw frames had — no hashing,
			// no collision risk, and no need to know the camera roster here.
			Balancer:     passthroughBalancer{},
			RequiredAcks: kafka.RequireOne,
			// See grabber/producer.go: default 1s BatchTimeout would stall
			// the per-message synchronous write path badly.
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

type passthroughBalancer struct{}

func (passthroughBalancer) Balance(msg kafka.Message, _ ...int) int {
	return msg.Partition
}

// EnsureTopic creates the topic with the given partition count if it
// doesn't exist yet. See grabber/producer.go for why explicit creation
// matters (auto-create defaults to 1 partition).
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Addr: kafka.TCP(brokers...),
		Topics: []kafka.TopicConfig{{
			Topic:             topic,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		}},
	})
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	if topicErr := resp.Errors[topic]; topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q: %w", topic, topicErr)
	}
	return nil
}

// ReadPartitionCount returns how many partitions an existing topic has, so
// the output topic can be created with the same count and keep a 1:1
// partition mapping with the input topic.
func ReadPartitionCount(brokers []string, topic string) (int, error) {
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, fmt.Errorf("read partitions of %q: %w", topic, err)
	}
	return len(partitions), nil
}

// FrameMeta is the metadata carried alongside a decoded frame, propagated
// from (or derived from) the corresponding raw-frames message.
type FrameMeta struct {
	FrameID      string
	CapturedAt   string
	RTPTimestamp string
	IsKeyframe   bool
	DecodedAt    time.Time
}

func (p *Producer) Send(ctx context.Context, key []byte, partition int, jpeg []byte, meta FrameMeta) error {
	headers := []kafka.Header{
		{Key: "frame_id", Value: []byte(meta.FrameID)},
		{Key: "captured_at_unix_nano", Value: []byte(meta.CapturedAt)},
		{Key: "rtp_pts_ns", Value: []byte(meta.RTPTimestamp)},
		{Key: "decoded_at_unix_nano", Value: []byte(strconv.FormatInt(meta.DecodedAt.UnixNano(), 10))},
		{Key: "is_keyframe", Value: []byte(strconv.FormatBool(meta.IsKeyframe))},
		{Key: "codec", Value: []byte("jpeg")},
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:       key,
		Value:     jpeg,
		Partition: partition,
		Headers:   headers,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
