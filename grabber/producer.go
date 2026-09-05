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

func NewProducer(brokers []string, topic string, cameras []string, partitions int) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
			Balancer:     newCameraBalancer(cameras, partitions),
			RequiredAcks: kafka.RequireOne,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

type cameraBalancer struct {
	partitionByCamera map[string]int
}

func newCameraBalancer(cameras []string, numPartitions int) *cameraBalancer {
	m := make(map[string]int, len(cameras))
	for i, cam := range cameras {
		m[cam] = i % numPartitions
	}
	return &cameraBalancer{partitionByCamera: m}
}

func (b *cameraBalancer) Balance(msg kafka.Message, partitions ...int) int {
	if p, ok := b.partitionByCamera[string(msg.Key)]; ok {
		return p
	}
	return partitions[0]
}

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


type Frame struct {
	CameraID string
	FrameID  uint64
	CapturedAt   time.Time
	RTPTimestamp int64
	IsKeyframe   bool
	Payload      []byte
}

func (p *Producer) Send(ctx context.Context, f Frame) error {
	headers := []kafka.Header{
		{Key: "frame_id", Value: []byte(strconv.FormatUint(f.FrameID, 10))},
		{Key: "captured_at_unix_nano", Value: []byte(strconv.FormatInt(f.CapturedAt.UnixNano(), 10))},
		{Key: "rtp_pts_ns", Value: []byte(strconv.FormatInt(int64(f.RTPTimestamp), 10))},
		{Key: "is_keyframe", Value: []byte(strconv.FormatBool(f.IsKeyframe))},
		{Key: "codec", Value: []byte("h264")},
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:     []byte(f.CameraID),
		Value:   f.Payload,
		Headers: headers,
	})
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
