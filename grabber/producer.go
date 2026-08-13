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
			// A generic hash balancer can still collide two cameras onto the
			// same partition (e.g. 2 cameras, 2 partitions is a coin flip).
			// The camera list is static and known up front, so assign each
			// camera a fixed partition deterministically instead.
			Balancer:     newCameraBalancer(cameras, partitions),
			RequiredAcks: kafka.RequireOne,
			// WriteMessages is called synchronously once per frame from the
			// RTP packet callback, so it directly gates how fast frames are
			// read off the stream. The 1s default BatchTimeout would stall
			// every single-frame write for up to a second, starving the RTP
			// read loop; keep it just long enough to still batch bursts.
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

// cameraBalancer deterministically maps each configured camera ID to a fixed
// partition (round-robin over the static camera list), so per-camera
// partition affinity holds exactly, not just probabilistically.
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
	// Unconfigured key: fall back to the first available partition rather
	// than panicking.
	return partitions[0]
}

// EnsureTopic creates the topic with the given partition count if it doesn't
// already exist yet. Must be called before any camera worker starts —
// relying on broker auto-create would default to 1 partition regardless of
// camera count, silently breaking per-camera partition affinity.
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

// Frame is one H264 access unit (all NAL units for a single decoded picture)
// ready to be published.
type Frame struct {
	CameraID string
	FrameID  uint64
	// CapturedAt is wall-clock time at the grabber, for end-to-end latency
	// measurement. RTPTimestamp is the stream's own RTP clock (90kHz for
	// H264), useful for playout pacing but not comparable across cameras.
	CapturedAt   time.Time
	RTPTimestamp int64
	IsKeyframe   bool
	Payload      []byte
}

// Send publishes a frame keyed by camera ID, with per-frame metadata carried
// in Kafka headers so the payload stays raw NAL bytes with no envelope
// encoding/decoding cost.
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
