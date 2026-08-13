package main

import (
	"bytes"
	"context"
	"errors"
	"image/jpeg"
	"log"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// runPartitionWorker owns exactly one Kafka partition (= one camera) for the
// lifetime of a single consumer group generation. It gets a brand new
// h264Decoder every time — on first assignment, on rebalance, and on
// restart — so decode state never survives a partition handoff.
func runPartitionWorker(ctx context.Context, gen *kafka.Generation, cfg Config, partition int, startOffset int64, producer *Producer) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   cfg.KafkaBrokers,
		Topic:     cfg.InputTopic,
		Partition: partition,
	})
	defer reader.Close()
	reader.SetOffset(startOffset)

	dec := &h264Decoder{}
	if err := dec.initialize(); err != nil {
		log.Printf("[partition %d] decoder init failed: %v", partition, err)
		return
	}
	defer dec.close()

	offset := startOffset
	processed := 0
	// A partition assignment may resume from an arbitrary offset (last
	// committed, not necessarily a keyframe boundary), and the fresh decoder
	// above has no reference frames yet — so always discard until the next
	// keyframe before decoding anything.
	waitingForKeyframe := true

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, kafka.ErrGenerationEnded) {
				if err := gen.CommitOffsets(map[string]map[int]int64{cfg.InputTopic: {partition: offset + 1}}); err != nil {
					log.Printf("[partition %d] final commit failed: %v", partition, err)
				}
				return
			}
			log.Printf("[partition %d] read error: %v", partition, err)
			return
		}
		offset = msg.Offset

		isKeyframe := headerBool(msg.Headers, "is_keyframe")
		if waitingForKeyframe {
			if !isKeyframe {
				continue
			}
			waitingForKeyframe = false
		}

		img, err := dec.decode(msg.Value)
		if err != nil {
			log.Printf("[partition %d] decode error: %v (resyncing on next keyframe)", partition, err)
			waitingForKeyframe = true
			continue
		}
		if img == nil {
			continue // decoder consumed the packet but has no frame to emit yet
		}

		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, img, &jpeg.Options{Quality: cfg.JPEGQuality}); err != nil {
			log.Printf("[partition %d] jpeg encode error: %v", partition, err)
			continue
		}

		meta := FrameMeta{
			FrameID:      headerString(msg.Headers, "frame_id"),
			CapturedAt:   headerString(msg.Headers, "captured_at_unix_nano"),
			RTPTimestamp: headerString(msg.Headers, "rtp_pts_ns"),
			IsKeyframe:   isKeyframe,
			DecodedAt:    time.Now(),
		}
		if err := producer.Send(ctx, msg.Key, partition, buf.Bytes(), meta); err != nil {
			log.Printf("[partition %d] kafka send error: %v", partition, err)
		}

		processed++
		if processed%cfg.CommitEvery == 0 {
			if err := gen.CommitOffsets(map[string]map[int]int64{cfg.InputTopic: {partition: offset + 1}}); err != nil {
				log.Printf("[partition %d] commit error: %v", partition, err)
			}
		}
	}
}

func headerString(headers []kafka.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func headerBool(headers []kafka.Header, key string) bool {
	v, _ := strconv.ParseBool(headerString(headers, key))
	return v
}
