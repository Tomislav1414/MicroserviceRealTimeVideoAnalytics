package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/bluenviron/gortsplib/v5"
	"github.com/bluenviron/gortsplib/v5/pkg/base"
	"github.com/bluenviron/gortsplib/v5/pkg/format"
	"github.com/bluenviron/gortsplib/v5/pkg/format/rtph264"
	"github.com/bluenviron/mediacommon/v2/pkg/codecs/h264"
	"github.com/pion/rtp"
)

const reconnectDelay = 3 * time.Second

func RunCamera(ctx context.Context, camID, rtspURL string, producer *Producer) {
	var frameID uint64

	for ctx.Err() == nil {
		if err := streamOnce(ctx, camID, rtspURL, producer, &frameID); err != nil && ctx.Err() == nil {
			log.Printf("[%s] stream error: %v (reconnecting in %s)", camID, err, reconnectDelay)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func streamOnce(ctx context.Context, camID, rtspURL string, producer *Producer, frameID *uint64) error {
	u, err := base.ParseURL(rtspURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}

	tcp := gortsplib.ProtocolTCP
	c := gortsplib.Client{
		Scheme: u.Scheme,
		Host:   u.Host,
		Protocol: &tcp,
	}

	if err := c.Start(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer c.Close()

	desc, _, err := c.Describe(u)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}

	var forma *format.H264
	medi := desc.FindFormat(&forma)
	if medi == nil {
		return errors.New("no H264 media in stream")
	}

	rtpDec, err := forma.CreateDecoder()
	if err != nil {
		return fmt.Errorf("create rtp decoder: %w", err)
	}

	if _, err := c.Setup(desc.BaseURL, medi, 0, 0); err != nil {
		return fmt.Errorf("setup: %w", err)
	}

	firstRandomAccess := false
	var sendErr error

	c.OnPacketRTP(medi, forma, func(pkt *rtp.Packet) {
		if sendErr != nil {
			return
		}

		pts, ok := c.PacketPTS(medi, pkt)
		if !ok {
			return
		}

		au, err := rtpDec.Decode(pkt)
		if err != nil {
			if !errors.Is(err, rtph264.ErrNonStartingPacketAndNoPrevious) && !errors.Is(err, rtph264.ErrMorePacketsNeeded) {
				log.Printf("[%s] rtp decode: %v", camID, err)
			}
			return
		}

		isKeyframe := h264.IsRandomAccess(au)

		if !firstRandomAccess {
			if !isKeyframe {
				return
			}
			firstRandomAccess = true
		}

		*frameID++
		if err := producer.Send(ctx, Frame{
			CameraID:      camID,
			FrameID:       *frameID,
			CapturedAt:    time.Now(),
			RTPTimestamp:  pts,
			IsKeyframe:    isKeyframe,
			Payload:       annexBEncode(au),
		}); err != nil {
			sendErr = fmt.Errorf("kafka send: %w", err)
		}
	})

	if _, err := c.Play(nil); err != nil {
		return fmt.Errorf("play: %w", err)
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- c.Wait() }()

	select {
	case <-ctx.Done():
		return nil
	case err := <-waitErr:
		if sendErr != nil {
			return sendErr
		}
		return err
	}
}

func annexBEncode(au [][]byte) []byte {
	size := 0
	for _, nalu := range au {
		size += 4 + len(nalu)
	}
	out := make([]byte, 0, size)
	for _, nalu := range au {
		out = append(out, 0x00, 0x00, 0x00, 0x01)
		out = append(out, nalu...)
	}
	return out
}
