package main

// #cgo pkg-config: libavcodec libavutil libswscale
// #include <libavcodec/avcodec.h>
// #include <libavutil/imgutils.h>
// #include <libswscale/swscale.h>
import "C"

import (
	"fmt"
	"image"
	"runtime"
	"unsafe"
)

func frameData(frame *C.AVFrame) **C.uint8_t {
	return (**C.uint8_t)(unsafe.Pointer(&frame.data[0]))
}

func frameLineSize(frame *C.AVFrame) *C.int {
	return (*C.int)(unsafe.Pointer(&frame.linesize[0]))
}

// h264Decoder wraps FFmpeg's H264 decoder
type h264Decoder struct {
	codecCtx    *C.AVCodecContext
	yuv420Frame *C.AVFrame
	rgbaFrame   *C.AVFrame
	swsCtx      *C.struct_SwsContext
}

func (d *h264Decoder) initialize() error {
	codec := C.avcodec_find_decoder(C.AV_CODEC_ID_H264)
	if codec == nil {
		return fmt.Errorf("avcodec_find_decoder() failed")
	}

	d.codecCtx = C.avcodec_alloc_context3(codec)
	if d.codecCtx == nil {
		return fmt.Errorf("avcodec_alloc_context3() failed")
	}

	if res := C.avcodec_open2(d.codecCtx, codec, nil); res < 0 {
		C.avcodec_free_context(&d.codecCtx)
		return fmt.Errorf("avcodec_open2() failed")
	}

	d.yuv420Frame = C.av_frame_alloc()
	if d.yuv420Frame == nil {
		C.avcodec_free_context(&d.codecCtx)
		return fmt.Errorf("av_frame_alloc() failed")
	}

	return nil
}

func (d *h264Decoder) close() {
	if d.swsCtx != nil {
		C.sws_freeContext(d.swsCtx)
	}
	if d.rgbaFrame != nil {
		C.av_frame_free(&d.rgbaFrame)
	}
	C.av_frame_free(&d.yuv420Frame)
	C.avcodec_free_context(&d.codecCtx)
}

func (d *h264Decoder) reinitDynamicStuff() error {
	if d.swsCtx != nil {
		C.sws_freeContext(d.swsCtx)
	}
	if d.rgbaFrame != nil {
		C.av_frame_free(&d.rgbaFrame)
	}

	d.rgbaFrame = C.av_frame_alloc()
	if d.rgbaFrame == nil {
		return fmt.Errorf("av_frame_alloc() failed")
	}

	d.rgbaFrame.format = C.AV_PIX_FMT_RGBA
	d.rgbaFrame.width = d.yuv420Frame.width
	d.rgbaFrame.height = d.yuv420Frame.height
	d.rgbaFrame.color_range = C.AVCOL_RANGE_JPEG

	if res := C.av_frame_get_buffer(d.rgbaFrame, 1); res < 0 {
		return fmt.Errorf("av_frame_get_buffer() failed")
	}

	d.swsCtx = C.sws_getContext(d.yuv420Frame.width, d.yuv420Frame.height, int32(d.yuv420Frame.format),
		d.rgbaFrame.width, d.rgbaFrame.height, (int32)(d.rgbaFrame.format), C.SWS_BILINEAR, nil, nil, nil)
	if d.swsCtx == nil {
		return fmt.Errorf("sws_getContext() failed")
	}

	return nil
}

func (d *h264Decoder) decode(annexb []byte) (*image.RGBA, error) {
	if len(annexb) == 0 {
		return nil, nil
	}

	var pkt C.AVPacket
	ptr := &annexb[0]
	var p runtime.Pinner
	p.Pin(ptr)
	pkt.data = (*C.uint8_t)(ptr)
	pkt.size = (C.int)(len(annexb))
	res := C.avcodec_send_packet(d.codecCtx, &pkt)
	p.Unpin()
	if res < 0 {
		return nil, fmt.Errorf("avcodec_send_packet() failed: %d", res)
	}

	res = C.avcodec_receive_frame(d.codecCtx, d.yuv420Frame)
	if res < 0 {
		// EAGAIN: decoder needs more input before it can produce a frame.
		return nil, nil
	}

	if d.rgbaFrame == nil || d.rgbaFrame.width != d.yuv420Frame.width || d.rgbaFrame.height != d.yuv420Frame.height {
		if err := d.reinitDynamicStuff(); err != nil {
			return nil, err
		}
	}

	res = C.sws_scale(d.swsCtx, frameData(d.yuv420Frame), frameLineSize(d.yuv420Frame),
		0, d.yuv420Frame.height, frameData(d.rgbaFrame), frameLineSize(d.rgbaFrame))
	if res < 0 {
		return nil, fmt.Errorf("sws_scale() failed")
	}

	rgbaFrameSize := C.av_image_get_buffer_size((int32)(d.rgbaFrame.format), d.rgbaFrame.width, d.rgbaFrame.height, 1)
	pix := (*[1 << 30]uint8)(unsafe.Pointer(d.rgbaFrame.data[0]))[:rgbaFrameSize:rgbaFrameSize]

	// Copy out of FFmpeg-owned memory: rgbaFrame's buffer is reused on the
	// next decode call, so the returned image must not alias it.
	pixCopy := make([]uint8, len(pix))
	copy(pixCopy, pix)

	return &image.RGBA{
		Pix:    pixCopy,
		Stride: 4 * (int)(d.rgbaFrame.width),
		Rect: image.Rectangle{
			Max: image.Point{X: (int)(d.rgbaFrame.width), Y: (int)(d.rgbaFrame.height)},
		},
	}, nil
}
