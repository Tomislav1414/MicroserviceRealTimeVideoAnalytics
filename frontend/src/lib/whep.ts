// Minimal WHEP (WebRTC-HTTP Egress Protocol) client for pulling a live view
// straight from mediamtx — no signaling server, no media relay through any
// pipeline service. Non-trickle ICE (waits for gathering to complete before
// sending the offer) is a deliberate simplification: everything here runs
// on one machine, so ICE gathering is host-candidates-only and fast enough
// that trickle ICE wouldn't meaningfully improve connect time.

export interface WhepSession {
  close(): void;
}

export async function connectWhep(
  whepUrl: string,
  video: HTMLVideoElement,
  onConnectionStateChange?: (state: RTCPeerConnectionState) => void,
): Promise<WhepSession> {
  const pc = new RTCPeerConnection();
  pc.addTransceiver("video", { direction: "recvonly" });
  pc.addTransceiver("audio", { direction: "recvonly" });

  const stream = new MediaStream();
  video.srcObject = stream;
  pc.ontrack = (event) => {
    stream.addTrack(event.track);
  };
  if (onConnectionStateChange) {
    pc.onconnectionstatechange = () => onConnectionStateChange(pc.connectionState);
  }

  const offer = await pc.createOffer();
  await pc.setLocalDescription(offer);
  await waitForIceGatheringComplete(pc);

  const resp = await fetch(whepUrl, {
    method: "POST",
    headers: { "Content-Type": "application/sdp" },
    body: pc.localDescription!.sdp,
  });
  if (!resp.ok) {
    pc.close();
    throw new Error(`WHEP POST to ${whepUrl} failed: ${resp.status}`);
  }
  const answerSdp = await resp.text();
  await pc.setRemoteDescription({ type: "answer", sdp: answerSdp });

  const location = resp.headers.get("Location");
  const resourceUrl = location ? new URL(location, whepUrl).toString() : null;

  return {
    close() {
      pc.close();
      if (resourceUrl) {
        // Best-effort: tell mediamtx to release the WHEP session. Ignore
        // failures — the peer connection is already closed either way.
        fetch(resourceUrl, { method: "DELETE" }).catch(() => {});
      }
    },
  };
}

function waitForIceGatheringComplete(pc: RTCPeerConnection): Promise<void> {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve) => {
    const onChange = () => {
      if (pc.iceGatheringState === "complete") {
        pc.removeEventListener("icegatheringstatechange", onChange);
        resolve();
      }
    };
    pc.addEventListener("icegatheringstatechange", onChange);
    // Safety net: proceed with whatever candidates were gathered rather
    // than hanging forever if gathering stalls.
    setTimeout(() => {
      pc.removeEventListener("icegatheringstatechange", onChange);
      resolve();
    }, 3000);
  });
}
