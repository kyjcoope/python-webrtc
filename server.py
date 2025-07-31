import asyncio
import json
import logging
import os
import uuid
import re  # Import the regular expression module

import av
from aiohttp import web
from aiortc import MediaStreamTrack, RTCPeerConnection, RTCSessionDescription
from aiortc.rtcrtpsender import RTCRtpSender

# =======================================================================
# CONFIGURATION: Set the H.264 profile you want to force.
# -----------------------------------------------------------------------
DESIRED_FMTP_LINE = "packetization-mode=1;profile-level-id=42e01f"
# Example without asymmetry:
# DESIRED_FMTP_LINE = "packetization-mode=1;profile-level-id=640c2a"
# =======================================================================

logging.basicConfig(level=logging.INFO)
ROOT = os.path.dirname(__file__)
peer_connections = {}


class MP4StreamTrack(MediaStreamTrack):
    kind = "video"

    def __init__(self, path):
        super().__init__()
        self.container = av.open(path)
        self.stream = self.container.streams.video[0]
        self.stream.thread_type = "AUTO"
        self.frame_iterator = self.container.decode(self.stream)
        logging.info(f"Streaming {path}")

    async def recv(self):
        try:
            frame = next(self.frame_iterator)
        except StopIteration:
            logging.info("End of stream, seeking to beginning")
            self.container.seek(0)
            self.frame_iterator = self.container.decode(self.stream)
            frame = next(self.frame_iterator)
        
        await asyncio.sleep(float(frame.time_base))
        return frame


async def request_offer(request):
    pc = RTCPeerConnection()
    pc_id = str(uuid.uuid4())
    peer_connections[pc_id] = pc

    def log_info(msg, *args):
        logging.info(f"PC({pc_id}) " + msg, *args)

    log_info("Offer requested, preparing H.264-only stream.")
    
    video_path = os.path.join(ROOT, "video.mp4")
    if os.path.exists(video_path):
        track = MP4StreamTrack(video_path)
        pc.addTrack(track)

    # =======================================================================
    # THE FINAL, CORRECT IMPLEMENTATION: Configure THEN Munge
    # -----------------------------------------------------------------------

    # 1. Get the video transceiver
    transceiver = next(t for t in pc.getTransceivers() if t.kind == "video")

    # 2. Get all supported codecs and filter for ONLY H.264
    all_codecs = RTCRtpSender.getCapabilities("video").codecs
    h264_codecs = [
        codec for codec in all_codecs if codec.mimeType.lower() == "video/h264"
    ]

    # 3. Set the transceiver's preferences to ONLY use H.264.
    #    This correctly configures the internal state of aiortc.
    if h264_codecs:
        transceiver.setCodecPreferences(h264_codecs)
        log_info("Successfully set codec preferences to H.264 only.")
    else:
        log_info("H.264 not found in capabilities, using default.")
    
    # =======================================================================

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        log_info("Connection state is %s", pc.connectionState)
        if pc.connectionState in ["failed", "closed", "disconnected"]:
            await pc.close()
            if pc_id in peer_connections:
                del peer_connections[pc_id]

    # 4. Create the offer. It will now only contain H.264.
    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    
    # 5. (Optional but recommended for testing) Gently munge the final SDP
    #    to ensure our exact profile string is used, overriding any "helpful"
    #    additions from the library.
    pattern = r"(a=fmtp:(\d+) )(.+profile-level-id.+)"
    replacement = r"\g<1>" + DESIRED_FMTP_LINE
    final_sdp, count = re.subn(pattern, replacement, pc.localDescription.sdp, flags=re.IGNORECASE)

    if count > 0:
        log_info("Successfully forced exact H.264 profile in the final SDP.")
    
    return web.Response(
        content_type="application/json",
        text=json.dumps({
            "id": pc_id,
            "sdp": final_sdp,
            "type": pc.localDescription.type,
        }),
    )

async def submit_answer(request):
    params = await request.json()
    pc_id = params["id"]
    
    if pc_id not in peer_connections:
        return web.Response(status=404, text="Peer connection not found")

    pc = peer_connections[pc_id]
    
    def log_info(msg, *args):
        logging.info(f"PC({pc_id}) " + msg, *args)

    log_info("Received answer")
    
    answer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])
    await pc.setRemoteDescription(answer)

    return web.Response(content_type="application/json", text=json.dumps({"status": "ok"}))


async def on_shutdown(app):
    coros = [pc.close() for pc in peer_connections.values()]
    await asyncio.gather(*coros)
    peer_connections.clear()


if __name__ == "__main__":
    app = web.Application()
    app.on_shutdown.append(on_shutdown)
    app.router.add_get("/request-offer", request_offer)
    app.router.add_post("/submit-answer", submit_answer)
    
    web.run_app(app, host="0.0.0.0", port=8080)