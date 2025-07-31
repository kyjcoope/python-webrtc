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
# CONFIGURATION: This string will be used to EXACTLY replace the H.264 format line.
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

    log_info("Offer requested, will force H.264 fmtp line to: %s", DESIRED_FMTP_LINE)
    
    video_path = os.path.join(ROOT, "video.mp4")
    if os.path.exists(video_path):
        track = MP4StreamTrack(video_path)
        pc.addTrack(track)

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        log_info("Connection state is %s", pc.connectionState)
        if pc.connectionState in ["failed", "closed", "disconnected"]:
            await pc.close()
            if pc_id in peer_connections:
                del peer_connections[pc_id]

    offer = await pc.createOffer()
    await pc.setLocalDescription(offer)
    
    pattern = r"(a=fmtp:(\d+) )(.+profile-level-id.+)"
    replacement = r"\g<1>" + DESIRED_FMTP_LINE
    munged_sdp, count = re.subn(pattern, replacement, pc.localDescription.sdp, flags=re.IGNORECASE)

    if count > 0:
        log_info("Successfully munged SDP to force H.264 profile.")
    else:
        log_info("WARNING: Could not find H.264 fmtp line in SDP to munge.")

    return web.Response(
        content_type="application/json",
        text=json.dumps({
            "id": pc_id,
            "sdp": munged_sdp,
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

    # =======================================================================
    # THE FIX: port="8080" has been changed to port=8080
    # =======================================================================
    web.run_app(app, host="0.0.0.0", port=8080)