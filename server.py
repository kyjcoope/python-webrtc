import asyncio
import json
import logging
import os
import uuid

import av
from aiohttp import web
from aiortc import MediaStreamTrack, RTCPeerConnection, RTCSessionDescription

# Setup logging
logging.basicConfig(level=logging.INFO)
ROOT = os.path.dirname(__file__)
pcs = set()


# This class reads an MP4 file and streams its frames
class MP4StreamTrack(MediaStreamTrack):
    """
    A video stream track that reads frames from an MP4 file.
    """
    kind = "video"

    def __init__(self, path):
        super().__init__()
        self.container = av.open(path)
        self.stream = self.container.streams.video[0]
        self.stream.thread_type = "AUTO"  # Important for performance
        # Initialize the frame iterator right away
        self.frame_iterator = self.container.decode(self.stream)
        logging.info(f"Streaming {path}")

    async def recv(self):
        """
        This is called by aiortc to get the next frame.
        """
        try:
            frame = next(self.frame_iterator)
        except StopIteration:
            # =======================================================================
            # THE FIX: This is the robust looping logic.
            # -----------------------------------------------------------------------
            # When the video ends, we seek the container, get a NEW iterator,
            # and then grab the first frame from it. No recursion needed.
            logging.info("End of stream, seeking to beginning")
            self.container.seek(0)
            self.frame_iterator = self.container.decode(self.stream)
            frame = next(self.frame_iterator)
            # =======================================================================
        
        # This simple sleep provides frame pacing.
        # It's the duration of a single frame in seconds.
        await asyncio.sleep(float(frame.time_base))
        
        return frame


async def offer(request):
    params = await request.json()
    offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])

    pc = RTCPeerConnection()
    pc_id = "PeerConnection(%s)" % uuid.uuid4()
    pcs.add(pc)

    def log_info(msg, *args):
        logging.info(pc_id + " " + msg, *args)

    log_info("Created for %s", request.remote)

    # Create and add the MP4 video track
    video_path = os.path.join(ROOT, "video.mp4")
    if os.path.exists(video_path):
        track = MP4StreamTrack(video_path)
        pc.addTrack(track)
    else:
        log_info(f"Video file not found at {video_path}")

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        log_info("Connection state is %s", pc.connectionState)
        if pc.connectionState == "failed" or pc.connectionState == "closed":
            await pc.close()
            pcs.discard(pc)

    # Handle offer
    await pc.setRemoteDescription(offer)

    # Send answer
    answer = await pc.createAnswer()
    await pc.setLocalDescription(answer)

    return web.Response(
        content_type="application/json",
        text=json.dumps(
            {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}
        ),
    )


async def on_shutdown(app):
    # close peer connections
    coros = [pc.close() for pc in pcs]
    await asyncio.gather(*coros)
    pcs.clear()


if __name__ == "__main__":
    app = web.Application()
    app.on_shutdown.append(on_shutdown)
    app.router.add_post("/offer", offer)

    # Remember to use 0.0.0.0 to be accessible from your mobile device
    web.run_app(app, host="0.0.0.0", port=8080)