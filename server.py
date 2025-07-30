import asyncio
import json
import logging
import os
import time
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
        # Get the frame rate for pacing
        self.fps = self.stream.average_rate
        # Create an iterator for the frames
        self.frame_iterator = self.container.decode(self.stream)
        self.start_time = None
        logging.info(f"Streaming {path} at {self.fps} fps")


    async def recv(self):
        """
        This method is called by aiortc to get the next frame.
        """
        if self.start_time is None:
            self.start_time = time.time()
        
        try:
            # Get the next frame from our iterator
            frame = next(self.frame_iterator)
        except StopIteration:
            logging.info("End of stream, seeking to beginning")
            # If the stream ends, seek to the beginning to loop it
            self.container.seek(0)
            self.frame_iterator = self.container.decode(self.stream)
            frame = next(self.frame_iterator)
            # Reset the start time for correct pacing on loop
            self.start_time = time.time()


        # Calculate the time to wait to maintain the original frame rate
        # This is the core of the real-time pacing logic
        pts_in_seconds = frame.pts * self.stream.time_base
        wall_clock_time = time.time() - self.start_time
        wait_time = float(pts_in_seconds) - wall_clock_time

        if wait_time > 0:
            await asyncio.sleep(wait_time)
            
        # The frame needs to have its pts set for aiortc
        frame.pts = int((time.time() - self.start_time) * 1000)
        frame.time_base = 1/1000 # Milliseconds
        
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