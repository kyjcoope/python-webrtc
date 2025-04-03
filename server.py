import argparse
import asyncio
import json
import logging
import os
import ssl
from typing import Set, Optional
from aiohttp import web
from aiortc import MediaStreamTrack, RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaPlayer, MediaRelay

#python server.py --rtsp-url "rtsp://user:pass@ip:port/path"
#python server.py --rtsp-url "rtsp://user:pass@ip:port/path" --cert-file cert.pem --key-file key.pem --host your_server_ip

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
pcs: Set[RTCPeerConnection] = set()
media_player: Optional[MediaPlayer] = None
relay = MediaRelay()

class RTSPVideoTrack(MediaStreamTrack):
    kind = "video"
    def __init__(self, source_track):
        super().__init__()
        self._source_track = source_track
    async def recv(self):
        frame = await self._source_track.recv()
        return frame

async def offer(request):
    try:
        params = await request.json()
        sdp = params.get("sdp")
        sdp_type = params.get("type")
        if not sdp or not sdp_type:
             logger.error("Received request missing 'sdp' or 'type'")
             return web.Response(content_type="application/json", text=json.dumps({"error": "Missing 'sdp' or 'type' in request body"}), status=400)
        offer_desc = RTCSessionDescription(sdp=sdp, type=sdp_type)
        logger.info(f"Received {sdp_type} offer from client {request.remote}")
    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON from {request.remote}")
        return web.Response(content_type="application/json", text=json.dumps({"error": "Invalid JSON format"}), status=400)
    except Exception as e:
         logger.error(f"Error processing request parameters: {e}", exc_info=True)
         return web.Response(content_type="application/json", text=json.dumps({"error": f"Error processing request: {e}"}), status=400)

    pc = RTCPeerConnection()
    pcs.add(pc)

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        logger.info(f"PC {pc}: Connection state is {pc.connectionState}")
        if pc.connectionState == "failed" or pc.connectionState == "closed" or pc.connectionState == "disconnected":
            logger.warning(f"PC {pc}: Closing due to state: {pc.connectionState}")
            await pc.close()
            if pc in pcs:
               pcs.discard(pc)
               logger.info(f"PC {pc}: Removed from active connections.")

    @pc.on("iceconnectionstatechange")
    async def on_iceconnectionstatechange():
        logger.info(f"PC {pc}: ICE connection state is {pc.iceConnectionState}")

    @pc.on("track")
    def on_track(track):
        logger.warning(f"PC {pc}: Track {track.kind} received, but not handled.")
        track.stop()

    if media_player is None or media_player.video is None:
        logger.error("Media player or video track not initialized!")
        await pc.close()
        if pc in pcs: pcs.discard(pc) # Ensure removal even if closed above
        return web.Response(content_type="application/json", text=json.dumps({"error": "RTSP stream source not available on server"}), status=503)

    try:
        relayed_track = relay.subscribe(media_player.video)
        logger.info(f"PC {pc}: Subscribed to relayed video track.")
        pc.addTrack(RTSPVideoTrack(relayed_track))
        logger.info(f"PC {pc}: Added RTSP video track to peer connection.")
        await pc.setRemoteDescription(offer_desc)
        logger.info(f"PC {pc}: Remote description (offer) set.")
        answer = await pc.createAnswer()
        logger.info(f"PC {pc}: Answer created.")
        await pc.setLocalDescription(answer)
        logger.info(f"PC {pc}: Local description (answer) set.")
        if pc.localDescription and pc.localDescription.sdp:
            logger.info(f"PC {pc}: Local description type: {pc.localDescription.type}")
            logger.info(f"PC {pc}: Local description SDP length: {len(pc.localDescription.sdp)}")
        else:
            logger.error(f"PC {pc}: !!! Local description or SDP is missing after setting !!!")
            raise Exception("Failed to generate valid local description")
    except Exception as e:
        logger.error(f"PC {pc}: Error during offer/answer exchange: {e}", exc_info=True)
        await pc.close()
        if pc in pcs:
            pcs.discard(pc)
            logger.info(f"PC {pc}: Removed from active connections due to error.")
        return web.Response(content_type="application/json", text=json.dumps({"error": f"Failed to establish WebRTC connection: {e}"}), status=500)

    logger.info(f"PC {pc}: Sending answer back to client {request.remote}")
    response_data = {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}
    return web.Response(content_type="application/json", text=json.dumps(response_data), status=200)

async def on_startup(app):
    global media_player
    args = app["args"]
    logger.info("Server starting up...")
    logger.info(f"Attempting to connect to RTSP stream: {args.rtsp_url}")
    try:
        media_player = MediaPlayer(args.rtsp_url, options={'rtsp_transport': 'tcp', 'stimeout': '5000000'})
        await asyncio.sleep(2)
        if media_player.video:
            logger.info(f"Successfully connected to RTSP stream and found video track: {args.rtsp_url}")
            # Optional background task to keep RTSP alive if needed
            # async def consume_frames(): ...
            # asyncio.create_task(consume_frames())
        else:
            logger.error(f"Connected to RTSP but could not find video track: {args.rtsp_url}")
            media_player = None
    except Exception as e:
        logger.error(f"Failed to initialize MediaPlayer for {args.rtsp_url}: {e}", exc_info=True)
        media_player = None
    if media_player is None:
        logger.critical("!!! Failed to setup RTSP media source. Server might not function correctly. !!!")

async def on_shutdown(app):
    logger.info("Shutting down server...")
    logger.info(f"Closing {len(pcs)} active peer connection(s)...")
    coros = [pc.close() for pc in list(pcs)]
    await asyncio.gather(*coros, return_exceptions=True)
    pcs.clear()
    logger.info("Peer connections closed.")
    if media_player and media_player.video:
        logger.info("Stopping media player video track...")
        logger.info("Media player video track cleanup attempted.")
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebRTC RTSP Proxy (Signaling Server)")
    parser.add_argument("--rtsp-url", required=True, help="The URL of the RTSP stream")
    parser.add_argument("--host", default="0.0.0.0", help="Host for HTTP signaling server (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Port for HTTP signaling server (default: 8080)")
    parser.add_argument("--cert-file", help="SSL certificate file (for HTTPS)")
    parser.add_argument("--key-file", help="SSL key file (for HTTPS)")
    args = parser.parse_args()
    ssl_context = None
    protocol = "http"
    if args.cert_file and args.key_file:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_context.load_cert_chain(args.cert_file, args.key_file)
            logger.info("SSL context loaded successfully. Server will use HTTPS.")
            protocol = "https"
        except Exception as e:
            logger.error(f"Error loading SSL certificate/key: {e}. Serving over HTTP instead.", exc_info=True)
            ssl_context = None
    else:
        logger.info("SSL certificate/key not provided. Serving over HTTP.")
        logger.warning("WebRTC typically requires HTTPS for client connections from non-localhost addresses.")
    app = web.Application()
    app["args"] = args
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app.router.add_post("/offer", offer)
    server_url = f"{protocol}://{args.host}:{args.port}"
    logger.info(f"Starting signaling server on {server_url}")
    logger.info(f"RTSP Source: {args.rtsp_url}")
    web.run_app(app, host=args.host, port=args.port, ssl_context=ssl_context)