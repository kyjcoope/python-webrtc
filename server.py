import argparse
import asyncio
import json
import logging
import os
import ssl
import time
import struct
from typing import Set, Optional, Dict, Tuple # Added Tuple hint

from aiohttp import web
import aiohttp
# Corrected aiortc imports - ONLY import what's needed and available at top level
from aiortc import (
    MediaStreamTrack,
    RTCPeerConnection,
    RTCSessionDescription,
    RTCRtpReceiver,
    # REMOVED RTCRtpTransceiverInit,
    # REMOVED TransceiverDirection,
)
from aiortc.contrib.media import MediaRelay
from aiortc.mediastreams import VIDEO_TIME_BASE, VIDEO_CLOCK_RATE

try:
    import av
    av_available = True
except ImportError:
    print("Warning: PyAV library not found. Install with 'pip install av'")
    av_available = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Global Variables ---
pcs: Set[RTCPeerConnection] = set()
frame_queues: Dict[str, asyncio.Queue[Optional[Tuple[int, bytes]]]] = {}
source_tracks: Dict[str, 'H264InputTrack'] = {}
relays: Dict[str, MediaRelay] = {}
stream_management_lock = asyncio.Lock()
# --- End Global Variables ---

class H264InputTrack(MediaStreamTrack):
    kind = "video"
    def __init__(self, queue: asyncio.Queue[Optional[Tuple[int, bytes]]], stream_id: str): # Updated hint
        super().__init__()
        self._queue = queue
        self._stream_id = stream_id
        self._start_time: Optional[float] = None
        logger.info(f"H264InputTrack initialized for stream '{self._stream_id}'")

    async def recv(self) -> av.Packet: # Return type hint
        """Fetch the next frame data from the queue and return as an AV Packet."""
        if self._start_time is None:
            self._start_time = time.time()
            logger.info(f"H264InputTrack '{self._stream_id}': First frame requested, recording start time {self._start_time:.3f}.")

        queued_item = await self._queue.get()

        if queued_item is None:
             logger.info(f"H264InputTrack '{self._stream_id}': Received stop sentinel from queue.")
             self.stop()
             raise StopAsyncIteration

        pts_ns, frame_data = queued_item
        logger.debug(f"H264InputTrack '{self._stream_id}': Dequeued frame, pts_ns={pts_ns}, size={len(frame_data)}")

        if not av_available:
            logger.error(f"H264InputTrack '{self._stream_id}': PyAV not available, cannot create av.Packet!")
            self.stop()
            raise StopAsyncIteration("PyAV library is required but not installed.")

        time_since_start_sec = (pts_ns / 1e9) - self._start_time

        if time_since_start_sec < -0.1:
            logger.warning(f"H264InputTrack '{self._stream_id}': Frame timestamp ({pts_ns / 1e9:.3f}) significantly earlier than start time ({self._start_time:.3f}). Resetting start time.")
            self._start_time = pts_ns / 1e9
            time_since_start_sec = 0.0
        elif time_since_start_sec < 0:
             logger.debug(f"H264InputTrack '{self._stream_id}': Slightly negative time_since_start ({time_since_start_sec:.3f}s), clamping PTS to 0.")
             time_since_start_sec = 0.0

        pts = int(time_since_start_sec * VIDEO_CLOCK_RATE)

        try:
            packet = av.Packet(frame_data)
            packet.pts = pts
            packet.time_base = VIDEO_TIME_BASE

            is_keyframe = False
            if len(frame_data) > 4:
                if frame_data[0:4] == b'\x00\x00\x00\x01':
                    nal_unit_type = frame_data[4] & 0x1F
                    if nal_unit_type == 5: is_keyframe = True
                elif frame_data[0:3] == b'\x00\x00\x01':
                    if len(frame_data) > 3:
                       nal_unit_type = frame_data[3] & 0x1F
                       if nal_unit_type == 5: is_keyframe = True

            packet.is_keyframe = is_keyframe
            log_level = logging.DEBUG if not is_keyframe else logging.INFO # Log keyframes at INFO
            logger.log(log_level, f"H264InputTrack '{self._stream_id}': Sending {'IDR' if is_keyframe else 'P'}-frame, PTS={pts}, Key={is_keyframe}, Size={len(frame_data)}")

        except Exception as e:
             logger.error(f"H264InputTrack '{self._stream_id}': Error creating av.Packet: {e}", exc_info=True)
             self._queue.task_done()
             return await self.recv() # Try next frame

        self._queue.task_done()
        return packet

    def stop(self):
        """Stops the track and signals the consumer by putting None in the queue."""
        if not getattr(self, '_MediaStreamTrack__ended', True):
            logger.info(f"H264InputTrack '{self._stream_id}': Stopping...")
            while not self._queue.empty():
                try:
                    item = self._queue.get_nowait()
                    self._queue.task_done()
                    logger.debug(f"H264InputTrack '{self._stream_id}': Discarding frame during stop.")
                except asyncio.QueueEmpty: break
                except Exception as e:
                    logger.warning(f"H264InputTrack '{self._stream_id}': Error draining queue during stop: {e}")
                    break
            try:
                 self._queue.put_nowait(None)
            except asyncio.QueueFull: logger.error(f"H264InputTrack '{self._stream_id}': Could not add stop sentinel, queue full!")
            except Exception as e: logger.error(f"H264InputTrack '{self._stream_id}': Error adding stop sentinel: {e}")

            super().stop()
            logger.info(f"H264InputTrack '{self._stream_id}': Signalled stop.")
        else:
             logger.debug(f"H264InputTrack '{self._stream_id}' already stopped or ending.")

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    peername = request.transport.get_extra_info('peername')
    host, port = peername if peername else ('unknown_host', 0)

    try:
        stream_id = request.match_info['stream_id']
        if not stream_id: raise ValueError("Stream ID cannot be empty")
        logger.info(f"WebSocket client {host}:{port} attempting connection for stream_id: '{stream_id}'")
    except KeyError:
        logger.error(f"WebSocket connection from {host}:{port} missing stream_id in URL path.")
        await ws.close(code=aiohttp.WSCloseCode.POLICY_VIOLATION, message=b"Missing stream_id")
        return ws
    except ValueError as e:
        logger.error(f"WebSocket connection from {host}:{port} invalid stream_id: {e}")
        await ws.close(code=aiohttp.WSCloseCode.POLICY_VIOLATION, message=f"Invalid stream_id: {e}".encode())
        return ws

    logger.info(f"WebSocket client connected for stream '{stream_id}': {host}:{port}")

    input_queue: Optional[asyncio.Queue[Optional[Tuple[int, bytes]]]] = None
    is_first_sender = False

    async with stream_management_lock:
        if stream_id not in frame_queues:
            logger.info(f"First sender for stream '{stream_id}'. Creating resources.")
            is_first_sender = True
            input_queue = asyncio.Queue(maxsize=60)
            frame_queues[stream_id] = input_queue
            source_tracks[stream_id] = H264InputTrack(input_queue, stream_id)
            relays[stream_id] = MediaRelay()
            logger.info(f"Resources created and assigned for stream '{stream_id}'.")
        else:
            input_queue = frame_queues.get(stream_id)
            if input_queue is None:
                 logger.error(f"Inconsistent state: stream '{stream_id}' key exists but queue is None!")
                 await ws.close(code=aiohttp.WSCloseCode.INTERNAL_ERROR, message=b"Internal server error")
                 return ws
            logger.info(f"Existing stream '{stream_id}'. Client {host}:{port} will feed the shared queue.")

    websocket_closed = False
    try:
        async for msg in ws:
            if websocket_closed: break
            if msg.type == aiohttp.WSMsgType.BINARY:
                frame_data = msg.data
                timestamp_ns = time.time_ns()
                if input_queue.full():
                    try:
                        dropped_ts, dropped_data = input_queue.get_nowait()
                        input_queue.task_done()
                        logger.warning(f"Queue for '{stream_id}' full! Dropped frame (size {len(dropped_data)}, ts {dropped_ts}).")
                    except asyncio.QueueEmpty:
                        logger.warning(f"Queue for '{stream_id}' full but get_nowait failed.")
                        pass
                await input_queue.put((timestamp_ns, frame_data))
                # Reduce verbosity of frame logging
                # logger.debug(f"WS '{stream_id}': Received binary frame (size {len(frame_data)}), put in queue.")
            elif msg.type == aiohttp.WSMsgType.TEXT: logger.info(f"WS '{stream_id}' received text from {host}:{port} (ignored): {msg.data}")
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f"WS connection for '{stream_id}' ({host}:{port}) closed with exception: {ws.exception()}")
                websocket_closed = True; break
            elif msg.type == aiohttp.WSMsgType.CLOSE:
                 logger.info(f"WS connection for '{stream_id}' received close from {host}:{port}, code={ws.close_code}")
                 websocket_closed = True; break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                 logger.info(f"WS connection for '{stream_id}' detected as closed for {host}:{port}")
                 websocket_closed = True; break
    except asyncio.CancelledError:
        logger.info(f"WS handler task for '{stream_id}' ({host}:{port}) cancelled.")
        websocket_closed = True
    except Exception as e:
        logger.error(f"Error in WS handler for '{stream_id}' ({host}:{port}): {e}", exc_info=True)
        websocket_closed = True
    finally:
        logger.info(f"WS client for stream '{stream_id}' ({host}:{port}) disconnecting.")
        if not ws.closed:
            await ws.close(code=aiohttp.WSCloseCode.GOING_AWAY, message=b'Handler finished')

        if is_first_sender:
            logger.info(f"Original sender for '{stream_id}' disconnected. Initiating resource cleanup.")
            async with stream_management_lock:
                track_to_stop = source_tracks.pop(stream_id, None)
                queue_to_clear = frame_queues.pop(stream_id, None) # Keep pop atomic with track
                relay_to_remove = relays.pop(stream_id, None)
            if track_to_stop:
                logger.info(f"Stopping source track for stream '{stream_id}'.")
                track_to_stop.stop()
            if relay_to_remove: logger.info(f"Relay object for '{stream_id}' marked for removal.")
            logger.info(f"Resource cleanup for stream '{stream_id}' completed.")
        else:
            logger.info(f"Non-original sender for '{stream_id}' disconnected. Shared resources remain.")
    return ws

# Offer function using string directions
# Offer function - Explicit Transceiver Creation First

async def offer(request):
    """Handles incoming WebRTC offer requests using explicit transceiver setup first."""
    try:
        stream_id = request.match_info['stream_id']
        if not stream_id: raise ValueError("Stream ID cannot be empty")
        logger.info(f"Offer request received for stream_id: '{stream_id}' from {request.remote}")
    except KeyError:
        logger.error(f"Offer request from {request.remote} missing stream_id.")
        return web.Response(status=400, text="Missing stream_id")
    except ValueError as e:
        logger.error(f"Offer request from {request.remote} invalid stream_id: {e}")
        return web.Response(status=400, text=f"Invalid stream_id: {e}")

    try:
        params = await request.json()
        sdp = params.get("sdp")
        sdp_type = params.get("type")
        if not sdp or not sdp_type or sdp_type != "offer":
            logger.error(f"Invalid offer body for '{stream_id}' from {request.remote}. Params: {params}")
            return web.Response(content_type="application/json", text=json.dumps({"error": "Invalid request body"}), status=400)
        offer_desc = RTCSessionDescription(sdp=sdp, type=sdp_type)
        logger.info(f"Parsed WebRTC offer for '{stream_id}' from {request.remote}")
        # logger.debug(f"{pc_id}: OFFER SDP received:\n{sdp}")
    except json.JSONDecodeError:
        body_text = await request.text()
        logger.error(f"Failed JSON parse for offer '{stream_id}' from {request.remote}. Body: '{body_text[:200]}...'")
        return web.Response(content_type="application/json", text=json.dumps({"error": "Invalid JSON"}), status=400)
    except Exception as e:
        logger.error(f"Error processing params for '{stream_id}' from {request.remote}: {e}", exc_info=True)
        return web.Response(content_type="application/json", text=json.dumps({"error": f"Error processing request: {e}"}), status=400)

    pc = RTCPeerConnection()
    pcs.add(pc)
    pc_id = f"PC-{id(pc)}-{stream_id}"

    # --- Event Handlers (same as before) ---
    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        logger.info(f"{pc_id}: Connection state: {pc.connectionState}")
        if pc.connectionState in ["failed", "closed", "disconnected"]:
            logger.warning(f"{pc_id}: Closing PC state: {pc.connectionState}")
            if pc in pcs: pcs.discard(pc); logger.info(f"{pc_id}: Removed PC.")
            asyncio.ensure_future(pc.close())

    @pc.on("iceconnectionstatechange")
    async def on_iceconnectionstatechange():
        logger.info(f"{pc_id}: ICE state: {pc.iceConnectionState}")
        if pc.iceConnectionState in ["failed", "closed", "disconnected"]:
            logger.warning(f"{pc_id}: Closing PC ICE state: {pc.iceConnectionState}")
            if pc in pcs: pcs.discard(pc); logger.info(f"{pc_id}: Removed PC (ICE).")
            asyncio.ensure_future(pc.close())

    @pc.on("track")
    def on_track(track):
        logger.warning(f"{pc_id}: Track {track.kind} ({track.id}) received unexpectedly. Stopping.")
        if hasattr(track, 'stop') and callable(track.stop):
           try: track.stop()
           except Exception as e: logger.error(f"{pc_id}: Error stopping track {track.id}: {e}")
    # --- End Event Handlers ---

    async with stream_management_lock:
        source_track: Optional[H264InputTrack] = source_tracks.get(stream_id)
        relay: Optional[MediaRelay] = relays.get(stream_id)

    if source_track is None or relay is None:
        logger.error(f"{pc_id}: Stream '{stream_id}' not found or sender not connected.")
        if pc in pcs: pcs.discard(pc)
        await pc.close()
        return web.Response(content_type="application/json", text=json.dumps({"error": f"Stream '{stream_id}' not available"}), status=404)

    try:
        if getattr(source_track, '_MediaStreamTrack__ended', False):
             raise RuntimeError(f"Source track '{stream_id}' stopped.")
        relayed_track = relay.subscribe(source_track)
        logger.info(f"{pc_id}: Subscribed to relayed track '{stream_id}'.")
    except Exception as e:
        logger.error(f"{pc_id}: Failed subscribe relay '{stream_id}': {e}", exc_info=True)
        if pc in pcs: pcs.discard(pc)
        await pc.close()
        return web.Response(content_type="application/json", text=json.dumps({"error": f"Error subscribing: {e}"}), status=500)

    try:
        # *** STEP 1: Explicitly add transceivers for what WE want to send/receive ***
        # We want to SEND video
        logger.info(f"{pc_id}: Adding video transceiver with track (direction=sendonly initially)...")
        video_transceiver = pc.addTransceiver(relayed_track, direction="sendonly")
        # We do NOT want to send or receive audio
        logger.info(f"{pc_id}: Adding audio transceiver (direction=inactive)...")
        audio_transceiver = pc.addTransceiver("audio", direction="inactive")

        # *** STEP 2: Set the REMOTE description (offer) ***
        # This will potentially update the directions of our existing transceivers
        # based on the offer's requests (e.g., if offer is recvonly, video might become sendrecv).
        logger.info(f"{pc_id}: Setting remote description (offer)...")
        await pc.setRemoteDescription(offer_desc)
        logger.info(f"{pc_id}: Remote description (offer) set.")

        # Log transceiver states *after* setRemoteDescription for debugging
        logger.debug(f"{pc_id}: Transceivers after setRemoteDescription:")
        for t in pc.getTransceivers():
            offer_dir = getattr(t, '_offerDirection', 'N/A') # Internal attribute, might change
            negotiated_dir = getattr(t, '_negotiatedDirection', 'N/A') # Internal attribute
            logger.debug(f"  - Kind: {t.kind}, MID: {t.mid}, CurrentDir: {t.direction}, OfferDir: {offer_dir}, NegDir: {negotiated_dir}, Sender: {t.sender is not None}, Receiver: {t.receiver is not None}")


        # *** STEP 3: Create the ANSWER ***
        # The answer should reflect the negotiated state based on our added transceivers
        # and the remote offer.
        logger.info(f"{pc_id}: Creating answer...")
        answer = await pc.createAnswer()
        logger.info(f"{pc_id}: Answer created.")
        logger.info(f"{pc_id}: FULL ANSWER SDP (before setLocal):\n{answer.sdp}")


        # *** STEP 4: Set the LOCAL description (answer) ***
        logger.info(f"{pc_id}: Setting local description (answer)...")
        await pc.setLocalDescription(answer) # <<< Error occurred here previously
        logger.info(f"{pc_id}: Local description (answer) set successfully.")

        # --- Final Checks ---
        if not (pc.localDescription and pc.localDescription.sdp):
            logger.error(f"{pc_id}: Local description/SDP missing after setting!")
        # Check if the sender we expect is active
        final_video_sender = next((t.sender for t in pc.getTransceivers() if t.sender and t.sender.track == relayed_track), None)
        if not final_video_sender: logger.warning(f"{pc_id}: Could not verify sender association after negotiation.")
        else: logger.info(f"{pc_id}: Verified video sender association is present.")


    except Exception as e:
        logger.error(f"{pc_id}: Error during offer/answer negotiation: {e}", exc_info=True) # Log full traceback
        if pc in pcs: pcs.discard(pc); logger.info(f"{pc_id}: Removed PC on error.")
        await pc.close()
        error_message = f"Failed negotiation processing for '{stream_id}': {e}"
        return web.Response(content_type="application/json", text=json.dumps({"error": error_message}), status=500)

    # --- Success ---
    logger.info(f"{pc_id}: Negotiation successful. Sending answer to {request.remote}")
    response_data = {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}
    return web.Response(content_type="application/json", text=json.dumps(response_data), status=200)


async def on_startup(app):
    logger.info("Server starting up...")
    logger.info("Stream management dictionaries initialized.")
    # app["pcs"] = pcs # Optional: Store in app context

async def on_shutdown(app):
    logger.info("Shutting down server...")
    active_pcs = list(pcs)
    logger.info(f"Closing {len(active_pcs)} active peer connection(s)...")
    await asyncio.gather(*(pc.close() for pc in active_pcs), return_exceptions=True)
    pcs.clear()
    logger.info("Peer connections closed.")

    logger.info(f"Stopping {len(source_tracks)} active source track(s)...")
    async with stream_management_lock:
        tracks_to_stop = list(source_tracks.values())
        for track in tracks_to_stop:
            logger.info(f"Stopping track '{track._stream_id}' during shutdown...")
            track.stop()
        source_tracks.clear()
        frame_queues.clear()
        relays.clear()
    await asyncio.sleep(0.2)
    logger.info("Stream resources cleared.")
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebRTC Multi-Stream H.264 Input Server")
    parser.add_argument("--host", default="localhost", help="Host (default: localhost)")
    parser.add_argument("--port", type=int, default=8080, help="Port (default: 8080)")
    parser.add_argument("--cert-file", help="SSL certificate file")
    parser.add_argument("--key-file", help="SSL key file")
    parser.add_argument("-v", "--verbose", help="Increase logging verbosity", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logging.getLogger("aiortc").setLevel(logging.INFO) # INFO or DEBUG for aiortc
        logging.getLogger("aiohttp").setLevel(logging.INFO)
        logger.info("Verbose logging enabled.")
    else:
        logging.basicConfig(level=logging.INFO)
        logger.setLevel(logging.INFO)
        logging.getLogger("aiortc").setLevel(logging.WARN)

    ssl_context = None
    protocol, ws_protocol = "http", "ws"
    if args.cert_file and args.key_file:
        if not os.path.exists(args.cert_file): logger.error(f"Cert file not found: {args.cert_file}"); exit(1)
        if not os.path.exists(args.key_file): logger.error(f"Key file not found: {args.key_file}"); exit(1)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_context.load_cert_chain(args.cert_file, args.key_file)
            logger.info(f"SSL context loaded from {args.cert_file}, {args.key_file}.")
            protocol, ws_protocol = "https", "wss"
        except Exception as e:
            logger.error(f"Error loading SSL cert/key: {e}. Serving HTTP.", exc_info=True)
            ssl_context = None
    else:
        logger.info("SSL cert/key not provided. Serving HTTP/WS.")

    app = web.Application()
    app["args"] = args
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app.router.add_post("/offer/{stream_id}", offer)
    app.router.add_get("/ws/{stream_id}", websocket_handler)

    server_url = f"{protocol}://{args.host}:{args.port}"
    logger.info(f"Starting server on {server_url}")
    logger.info(f"WebSocket: {ws_protocol}://{args.host}:{args.port}/ws/{{stream_id}}")
    logger.info(f"Signaling: {protocol}://{args.host}:{args.port}/offer/{{stream_id}}")

    try:
        web.run_app(app, host=args.host, port=args.port, ssl_context=ssl_context, access_log=None)
    except OSError as e: logger.error(f"Failed start server {args.host}:{args.port}. Error: {e}")
    except Exception as e: logger.error(f"Unexpected server startup error: {e}", exc_info=True)