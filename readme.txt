Okay, let's create a Python server using aiortc for WebRTC and aiohttp for signaling. aiortc conveniently includes media helpers that can often connect directly to RTSP streams using FFmpeg behind the scenes (via the pyav library). This makes the setup relatively straightforward.

Prerequisites:

Python 3.7+: Make sure you have a compatible Python version installed.

pip: Python's package installer.

FFmpeg: aiortc's media handling often relies on FFmpeg libraries (libavformat, libavcodec, etc.). You need to install FFmpeg on your system.

Ubuntu/Debian: sudo apt update && sudo apt install ffmpeg libavdevice-dev libavfilter-dev libopus-dev libvpx-dev pkg-config

macOS: brew install ffmpeg pkg-config

Windows: Download binaries from the FFmpeg website and add them to your system's PATH.

Local RTSP Stream: You need the URL of your H.264 camera stream (e.g., rtsp://user:password@192.168.1.100:554/stream1).

Project Setup:

Create a project directory: mkdir webrtc_rtsp_server && cd webrtc_rtsp_server

Create a virtual environment (recommended): python -m venv venv

Activate the virtual environment:

Linux/macOS: source venv/bin/activate

Windows: .\venv\Scripts\activate

Install necessary Python libraries:

pip install aiohttp aiortc pyav Pillow # Pillow might be needed by some aiortc examples/internals


Note: pyav provides Python bindings for FFmpeg libraries.

Code:

Create two files in your project directory: server.py and client.html.

1. server.py

import argparse
import asyncio
import json
import logging
import os
import ssl
from typing import Dict, Optional, Set

from aiohttp import web
from aiortc import MediaStreamTrack, RTCPeerConnection, RTCSessionDescription
from aiortc.contrib.media import MediaPlayer, MediaRelay

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global set to keep track of peer connections
pcs: Set[RTCPeerConnection] = set()

# Global variable for the media source (MediaPlayer)
media_player: Optional[MediaPlayer] = None
relay = MediaRelay() # Use relay to avoid creating a new MediaPlayer for each connection

class RTSPVideoTrack(MediaStreamTrack):
    """
    A custom video track that relays frames from the MediaPlayer.
    """
    kind = "video"

    def __init__(self, track):
        super().__init__()  # Don't forget this!
        self._track = track

    async def recv(self):
        frame = await self._track.recv()
        return frame

async def serve_client(request):
    """Serve the client HTML file."""
    content = open(os.path.join(os.path.dirname(__file__), "client.html"), "r").read()
    return web.Response(content_type="text/html", text=content)

async def offer(request):
    """Handle the WebRTC offer from the client."""
    params = await request.json()
    offer = RTCSessionDescription(sdp=params["sdp"], type=params["type"])

    pc = RTCPeerConnection()
    pcs.add(pc)

    @pc.on("connectionstatechange")
    async def on_connectionstatechange():
        logger.info(f"Connection state is {pc.connectionState}")
        if pc.connectionState == "failed" or pc.connectionState == "closed":
            await pc.close()
            pcs.discard(pc)
            logger.info("Peer connection closed")

    @pc.on("track")
    def on_track(track):
        # We are only sending video, not receiving. Log if we receive something unexpected.
        logger.warning(f"Track {track.kind} received, but not handled")
        # If you wanted to handle incoming tracks (e.g., audio backchannel), you'd do it here.
        # @track.on("ended")
        # async def on_ended():
        #     logger.info(f"Track {track.kind} ended")

    # Get the video track from the relay
    # This ensures we only have one connection to the RTSP source
    video_track = relay.subscribe(media_player.video)

    # Add the track to the peer connection
    pc.addTrack(RTSPVideoTrack(video_track)) # Use the wrapper if needed, or directly video_track
    logger.info("Video track added to peer connection")

    try:
        # Set the remote description (the offer)
        await pc.setRemoteDescription(offer)
        logger.info("Remote description (offer) set")

        # Create the answer
        answer = await pc.createAnswer()
        logger.info("Answer created")

        # Set the local description (the answer)
        await pc.setLocalDescription(answer)
        logger.info("Local description (answer) set")

    except Exception as e:
        logger.error(f"Error during offer/answer exchange: {e}")
        await pc.close()
        pcs.discard(pc)
        return web.Response(
            content_type="application/json",
            text=json.dumps({"error": str(e)}),
            status=500
        )

    logger.info("Sending answer to client")
    return web.Response(
        content_type="application/json",
        text=json.dumps(
            {"sdp": pc.localDescription.sdp, "type": pc.localDescription.type}
        ),
    )

async def on_shutdown(app):
    """Cleanup resources on server shutdown."""
    logger.info("Shutting down server...")
    # Close all peer connections
    coros = [pc.close() for pc in pcs]
    await asyncio.gather(*coros)
    pcs.clear()
    logger.info("Peer connections closed.")

    # Stop the media player
    if media_player:
        logger.info("Stopping media player...")
        await media_player.video.stop() # Use await if stop is async (check aiortc version/docs)
        # Check if MediaPlayer itself has an async close/stop method
        logger.info("Media player stopped.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="WebRTC RTSP proxy server")
    parser.add_argument("--rtsp-url", required=True, help="The URL of the RTSP stream")
    parser.add_argument("--host", default="0.0.0.0", help="Host for HTTP server (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Port for HTTP server (default: 8080)")
    parser.add_argument("--cert-file", help="SSL certificate file (for HTTPS)")
    parser.add_argument("--key-file", help="SSL key file (for HTTPS)")

    args = parser.parse_args()

    # Configure SSL if certificate and key files are provided
    ssl_context = None
    if args.cert_file and args.key_file:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        try:
            ssl_context.load_cert_chain(args.cert_file, args.key_file)
            logger.info("SSL context loaded successfully.")
        except Exception as e:
            logger.error(f"Error loading SSL certificate/key: {e}. Serving over HTTP instead.")
            ssl_context = None # Fallback to HTTP
    else:
        logger.info("SSL certificate/key not provided. Serving over HTTP.")
        logger.warning("WebRTC requires HTTPS (or localhost) to function in most modern browsers.")


    # Initialize the media player (connects to RTSP)
    # Use options={'rtsp_transport': 'tcp'} if UDP causes issues
    # format='h264' might help aiortc understand the input better
    media_player = MediaPlayer(
        args.rtsp_url,
        # format="h264", # Sometimes helps, sometimes hinders. Test with your stream.
        options={'rtsp_transport': 'tcp'} # TCP often more reliable over internet/wifi
    )
    if media_player.video is None:
       logger.error(f"Could not open video stream at {args.rtsp_url}")
       exit(1)
    logger.info(f"Successfully connected to RTSP stream: {args.rtsp_url}")


    # Setup aiohttp web application
    app = web.Application()
    app.on_shutdown.append(on_shutdown)
    app.router.add_get("/", serve_client)
    app.router.add_post("/offer", offer)

    # Run the web server
    logger.info(f"Starting server on {args.host}:{args.port}...")
    web.run_app(
        app,
        host=args.host,
        port=args.port,
        ssl_context=ssl_context
    )
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Python
IGNORE_WHEN_COPYING_END

2. client.html

<!DOCTYPE html>
<html>
<head>
    <title>WebRTC RTSP Stream</title>
    <style>
        body {
            font-family: sans-serif;
            padding: 20px;
        }
        video {
            max-width: 100%;
            border: 1px solid #ccc;
            background-color: #000;
        }
        #status {
            margin-top: 10px;
            font-weight: bold;
        }
        button {
            padding: 10px 15px;
            font-size: 1em;
            margin-bottom: 10px;
        }
    </style>
</head>
<body>

    <h1>WebRTC RTSP Stream Viewer</h1>
    <p>Connects to the Python server to view the proxied RTSP stream.</p>

    <video id="video" autoplay playsinline controls></video>
    <br>
    <button id="connectButton">Connect</button>
    <button id="disconnectButton" disabled>Disconnect</button>
    <div id="status">Status: Disconnected</div>

    <script>
        const connectButton = document.getElementById('connectButton');
        const disconnectButton = document.getElementById('disconnectButton');
        const videoElement = document.getElementById('video');
        const statusElement = document.getElementById('status');

        let pc = null; // PeerConnection

        function updateStatus(message) {
            console.log(message);
            statusElement.textContent = `Status: ${message}`;
        }

        async function connect() {
            if (pc) {
                updateStatus("Already connected or connecting.");
                return;
            }

            connectButton.disabled = true;
            updateStatus("Connecting...");

            // --- Check for HTTPS ---
            // WebRTC requires HTTPS or localhost for secure context
            if (location.protocol !== 'https:' && location.hostname !== 'localhost' && location.hostname !== '127.0.0.1') {
                updateStatus("Error: WebRTC requires HTTPS or localhost.");
                console.error("WebRTC requires HTTPS for secure context. Serve this page over HTTPS or access via localhost.");
                connectButton.disabled = false;
                return;
            }

            // --- Create PeerConnection ---
            const configuration = {}; // Add STUN/TURN servers if needed for NAT traversal
            pc = new RTCPeerConnection(configuration);

            pc.oniceconnectionstatechange = () => {
                updateStatus(`ICE Connection State: ${pc.iceConnectionState}`);
                if (pc.iceConnectionState === 'connected' || pc.iceConnectionState === 'completed') {
                    disconnectButton.disabled = false;
                } else if (['failed', 'disconnected', 'closed'].includes(pc.iceConnectionState)) {
                   disconnect(); // Attempt cleanup
                }
            };

            pc.onconnectionstatechange = () => {
                 updateStatus(`Connection State: ${pc.connectionState}`);
                 if (pc.connectionState === 'connected') {
                     disconnectButton.disabled = false;
                 } else if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) {
                    disconnect(); // Attempt cleanup
                 }
            };

            pc.ontrack = (event) => {
                updateStatus("Track received");
                console.log("Received track:", event.track, "Stream:", event.streams[0]);
                if (event.track.kind === 'video') {
                    if (videoElement.srcObject !== event.streams[0]) {
                        videoElement.srcObject = event.streams[0];
                        updateStatus("Streaming video...");
                    }
                }
            };

            // --- Signaling ---
            try {
                // Add a transceiver for receiving video
                // This tells the other peer we want to receive video.
                pc.addTransceiver('video', { direction: 'recvonly' });
                updateStatus("Video transceiver added.");

                // Create offer
                const offer = await pc.createOffer();
                await pc.setLocalDescription(offer);
                updateStatus("Offer created and set as local description.");

                // Send offer to server
                updateStatus("Sending offer to server...");
                const response = await fetch('/offer', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        sdp: offer.sdp,
                        type: offer.type,
                    }),
                });

                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(`Server error: ${response.status} - ${errorData.error || 'Unknown error'}`);
                }

                const answer = await response.json();
                updateStatus("Received answer from server.");

                // Set remote description (the answer)
                await pc.setRemoteDescription(new RTCSessionDescription(answer));
                updateStatus("Remote description (answer) set.");

            } catch (error) {
                updateStatus(`Error during connection: ${error.message}`);
                console.error("Connection error:", error);
                disconnect(); // Clean up on error
            }
        }

        function disconnect() {
            updateStatus("Disconnecting...");
            if (pc) {
                pc.close();
                pc = null;
            }
            videoElement.srcObject = null;
            connectButton.disabled = false;
            disconnectButton.disabled = true;
            updateStatus("Disconnected");
        }

        connectButton.onclick = connect;
        disconnectButton.onclick = disconnect;

    </script>

</body>
</html>
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Html
IGNORE_WHEN_COPYING_END

How to Run:

Make sure FFmpeg is installed and accessible.

Activate your virtual environment: source venv/bin/activate (or .\venv\Scripts\activate on Windows).

Start the server:

python server.py --rtsp-url "rtsp://your_user:your_password@your_camera_ip:554/your_stream_path"
IGNORE_WHEN_COPYING_START
content_copy
download
Use code with caution.
Bash
IGNORE_WHEN_COPYING_END

Replace the --rtsp-url with your actual camera's RTSP URL.

If your camera doesn't need authentication, it might look like rtsp://your_camera_ip/your_stream_path.

The server will run on http://0.0.0.0:8080 by default.

Access the Client:

Open your web browser (Chrome/Firefox recommended) and navigate to http://localhost:8080 (or http://<server_ip>:8080 if accessing from another device on the same network).

Important: For WebRTC to work reliably (especially with getUserMedia which we aren't using here, but it's a general rule), the webpage usually needs to be served over HTTPS or accessed via localhost.

If you access from localhost, HTTP should work.

If you access from a different IP address, you'll likely need HTTPS. You can generate a self-signed certificate for testing (use --cert-file and --key-file arguments) and accept the browser warning, or use a proper certificate. The Python code includes arguments for SSL certificate files.

Click the "Connect" button in the browser.

You should see status messages, and if everything works, the video stream from your camera will appear in the <video> element.

Key Concepts and Considerations:

aiortc.contrib.media.MediaPlayer: This class is a high-level wrapper that uses pyav (FFmpeg bindings) to open media files or streams (including RTSP). It simplifies getting media frames.

aiortc.contrib.media.MediaRelay: This prevents creating a new RTSP connection for every WebRTC client. It takes the source track (media_player.video) and allows multiple clients to subscribe to it efficiently.

Signaling: The /offer endpoint implements a basic signaling mechanism. The client sends its SDP offer, the server configures its RTCPeerConnection, generates an SDP answer, and sends it back.

H.264 Passthrough: aiortc and pyav are generally good at handling codecs. If the RTSP stream is H.264 and the browser supports H.264, aiortc will likely try to pass the encoded frames through without re-encoding, which is efficient. If not, it might fall back to VP8/VP9 encoding.

NAT Traversal (STUN/TURN): The provided code doesn't include STUN or TURN server configuration in RTCPeerConnection. This means it will likely only work when the client and server are on the same local network. For connections over the internet, you'll need to configure STUN servers (usually public ones are sufficient) or even a TURN server if dealing with restrictive firewalls/NATs.

Error Handling: The code has basic error handling, but a production system would need more robust error checking, logging, and recovery mechanisms.

HTTPS: As mentioned, WebRTC often requires a secure context (HTTPS or localhost). Plan for this if deploying beyond local testing.

RTSP Reliability: RTSP stream stability can vary. The rtsp_transport=tcp option often helps improve reliability compared to the default UDP.

Resource Management: The on_shutdown handler ensures peer connections and the media player are closed cleanly when the server stops.