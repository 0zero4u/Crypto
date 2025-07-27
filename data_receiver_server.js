// data_receiver_server.js (uWebSockets.js version with Manual Broadcasting)
const uWS = require('uWebSockets.js');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130; // Connection is closed if idle for this duration.

let listenSocketPublic, listenSocketInternal;

// This Set will hold all connected Android clients to manually iterate over for broadcasting.
const androidClients = new Set();

// **NEW**: This object will store the last message and its format (binary/text)
// received from the internal listener, so we can send it on demand.
let lastBroadcastData = {
    message: null,
    isBinary: false
};


uWS.App({})
    // --- Public WebSocket Server (for Android Clients) ---
    .ws('/public', {
        compression: uWS.SHARED_COMPRESSOR,
        maxPayloadLength: 16 * 1024,
        idleTimeout: IDLE_TIMEOUT_SECONDS,

        open: (ws) => {
            const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] Public client connected from ${clientIp}. Adding to broadcast set.`);
            
            // Add the newly connected client to our manual collection.
            androidClients.add(ws);
        },
        message: (ws, message, isBinary) => {
            // **MODIFIED**: Handle specific messages from Android clients.
            try {
                // uWebSockets.js message is an ArrayBuffer. Convert to string for parsing.
                const messageString = Buffer.from(message).toString();
                const clientCommand = JSON.parse(messageString);

                // Check for the special "semi_auto" mode request.
                if (clientCommand.event === 'set_mode' && clientCommand.mode === 'semi_auto') {
                    console.log(`[Receiver] Client requested 'semi_auto' mode.`);
                    
                    // If we have a last known price, send it immediately to THIS client.
                    if (lastBroadcastData.message) {
                        try {
                            console.log(`[Receiver] Sending last known price to the requesting client.`);
                            ws.send(lastBroadcastData.message, lastBroadcastData.isBinary);
                        } catch (e) {
                            console.error(`[Receiver] FAILED to send immediate price to client: ${e.message}`);
                        }
                    } else {
                        console.log(`[Receiver] Client requested last price, but no price has been received yet.`);
                    }
                }
            } catch (e) {
                // This will catch non-JSON messages or parsing errors.
                // We can ignore them as they are not valid commands.
                // console.log(`[Receiver] Received non-command message from public client.`);
            }
        },
        close: (ws, code, message) => {
            console.log(`[Receiver] Public client disconnected. Removing from broadcast set.`);

            // IMPORTANT: Remove the client from the collection on disconnect.
            androidClients.delete(ws);
        }
    })
    // --- Internal WebSocket Server (for binance_listener.js) ---
    .ws('/internal', {
        compression: uWS.DISABLED,
        maxPayloadLength: 4 * 1024,
        idleTimeout: 30,

        open: (ws) => {
            console.log('[Receiver] Internal listener connected.');
        },
        message: (ws, message, isBinary) => {
            // **MODIFIED**: Store the latest message before broadcasting it.
            lastBroadcastData.message = message;
            lastBroadcastData.isBinary = isBinary;

            // Manual broadcast loop to all connected Android clients.
            if (androidClients.size > 0) {
                androidClients.forEach(client => {
                    try {
                        client.send(message, isBinary);
                    } catch (e) {
                        console.error(`[Receiver] Error sending to a client: ${e.message}`);
                    }
                });
            }
        },
        close: (ws, code, message) => {
            console.log('[Receiver] Internal listener disconnected.');
        }
    })
    .listen(PUBLIC_PORT, (token) => {
        listenSocketPublic = token;
        if (token) {
            console.log(`[Receiver] Public WebSocket server listening on port ${PUBLIC_PORT}`);
        } else {
            console.error(`[Receiver] FAILED to listen on port ${PUBLIC_PORT}`);
            process.exit(1);
        }
    })
    .listen(INTERNAL_LISTENER_PORT, (token) => {
        listenSocketInternal = token;
        if (token) {
            console.log(`[Receiver] Internal WebSocket server listening on port ${INTERNAL_LISTENER_PORT}`);
        } else {
            console.error(`[Receiver] FAILED to listen on port ${INTERNAL_LISTENER_PORT}`);
            process.exit(1);
        }
    });

// --- Graceful Shutdown ---
function shutdown() {
    console.log('[Receiver] Shutting down...');
    if (listenSocketPublic) {
        uWS.us_listen_socket_close(listenSocketPublic);
    }
    if (listenSocketInternal) {
        uWS.us_listen_socket_close(listenSocketInternal);
    }
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(`[Receiver] PID: ${process.pid} --- Server initialized in manual broadcast mode.`);
