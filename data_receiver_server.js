// data_receiver_server.js (uWebSockets.js version with Manual Broadcasting)
const uWS = require('uWebSockets.js');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130; // Connection is closed if idle for this duration.

let listenSocketPublic, listenSocketInternal;

// This Set will hold all connected Android clients to manually iterate over for broadcasting.
const androidClients = new Set();
// Store the last received payload from the internal listener to send on demand.
let lastKnownPricePayload = { message: null, isBinary: false };

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
            // Android clients can now send a message to request the last price.
            try {
                const decodedMessage = Buffer.from(message).toString();
                const data = JSON.parse(decodedMessage);

                if (data.event === 'set_mode' && data.mode === 'semi_auto') {
                    console.log('[Receiver] Received "set_mode: semi_auto" request from a client.');
                    
                    // If we have a last known price, send it immediately to THIS client.
                    if (lastKnownPricePayload.message) {
                        console.log(`[Receiver] Sending last known price to the requesting client.`);
                        ws.send(lastKnownPricePayload.message, lastKnownPricePayload.isBinary);
                    } else {
                        console.log('[Receiver] No last known price available to send for semi_auto mode.');
                    }
                }
            } catch (e) {
                // Silently ignore messages that are not in the expected JSON format.
            }
        },
        close: (ws, code, message) => {
            console.log(`[Receiver] Public client disconnected. Removing from broadcast set.`);

            // IMPORTANT: Remove the client from the collection on disconnect to prevent memory leaks.
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
            // Store the latest message before broadcasting.
            // We store the raw message (which is an ArrayBuffer) to avoid re-processing.
            lastKnownPricePayload.message = message;
            lastKnownPricePayload.isBinary = isBinary;
            
            // Manual broadcast loop.
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
