// data_receiver_server.js (uWebSockets.js version with Manual Broadcasting)
const uWS = require('uWebSockets.js');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130; // Connection is closed if idle for this duration.

let listenSocketPublic, listenSocketInternal;

// This Set will hold all connected Android clients to manually iterate over for broadcasting.
// This replicates the behavior of the original `ws` server's `wss.clients`.
const androidClients = new Set();

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
            // Android clients are not expected to send messages.
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
            // Manual broadcast loop, as requested.
            // This iterates over every client in our Set and sends the message.
            if (androidClients.size > 0) {
                androidClients.forEach(client => {
                    // uWebSockets.js's .send() handles backpressure by returning true/false.
                    // For this simple forwarding case, we can send directly.
                    // A try-catch is a safeguard against potential errors on a closing socket.
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
