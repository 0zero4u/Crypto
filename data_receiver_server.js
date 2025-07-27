// data_receiver_server.js
const uWS = require('uWebSockets.js');
const axios = require('axios');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130; // Connection is closed if idle for this duration.

// Binance API URL for an instant price quote
const BINANCE_TICKER_URL = 'https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=BTCUSDT';

let listenSocketPublic, listenSocketInternal;

// This Set will hold all connected Android clients to manually iterate over for broadcasting.
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
        message: async (ws, message, isBinary) => {
            // --- NEW: Handle client-side requests for on-demand data ---
            try {
                const request = JSON.parse(Buffer.from(message).toString());

                // Check for the specific "semi_auto" mode request from an Android client
                if (request.event === 'set_mode' && request.mode === 'semi_auto') {
                    console.log('[Receiver] Received semi_auto request. Fetching instant price for client.');

                    // Fetch the latest price from Binance REST API to avoid any delays in the main pipeline.
                    const response = await axios.get(BINANCE_TICKER_URL);
                    const lastPrice = parseFloat(response.data.lastPrice);

                    if (!isNaN(lastPrice)) {
                        // Format the payload exactly like the binance_listener does
                        const payload = {
                            type: 'S',
                            p: lastPrice
                        };
                        
                        // Send the price immediately to ONLY the requesting client.
                        // A try-catch is a safeguard against a socket that may have closed mid-request.
                        try {
                           ws.send(JSON.stringify(payload), isBinary);
                           console.log(`[Receiver] Sent instant price ${lastPrice} to semi_auto client.`);
                        } catch (e) {
                           console.error(`[Receiver] Error sending instant price to client: ${e.message}`);
                        }
                    }
                }
            } catch (e) {
                // This block catches errors from JSON.parse or axios.get.
                // We assume most messages are not JSON or not intended for this logic,
                // so we don't log an error unless debugging is needed.
                // console.log(`[Receiver] Non-JSON or irrelevant message received from client.`);
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
            // Manual broadcast loop: This iterates over every client and sends the live stream data.
            if (androidClients.size > 0) {
                androidClients.forEach(client => {
                    try {
                        client.send(message, isBinary);
                    } catch (e) {
                        console.error(`[Receiver] Error broadcasting to a client: ${e.message}`);
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

console.log(`[Receiver] PID: ${process.pid} --- Server initialized.`);
