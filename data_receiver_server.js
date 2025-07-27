// data_receiver_server.js (uWebSockets.js version with Manual Broadcasting)
const uWS = require('uWebSockets.js');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130;
const SEMI_AUTO_SEND_DELAY_MS = 1000;
const MAX_DATA_STALENESS_MS = 60000;

let listenSocketPublic, listenSocketInternal;
let internalListenerSocket = null;

const androidClients = new Set();

// This state object holds the last valid price data received.
let lastBroadcastData = {
    price: null,
    timestamp: 0
};


uWS.App({})
    // ===================================================================
    //  WORKFLOW FOR PUBLIC CLIENTS (Android App)
    // ===================================================================
    .ws('/public', {
        compression: uWS.SHARED_COMPRESSOR,
        maxPayloadLength: 16 * 1024,
        idleTimeout: IDLE_TIMEOUT_SECONDS,

        open: (ws) => {
            const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] Public client connected from ${clientIp}. Adding to broadcast set.`);
            androidClients.add(ws);
        },
        
        // This code ONLY runs when a specific Android client sends a message.
        message: (ws, message, isBinary) => {
            try {
                const messageString = Buffer.from(message).toString();
                const clientCommand = JSON.parse(messageString);

                // We only care about the 'semi_auto' command.
                if (clientCommand.event === 'set_mode' && clientCommand.mode === 'semi_auto') {
                    const isDataAvailable = lastBroadcastData.price !== null;
                    const isDataFresh = isDataAvailable && (Date.now() - lastBroadcastData.timestamp < MAX_DATA_STALENESS_MS);

                    if (isDataFresh) {
                        // If data is good, send it to THIS SPECIFIC client after a delay.
                        // This does NOT affect any other clients.
                        console.log(`[Receiver] Data is fresh. Scheduling price send in ${SEMI_AUTO_SEND_DELAY_MS}ms.`);
                        setTimeout(() => {
                            try {
                                const clientPayload = JSON.stringify({ type: 'S', p: lastBroadcastData.price });
                                ws.send(clientPayload, false);
                            } catch (e) {
                                console.error(`[Receiver] FAILED to send delayed price. Client likely disconnected: ${e.message}`);
                            }
                        }, SEMI_AUTO_SEND_DELAY_MS);
                    } else {
                        // If data is stale, ask the listener for a new price. The new price will be
                        // broadcast to EVERYONE via the '/internal' message handler below.
                        if (internalListenerSocket) {
                            console.log(`[Receiver] Data is stale or unavailable. Requesting fresh price from listener.`);
                            try {
                                internalListenerSocket.send(JSON.stringify({ action: 'get_fresh_price' }));
                            } catch (e) {
                                console.error(`[Receiver] Failed to send refresh command to listener: ${e.message}`);
                            }
                        } else {
                            console.log('[Receiver] Cannot refresh stale data: Internal listener is not connected.');
                        }
                    }
                }
            } catch (e) {
                // Ignore non-command messages.
            }
        },
        close: (ws, code, message) => {
            console.log(`[Receiver] Public client disconnected. Removing from broadcast set.`);
            androidClients.delete(ws);
        }
    })
    // ===================================================================
    //  WORKFLOW FOR INTERNAL LISTENER (binance_listener.js)
    // ===================================================================
    .ws('/internal', {
        compression: uWS.DISABLED,
        maxPayloadLength: 4 * 1024,
        idleTimeout: 30,
        
        open: (ws) => {
            console.log('[Receiver] Internal listener connected.');
            internalListenerSocket = ws;
        },

        // This code runs for EVERY message from binance_listener (pings and price updates).
        // This is where the STANDARD BROADCAST happens.
        message: (ws, message, isBinary) => {
            let parsedData;
            try {
                const dataString = Buffer.from(message).toString();
                parsedData = JSON.parse(dataString);
                
                // If it's a heartbeat, ignore it and stop. Its only job was to keep the connection alive.
                if (parsedData.type === 'ping') {
                    return;
                }
                
                // If it's a valid price message, store the data.
                if(parsedData.timestamp && typeof parsedData.p !== 'undefined') {
                    lastBroadcastData.price = parsedData.p;
                    lastBroadcastData.timestamp = parsedData.timestamp;
                } else {
                    return; // Ignore malformed messages.
                }
            } catch (e) {
                console.error(`[Receiver] Error parsing message from internal listener: ${e.message}`);
                return;
            }

            // **CRITICAL**: This part broadcasts the price update to ALL connected clients,
            // ensuring normal clients get their tick-based updates.
            const clientPayload = JSON.stringify({ type: 'S', p: parsedData.p });

            if (androidClients.size > 0) {
                androidClients.forEach(client => {
                    try {
                        client.send(clientPayload, false);
                    } catch (e) {
                        console.error(`[Receiver] Error sending to a client: ${e.message}`);
                    }
                });
            }
        },
        close: (ws, code, message) => {
            console.log('[Receiver] Internal listener disconnected. Clearing last known price and socket.');
            lastBroadcastData.price = null;
            lastBroadcastData.timestamp = 0;
            internalListenerSocket = null;
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
