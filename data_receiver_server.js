// data_receiver_server.js (uWebSockets.js version with Manual Broadcasting)
const uWS = require('uWebSockets.js');

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130;
const SEMI_AUTO_SEND_DELAY_MS = 1000;
const MAX_DATA_STALENESS_MS = 60000;

let listenSocketPublic, listenSocketInternal;
let internalListenerSocket = null; // **NEW**: Keep a reference to the listener's socket.

const androidClients = new Set();

let lastBroadcastData = {
    message: null,
    isBinary: false,
    timestamp: 0
};


uWS.App({})
    // --- Public WebSocket Server (for Android Clients) ---
    .ws('/public', {
        // (No changes to open, compression, maxPayloadLength, idleTimeout)
        compression: uWS.SHARED_COMPRESSOR,
        maxPayloadLength: 16 * 1024,
        idleTimeout: IDLE_TIMEOUT_SECONDS,

        open: (ws) => {
            const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] Public client connected from ${clientIp}. Adding to broadcast set.`);
            androidClients.add(ws);
        },
        message: (ws, message, isBinary) => {
            try {
                const messageString = Buffer.from(message).toString();
                const clientCommand = JSON.parse(messageString);

                if (clientCommand.event === 'set_mode' && clientCommand.mode === 'semi_auto') {
                    const isDataAvailable = lastBroadcastData.message !== null;
                    const isDataFresh = isDataAvailable && (Date.now() - lastBroadcastData.timestamp < MAX_DATA_STALENESS_MS);

                    if (isDataFresh) {
                        // Data is fresh, send it after the normal delay.
                        console.log(`[Receiver] Data is fresh. Scheduling price send in ${SEMI_AUTO_SEND_DELAY_MS}ms.`);
                        setTimeout(() => {
                            try {
                                ws.send(lastBroadcastData.message, lastBroadcastData.isBinary);
                            } catch (e) {
                                console.error(`[Receiver] FAILED to send delayed price. Client likely disconnected: ${e.message}`);
                            }
                        }, SEMI_AUTO_SEND_DELAY_MS);
                    } else {
                        // **MODIFIED**: Data is stale or unavailable, so command a refresh.
                        if (internalListenerSocket) {
                            console.log(`[Receiver] Data is stale or unavailable. Requesting fresh price from listener.`);
                            try {
                                internalListenerSocket.send(JSON.stringify({ action: 'get_fresh_price' }));
                                // We don't send anything to the client here. The price will arrive
                                // via the regular broadcast once the listener provides it.
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
    // --- Internal WebSocket Server (for binance_listener.js) ---
    .ws('/internal', {
        // (No changes to compression, maxPayloadLength, idleTimeout)
        compression: uWS.DISABLED,
        maxPayloadLength: 4 * 1024,
        idleTimeout: 30,
        
        open: (ws) => {
            console.log('[Receiver] Internal listener connected.');
            internalListenerSocket = ws; // **NEW**: Store the socket reference.
        },
        message: (ws, message, isBinary) => {
            try {
                const dataString = Buffer.from(message).toString();
                const parsedData = JSON.parse(dataString);
                
                if(parsedData.timestamp) {
                    lastBroadcastData.message = message;
                    lastBroadcastData.isBinary = isBinary;
                    lastBroadcastData.timestamp = parsedData.timestamp;
                }
            } catch (e) {
                console.error(`[Receiver] Error parsing message from internal listener: ${e.message}`);
                return;
            }

            // Broadcast to all clients
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
            console.log('[Receiver] Internal listener disconnected. Clearing last known price and socket.');
            lastBroadcastData.message = null;
            lastBroadcastData.isBinary = false;
            lastBroadcastData.timestamp = 0;
            internalListenerSocket = null; // **NEW**: Clear the socket reference.
        }
    })
    // --- listen and shutdown sections ---
    // (No changes to this section)
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
