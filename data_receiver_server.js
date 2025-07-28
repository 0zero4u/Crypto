// data_receiver_server.js
const uWS = require('uWebSockets.js');
const axios = require('axios'); // This dependency is now correctly managed by package.json

const PUBLIC_PORT = 8081;
const INTERNAL_LISTENER_PORT = 8082;
const IDLE_TIMEOUT_SECONDS = 130;

// Binance API URL for an instant spot price quote
const BINANCE_TICKER_URL = 'https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT';

let listenSocketPublic, listenSocketInternal;
const androidClients = new Set();

uWS.App({})
    .ws('/public', {
        compression: uWS.SHARED_COMPRESSOR,
        maxPayloadLength: 16 * 1024,
        idleTimeout: IDLE_TIMEOUT_SECONDS,

        open: (ws) => {
            const clientIp = Buffer.from(ws.getRemoteAddressAsText()).toString();
            console.log(`[Receiver] Public client connected from ${clientIp}. Adding to broadcast set.`);
            androidClients.add(ws);
        },
        message: async (ws, message, isBinary) => {
            try {
                const request = JSON.parse(Buffer.from(message).toString());

                if (request.event === 'set_mode' && request.mode === 'semi_auto') {
                    console.log('[Receiver] Received semi_auto request. Fetching instant price for client.');
                    const response = await axios.get(BINANCE_TICKER_URL);
                    const lastPrice = parseFloat(response.data.lastPrice);

                    if (!isNaN(lastPrice)) {
                        const payload = { type: 'S', p: lastPrice };
                        try {
                           ws.send(JSON.stringify(payload), isBinary);
                           console.log(`[Receiver] Sent instant price ${lastPrice} to semi_auto client.`);
                        } catch (e) {
                           console.error(`[Receiver] Error sending instant price to client: ${e.message}`);
                        }
                    }
                }
            } catch (e) {
                // Non-JSON or irrelevant message received, can be ignored.
            }
        },
        close: (ws, code, message) => {
            console.log(`[Receiver] Public client disconnected. Removing from broadcast set.`);
            androidClients.delete(ws);
        }
    })
    .ws('/internal', {
        compression: uWS.DISABLED,
        maxPayloadLength: 4 * 1024,
        idleTimeout: 30,

        open: (ws) => {
            console.log('[Receiver] Internal listener connected.');
        },
        message: (ws, message, isBinary) => {
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
        if (token) console.log(`[Receiver] Public WebSocket server listening on port ${PUBLIC_PORT}`);
        else {
            console.error(`[Receiver] FAILED to listen on port ${PUBLIC_PORT}`);
            process.exit(1);
        }
    })
    .listen(INTERNAL_LISTENER_PORT, (token) => {
        listenSocketInternal = token;
        if (token) console.log(`[Receiver] Internal WebSocket server listening on port ${INTERNAL_LISTENER_PORT}`);
        else {
            console.error(`[Receiver] FAILED to listen on port ${INTERNAL_LISTENER_PORT}`);
            process.exit(1);
        }
    });

function shutdown() {
    console.log('[Receiver] Shutting down...');
    if (listenSocketPublic) uWS.us_listen_socket_close(listenSocketPublic);
    if (listenSocketInternal) uWS.us_listen_socket_close(listenSocketInternal);
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log(`[Receiver] PID: ${process.pid} --- Server initialized.`);
