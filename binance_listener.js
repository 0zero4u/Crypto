
// bybit_listener_optimized.js
const uWS = require('uWebSockets.js');

// --- Process-wide Error Handling ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    process.exit(1); // uWS doesn't have a built-in cleanup, so we exit directly
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    process.exit(1);
});

// --- Configuration ---
const SYMBOL = 'BTCUSDT';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.1;

const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const EXCHANGE_STREAM_URL = 'wss://stream.bybit.com/v5/public/spot';

// --- WebSocket State Management ---
let internalWs = null;      // Will hold the uWS socket for the internal connection
let exchangeWs = null;      // Will hold the uWS socket for the exchange connection
let heartbeatInterval = null;
let last_sent_price = null;
const payload_to_send = { type: 'S', p: 0.0 };

/**
 * Sends a payload to the internal WebSocket client.
 * @param {object} payload - The data to send.
 */
function sendToInternalClient(payload) {
    // Check if the internalWs object is alive before sending
    if (internalWs) {
        try {
            internalWs.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}. The connection might be closing.`);
            // uWS can throw if the socket is closed during the send operation
        }
    }
}

const app = uWS.App({});

function connectToExchange() {
    app.ws(EXCHANGE_STREAM_URL, {
        /* Options */
        compression: uWS.SHARED_COMPRESSOR,
        maxPayloadLength: 16 * 1024,
        idleTimeout: 30, // Bybit requires pings every 20s, so 30s timeout is safe

        /* Handlers */
        open: (ws) => {
            exchangeWs = ws;
            console.log(`[Bybit] Connection established to: ${EXCHANGE_STREAM_URL}`);
            
            const subscriptionMessage = { op: "subscribe", args: [`orderbook.1.${SYMBOL}`] };
            ws.send(JSON.stringify(subscriptionMessage));
            console.log(`[Bybit] Subscribed to ${subscriptionMessage.args[0]}`);
            
            last_sent_price = null; // Reset on new connection

            // Bybit requires a ping every 20 seconds
            heartbeatInterval = setInterval(() => {
                if (exchangeWs) { // Ensure socket still exists
                    exchangeWs.send(JSON.stringify({ op: 'ping' }));
                }
            }, 20000);
        },

        message: (ws, message, isBinary) => {
            try {
                const data = JSON.parse(Buffer.from(message).toString());
                if (data.topic && data.topic.startsWith('orderbook.1') && data.data) {
                    const bids = data.data.b;
                    if (bids && bids.length > 0 && bids[0].length > 0) {
                        const bestBidPrice = parseFloat(bids[0][0]);
                        if (isNaN(bestBidPrice)) return;

                        const shouldSendPrice = (last_sent_price === null) || (Math.abs(bestBidPrice - last_sent_price) >= MINIMUM_TICK_SIZE);
                        if (shouldSendPrice) {
                            payload_to_send.p = bestBidPrice;
                            sendToInternalClient(payload_to_send);
                            last_sent_price = bestBidPrice;
                        }
                    }
                }
            } catch (e) {
                console.error(`[Bybit] Error processing message: ${e.message}`);
            }
        },

        close: (ws, code, message) => {
            exchangeWs = null;
            if (heartbeatInterval) clearInterval(heartbeatInterval);
            console.error(`[Bybit] Connection closed. Code: ${code}. Reconnecting...`);
            setTimeout(connectToExchange, RECONNECT_INTERVAL_MS);
        },

        drain: (ws) => {
            // console.log('[Bybit] WebSocket backpressure drained');
        }
    });
}

function connectToInternalReceiver() {
    app.ws(internalReceiverUrl, {
        /* Options */
        compression: uWS.DISABLED, // Matches receiver config
        maxPayloadLength: 4 * 1024,
        idleTimeout: 35, // Should be higher than receiver's idle timeout

        /* Handlers */
        open: (ws) => {
            internalWs = ws;
            console.log('[Internal] Connection established.');
        },

        message: (ws, message, isBinary) => {
            // This client only sends, not expecting messages
        },

        close: (ws, code, message) => {
            internalWs = null;
            console.error(`[Internal] Connection closed. Code: ${code}. Reconnecting...`);
            setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
        },

        drain: (ws) => {
            // console.log('[Internal] WebSocket backpressure drained');
        }
    });
}


// --- Script Entry Point ---
console.log(`[Listener] Starting with uWebSockets.js... PID: ${process.pid}`);
connectToInternalReceiver();
connectToExchange();

// uWebSockets.js requires a listening port to run the event loop,
// but we can use a dummy one since this is a client-only script.
app.listen(9001, (token) => {
    if (token) {
        console.log('[Listener] Event loop started.');
    } else {
        console.error('[Listener] FAILED to start event loop.');
        process.exit(1);
    }
});
