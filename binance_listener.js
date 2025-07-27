// binance_listener_optimized.js
const uWS = require('uWebSockets.js'); // Replaced 'ws' with 'uWebSockets.js'

process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    process.exit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    process.exit(1);
});

const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;

const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

let internalWsClient;
let last_sent_trade_price = null;
const app = uWS.App({}); // uWS requires an App instance to create clients

function sendToInternalClient(payload) {
    // Check if the client exists and its connection is open
    if (internalWsClient && internalWsClient.getUserData().state === 'open') {
        try {
            // uWS can send strings, Buffers, or ArrayBuffers. We stringify the JSON payload.
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message (backpressure may be high): ${e.message}`);
        }
    }
}

// Define the behavior for the connection to Binance
const binanceConnection = {
    compression: uWS.SHARED_COMPRESSOR,
    idleTimeout: 35,
    open: (ws) => {
        console.log(`[Binance] Connection established to stream: ${SYMBOL}@trade`);
        last_sent_trade_price = null;
        ws.getUserData().state = 'open'; // Use userData to store connection state
    },
    message: (ws, message, isBinary) => {
        try {
            // Using standard JSON.parse as requested
            const messageObj = JSON.parse(Buffer.from(message).toString());

            if (messageObj.e === 'trade' && messageObj.p) {
                const current_trade_price = parseFloat(messageObj.p);
                if (isNaN(current_trade_price)) return;

                const shouldSendPrice = (last_sent_trade_price === null) || (Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE);

                if (shouldSendPrice) {
                    const payload = { type: 'S', p: current_trade_price };
                    sendToInternalClient(payload);
                    last_sent_trade_price = current_trade_price;
                }
            }
        } catch (e) {
            console.error(`[Binance] Error processing message: ${e.message}`);
        }
    },
    close: (ws, code, message) => {
        ws.getUserData().state = 'closed';
        console.error(`[Binance] Connection closed. Code: ${code}. Reconnecting...`);
        // Schedule a reconnection attempt
        setTimeout(() => app.ws(BINANCE_FUTURES_STREAM_URL, binanceConnection), RECONNECT_INTERVAL_MS);
    },
    error: (ws, err) => {
        console.error('[Binance] Connection error:', err);
    }
};

// Define the behavior for the connection to the internal receiver
const internalConnection = {
    idleTimeout: 30,
    open: (ws) => {
        console.log('[Internal] Connection established.');
        internalWsClient = ws; // Store the active client socket
        ws.getUserData().state = 'open';
    },
    message: (ws, message, isBinary) => {
        // The listener primarily sends data; incoming messages are not expected but can be logged.
        console.log('[Internal] Received unexpected message:', Buffer.from(message).toString());
    },
    close: (ws, code, message) => {
        ws.getUserData().state = 'closed';
        internalWsClient = null; // Clear the client on disconnect
        console.error('[Internal] Connection closed. Reconnecting...');
        setTimeout(() => app.ws(internalReceiverUrl, internalConnection), RECONNECT_INTERVAL_MS);
    },
    error: (ws, err) => {
        console.error(`[Internal] WebSocket error: ${err}`);
    }
};

console.log(`[Listener] Starting... PID: ${process.pid}`);

// Initiate both client connections
app.ws(BINANCE_FUTURES_STREAM_URL, binanceConnection);
app.ws(internalReceiverUrl, internalConnection);

// The uWS App must be "running" for clients to function.
// We can listen on a dummy port to keep the event loop alive.
app.listen(9001, (token) => {
    if (token) {
        console.log('[Listener] uWS client engine is running.');
    } else {
        console.error('[Listener] FATAL: Could not start uWS client engine.');
        process.exit(1);
    }
});
