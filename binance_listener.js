// binance_listener_uw_corrected.js
const uWS = require('uWebSockets.js');

// --- Global Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;

const INTERNAL_RECEIVER_URL = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

let internalWsClient = null;
let binanceWsClient = null;
let last_sent_trade_price = null;

// --- Graceful Shutdown and Error Handling ---
process.on('uncaughtException', (err) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    process.exit(1);
});
process.on('unhandledRejection', (reason) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED REJECTION`, reason);
    process.exit(1);
});
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));

// --- Main Application Logic ---
const app = uWS.App({})
    // Behavior for the connection to the internal data_receiver
    .ws('/internal', {
        open: (ws) => {
            console.log('[Internal] Connection established.');
            internalWsClient = ws;
        },
        close: (ws, code, message) => {
            console.error(`[Internal] Connection closed. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
            internalWsClient = null;
            setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
        },
        drain: (ws) => {
            console.log('[Internal] WebSocket backpressure drained.');
        },
        message: (ws, message, isBinary) => {
            // Not expecting messages from the receiver
        }
    })
    // Behavior for the connection to the Binance API
    .ws(`/${SYMBOL}@trade`, {
        open: (ws) => {
            console.log(`[Binance] Connection established to stream: ${SYMBOL}@trade`);
            last_sent_trade_price = null;
            binanceWsClient = ws;
        },
        close: (ws, code, message) => {
            console.error(`[Binance] Connection closed. Reconnecting in ${RECONNECT_INTERVAL_MS}ms...`);
            binanceWsClient = null;
            setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
        },
        drain: (ws) => {
            console.log('[Binance] WebSocket backpressure drained.');
        },
        message: (ws, message, isBinary) => {
            try {
                const data = JSON.parse(Buffer.from(message));
                if (data.e === 'trade' && data.p) {
                    const current_trade_price = parseFloat(data.p);
                    if (isNaN(current_trade_price)) return;

                    if (last_sent_trade_price === null || Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE) {
                        const payload = { type: 'S', p: current_trade_price };
                        sendToInternalClient(payload);
                        last_sent_trade_price = current_trade_price;
                    }
                }
            } catch (e) {
                console.error(`[Binance] Error processing message: ${e.message}`);
            }
        }
    });

// --- Connection Functions ---
function connectToInternalReceiver() {
    if (internalWsClient) return;
    console.log('[Internal] Attempting to connect...');
    const url = new URL(INTERNAL_RECEIVER_URL);
    app.connect(url.href, {}); // The 'path' in the URL must match a defined behavior
}

function connectToBinance() {
    if (binanceWsClient) return;
    console.log('[Binance] Attempting to connect...');
    const url = new URL(BINANCE_FUTURES_STREAM_URL);
    // The path `/ws/btcusdt@trade` must match a behavior. We strip the leading `/ws`
    const behaviorPath = url.pathname.startsWith('/ws/') ? url.pathname.substring(3) : url.pathname;
    app.connect(url.href, { path: behaviorPath });
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.getBufferedAmount() === 0) {
        internalWsClient.send(JSON.stringify(payload));
    }
}

// --- Start the Listener ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver();
connectToBinance();

// We still need to keep the event loop alive. uWS does not require a listen() call for clients.
// A persistent interval is a standard way to prevent the script from exiting.
setInterval(() => {}, 1000 * 60 * 60);
