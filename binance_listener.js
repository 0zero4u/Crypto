// binance_listener_final.js
const uWS = require('uWebSockets.js');
const { URL } = require('url'); // Native Node.js module

// --- Global Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;

const INTERNAL_RECEIVER_URL = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

let internalWsClient = null; // Holds the active client socket for the internal receiver
let binanceWsClient = null;  // Holds the active client socket for Binance
let last_sent_trade_price = null;

// --- Graceful Shutdown and Global Error Handling ---
const exitHandler = (signal) => {
    console.log(`[Listener] Received ${signal}. Shutting down gracefully.`);
    if (internalWsClient) internalWsClient.close();
    if (binanceWsClient) binanceWsClient.close();
    setTimeout(() => process.exit(0), 1000).unref();
};

process.on('uncaughtException', (err) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    process.exit(1);
});
process.on('unhandledRejection', (reason) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED REJECTION`, reason);
    process.exit(1);
});
process.on('SIGINT', () => exitHandler('SIGINT'));
process.on('SIGTERM', () => exitHandler('SIGTERM'));

// --- Main Application Logic ---
const app = uWS.App({});

function connectToBinance() {
    if (binanceWsClient) return; // Prevent multiple connection attempts
    console.log(`[Binance] Attempting to connect to ${BINANCE_FUTURES_STREAM_URL}`);
    
    // For client connections, you define the behavior directly in the ws() call with the URI
    app.ws(BINANCE_FUTURES_STREAM_URL, {
        compression: uWS.SHARED_COMPRESSOR, // Enable compression for the Binance connection
        open: (ws) => {
            console.log(`[Binance] Connection established to stream: ${SYMBOL}@trade`);
            last_sent_trade_price = null;
            binanceWsClient = ws;
        },
        message: (ws, message, isBinary) => {
            try {
                const data = JSON.parse(Buffer.from(message));
                if (data.e === 'trade' && data.p) {
                    const current_trade_price = parseFloat(data.p);
                    if (isNaN(current_trade_price)) return;

                    const shouldSendPrice = last_sent_trade_price === null || Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE;
                    
                    if (shouldSendPrice) {
                        sendToInternalClient({ type: 'S', p: current_trade_price });
                        last_sent_trade_price = current_trade_price;
                    }
                }
            } catch (e) {
                console.error(`[Binance] Error processing message: ${e.message}`);
            }
        },
        close: (ws, code, message) => {
            console.error(`[Binance] Connection closed. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
            binanceWsClient = null; // Clear the handle
            setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
        }
    });
}

function connectToInternalReceiver() {
    if (internalWsClient) return; // Prevent multiple connection attempts
    console.log(`[Internal] Attempting to connect to ${INTERNAL_RECEIVER_URL}`);
    
    app.ws(INTERNAL_RECEIVER_URL, {
        compression: uWS.DISABLED, // Disable compression for internal LAN traffic
        open: (ws) => {
            console.log('[Internal] Connection established.');
            internalWsClient = ws;
        },
        message: (ws, message, isBinary) => {
            // Not expecting messages from the receiver, but handler must exist
        },
        close: (ws, code, message) => {
            console.error(`[Internal] Connection closed. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
            internalWsClient = null; // Clear the handle
            setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
        }
    });
}

function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.getBufferedAmount() === 0) {
        internalWsClient.send(JSON.stringify(payload));
    }
}

// --- Start the Listener ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToBinance();
connectToInternalReceiver();

// Keep the Node.js event loop running. This is essential for client-only applications.
setInterval(() => {}, 1000 * 60 * 60);
