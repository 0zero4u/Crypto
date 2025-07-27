// binance_listener.js (Optimized with Manual String Slicing - The Correct Low-Latency Approach)
const WebSocket = require('ws');

// --- Process-level error handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`, err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`, reason);
    cleanupAndExit(1);
});

// --- Graceful shutdown function ---
function cleanupAndExit(exitCode = 1) {
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    console.error('[Listener] Initiating cleanup...');
    clientsToTerminate.forEach(client => {
        if (client && (client.readyState === WebSocket.OPEN || client.readyState === WebSocket.CONNECTING)) {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error during WebSocket termination: ${e.message}`); }
        }
    });
    setTimeout(() => {
        console.error(`[Listener] Exiting with code ${exitCode}.`);
        process.exit(exitCode);
    }, 1000).unref();
}

// --- Configuration ---
const SYMBOL = 'btcusdt';
const RECONNECT_INTERVAL_MS = 5000;
const MINIMUM_TICK_SIZE = 0.2;
const internalReceiverUrl = 'ws://instance-20250627-040948.asia-south2-a.c.ace-server-460719-b7.internal:8082/internal';
const BINANCE_FUTURES_STREAM_URL = `wss://fstream.binance.com/ws/${SYMBOL}@trade`;

// --- State variables ---
let internalWsClient, binanceWsClient;
let last_sent_trade_price = null;
const payload_to_send = { type: 'S', p: 0 }; // Reusable payload

// --- Internal WebSocket Client ---
function connectToInternalReceiver() { /* ... implementation from previous response ... */ }
function sendToInternalClient(payload) { /* ... implementation from previous response ... */ }
const price_key = '"p":"'; // Define the key we are searching for once

// --- Binance WebSocket Client ---
function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_FUTURES_STREAM_URL);
    
    binanceWsClient.on('open', () => {
        console.log(`[Binance] Connection established to stream: ${SYMBOL}@trade`);
        last_sent_trade_price = null;
    });
    
    binanceWsClient.on('message', (data) => {
        try {
            const messageStr = data.toString();

            // The correct, high-performance string slicing method. NO REGEX.
            const priceKeyIndex = messageStr.indexOf(price_key);
            if (priceKeyIndex === -1) return;

            const priceStartIndex = priceKeyIndex + price_key.length;
            const priceEndIndex = messageStr.indexOf('"', priceStartIndex);
            if (priceEndIndex === -1) return;

            const priceStr = messageStr.substring(priceStartIndex, priceEndIndex);
            const current_trade_price = parseFloat(priceStr);

            if (isNaN(current_trade_price)) return;

            if ((last_sent_trade_price === null) || (Math.abs(current_trade_price - last_sent_trade_price) >= MINIMUM_TICK_SIZE)) {
                payload_to_send.p = current_trade_price;
                sendToInternalClient(payload_to_send);
                last_sent_trade_price = current_trade_price;
            }

        } catch (e) { 
            console.error(`[Binance] Error processing message: ${e.message}`);
        }
    });

    binanceWsClient.on('error', (err) => console.error('[Binance] Connection error:', err.message));
    
    binanceWsClient.on('close', () => {
        console.error('[Binance] Connection closed. Reconnecting...');
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Application Entry Point ---
console.log(`[Listener] Starting... PID: ${process.pid}`);
connectToInternalReceiver(); // Assume this function is defined as before
connectToBinance();
