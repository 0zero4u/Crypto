const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    cleanupAndExit(1);
});
process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    cleanupAndExit(1);
});

// --- State Management ---
function cleanupAndExit(exitCode = 1) {
    // Terminate both WebSocket clients
    const clientsToTerminate = [internalWsClient, binanceWsClient];
    clientsToTerminate.forEach(client => {
        if (client && typeof client.terminate === 'function') {
            try { client.terminate(); } catch (e) { console.error(`[Listener] Error terminating a WebSocket client: ${e.message}`); }
        }
    });
    setTimeout(() => { process.exit(exitCode); }, 1000).unref();
}

// --- Listener Configuration ---
const BINANCE_SYMBOL = 'btcusdt'; // Binance uses lowercase without the dash
const RECONNECT_INTERVAL_MS = 5000;
// A "valid tick change" is defined as a price movement of at least this amount.
// For BTC/USDT, 0.1 represents a $0.10 change. Adjust as needed.
const MINIMUM_TICK_SIZE = 0.1;
const internalReceiverUrl = 'ws://localhost:8082';

// --- Exchange Stream URL ---
const BINANCE_STREAM_URL = `wss://stream.binance.com:9443/ws/${BINANCE_SYMBOL}@bookTicker`;

// --- Listener State Variables ---
let internalWsClient = null;
let binanceWsClient = null;
let last_sent_price = null;

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) return;
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => { /* Connection established */ });
    internalWsClient.on('error', (err) => console.error(`[Internal] WebSocket error: ${err.message}`));
    internalWsClient.on('close', () => {
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

// --- Data Forwarding to Internal Client ---
function sendToInternalClient(payload) {
    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        try {
            internalWsClient.send(JSON.stringify(payload));
        } catch (e) {
            console.error(`[Internal] Failed to send message: ${e.message}`);
        }
    } else {
        console.error('[Internal] Cannot send message, receiver not connected.');
    }
}

// --- Binance Data Processing ---
function processBinancePrice(current_price) {
    let shouldSend = false;

    // Always send the first price received after a connection.
    if (last_sent_price === null) {
        shouldSend = true;
    } else {
        // Only send if the price change is significant enough.
        const price_difference = Math.abs(current_price - last_sent_price);
        if (price_difference >= MINIMUM_TICK_SIZE) {
            shouldSend = true;
        }
    }
    
    if (shouldSend) {
        const pricePayload = { p: current_price };
        sendToInternalClient(pricePayload);
        last_sent_price = current_price;
    }
}

// --- Binance Exchange Connection Function ---
function connectToBinance() {
    binanceWsClient = new WebSocket(BINANCE_STREAM_URL);

    binanceWsClient.on('open', () => {
        // Reset last price on new connection to ensure the first tick is always sent.
        last_sent_price = null;
    });

    binanceWsClient.on('message', (data) => {
        try {
            const message = JSON.parse(data.toString());
            // 'b' is the best bid price in the Binance bookTicker stream
            const bestBid = message.b; 
            if (bestBid) {
                processBinancePrice(parseFloat(bestBid));
            }
        } catch (e) {
            // Silently ignore non-JSON messages or parsing errors
        }
    });

    binanceWsClient.on('error', (err) => {
        console.error('[Binance Listener] WebSocket connection error:', err.message);
    });

    binanceWsClient.on('close', () => {
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Start all connections ---
connectToInternalReceiver();
connectToBinance();
