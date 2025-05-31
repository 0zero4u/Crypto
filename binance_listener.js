// binance_listener.js (Modified for BookTicker, simplified payload, threshold-only push, reduced logging)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) { try { clearInterval(binancePingIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binancePingIntervalId) { try { clearInterval(binancePingIntervalId); } catch(e) { /* ignore */ } }
        process.exit(1);
    }, 1000).unref();
});

// --- Listener Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@bookTicker';
const internalReceiverUrl = 'ws://localhost:8082'; // Your internal client address
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2; // Push to internal client only if best bid price changes by this amount

// --- Listener State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let binancePingIntervalId = null;
let lastPriceMeetingThreshold = null; // Stores the last price that triggered a push

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver at ${internalReceiverUrl}...`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        // console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`);
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        // console.log(`[Listener] PID: ${process.pid} --- Disconnected from internal receiver. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Extracts minimal data (only price) from a bookTicker message string.
 * Returns an object { p: priceStr } or null.
 */
function extractPriceData(messageString) {
    try {
        const data = JSON.parse(messageString);

        if (!data || typeof data.b !== 'string' || data.b.length === 0) {
            return null;
        }
        const priceStr = data.b; // Best bid price
        
        if (isNaN(parseFloat(priceStr))) {
            return null;
        }

        return { p: priceStr }; // Simplified payload
    } catch (error) {
        // This log can be enabled for deep debugging of parsing issues if needed.
        // console.error(`[Listener] Error parsing bookTicker JSON: ${error.message}. Data: ${messageString}`);
        return null;
    }
}

// --- Binance Stream Connection (BookTicker) ---
function connectToBinanceStream() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    // console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance stream (${binanceStreamUrl})...`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        // console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream (${binanceStreamUrl}).`);
        lastPriceMeetingThreshold = null; // Reset on new connection

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceWsClient.ping(() => {});
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance:`, pingError.message);
                }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const priceData = extractPriceData(messageString);

            if (priceData) {
                const currentPrice = parseFloat(priceData.p);
                if (isNaN(currentPrice)) {
                    return;
                }

                let isSignificantChange = false;
                if (lastPriceMeetingThreshold === null) {
                    isSignificantChange = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastPriceMeetingThreshold);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                        isSignificantChange = true;
                    }
                }

                if (isSignificantChange) {
                    lastPriceMeetingThreshold = currentPrice;

                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        let payloadString;
                        try {
                            payloadString = JSON.stringify(priceData);
                        } catch (stringifyError) {
                            console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying data for push:`, stringifyError.message);
                            return;
                        }
                        try {
                            internalWsClient.send(payloadString);
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Error sending data to internal receiver:`, sendError.message);
                        }
                    }
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance message handler:`, e.message, e.stack);
        }
    });

    binanceWsClient.on('pong', () => { });

    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance WebSocket error:`, err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        // console.log(`[Listener] PID: ${process.pid} --- Binance WebSocket closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'N/A'}`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinanceStream, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
// console.log(`[Listener] PID: ${process.pid} --- Binance Listener (BookTicker, Threshold Push) starting...`);

connectToInternalReceiver();
connectToBinanceStream();
