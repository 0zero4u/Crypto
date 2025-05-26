// binance_listener.js (Modified for @aggTrade, sending @trade format, enhanced error handling)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    // Give a brief moment for logs to flush, then exit.
    // Important: If this handler itself throws, the process will terminate immediately.
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        process.exit(1);
    }, 1000).unref(); // .unref() allows the program to exit if this is the only timer.
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    // Similar exit strategy as uncaughtException
    setTimeout(() => {
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        process.exit(1);
    }, 1000).unref();
});

// --- Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade';
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL_MS = 5000;
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes for Binance official stream
const PRICE_CHANGE_THRESHOLD = 1.2;

// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let binancePingIntervalId = null;
let lastSentPrice = null; // Stores the numeric value of the last price sent

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Internal receiver connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log(`[Listener] PID: ${process.pid} --- Connected to internal receiver.`);
        lastSentPrice = null; // Reset on new connection to ensure fresh data
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
        // The 'close' event will also be triggered, which handles reconnection.
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        internalWsClient = null; // Ensure old client is cleared
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Manually extracts minimal data from an aggTrade message string and transforms it
 * into the format: { e: "trade", E: number, p: string } or null.
 */
function manualExtractMinimalData(messageString) {
    try {
        const outputEventTypeStr = "trade";
        let eventTimeNum = null;
        let priceStr = null;

        const aggTradeEventTypeKey = '"e":"aggTrade"';
        let currentIndex = messageString.indexOf(aggTradeEventTypeKey);
        if (currentIndex === -1) return null;
        let searchStartIndex = currentIndex + aggTradeEventTypeKey.length;

        const eventTimeKey = '"E":';
        currentIndex = messageString.indexOf(eventTimeKey, searchStartIndex);
        if (currentIndex === -1) return null;
        currentIndex += eventTimeKey.length;
        let valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null;
        eventTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(eventTimeNum)) return null;

        const priceKey = '"p":"';
        let priceStartIndex = messageString.indexOf(priceKey, valueEndIndex);
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;

        return { e: outputEventTypeStr, E: eventTimeNum, p: priceStr };
    } catch (error) {
        // console.error('[Listener] Error in manualExtractMinimalData:', error.message); // Keep silent by default
        return null;
    }
}

// --- Binance Stream Connection ---
function connectToBinance() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        console.log('[Listener] Binance connection attempt skipped: already open or connecting.');
        return;
    }
    console.log(`[Listener] PID: ${process.pid} --- Connecting to Binance: ${binanceStreamUrl}`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log(`[Listener] PID: ${process.pid} --- Connected to Binance stream (btcusdt@aggTrade).`);
        lastSentPrice = null;

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceWsClient.ping(() => {}); // Empty callback
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance:`, pingError.message);
                    // If ping fails, the connection might be stale. Rely on 'close' or 'error' events.
                }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        try { // *** ADDED: Outer try-catch for the entire message handler logic ***
            const messageString = data.toString();
            const minimalData = manualExtractMinimalData(messageString);

            if (minimalData) {
                const currentPrice = parseFloat(minimalData.p);
                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Invalid price in transformed aggTrade data:`, minimalData.p);
                    return;
                }

                let shouldSendData = false;
                if (lastSentPrice === null) {
                    shouldSendData = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastSentPrice);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                        shouldSendData = true;
                    }
                }

                if (shouldSendData) {
                    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
                        let minimalJsonString;
                        try {
                            minimalJsonString = JSON.stringify(minimalData);
                        } catch (stringifyError) {
                            console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying minimal data:`, stringifyError.message, stringifyError.stack);
                            return; // Don't attempt to send if stringify failed
                        }
                        
                        try { // *** ADDED: try-catch around internalWsClient.send() ***
                            internalWsClient.send(minimalJsonString);
                            lastSentPrice = currentPrice;
                            // console.log(`[Listener] Sent to internal: ${minimalJsonString}`); // Optional: for high-frequency debugging
                        } catch (sendError) {
                            console.error(`[Listener] PID: ${process.pid} --- Error sending data to internal receiver:`, sendError.message, sendError.stack);
                            // If send fails, the 'close' event on internalWsClient should eventually trigger and handle reconnection.
                            // Or, you could force a close/reconnect here:
                            // if (internalWsClient) internalWsClient.terminate(); // This will trigger 'close'
                        }
                    } else {
                        // console.warn(`[Listener] PID: ${process.pid} --- Internal receiver not open. Qualified data NOT sent.`); // Can be noisy
                    }
                }
            } else {
                 // Only log if it's not a known non-data message from Binance (like pongs, or other stream types if you subscribe to more)
                if (messageString && !messageString.includes('"e":"pong"')) { // Binance uses "e":"pong" for server-sent pongs to client pings
                    // Check if it's a JSON object at all
                    let isPotentiallyJson = false;
                    try {
                        JSON.parse(messageString); // Just to test if it's valid JSON
                        isPotentiallyJson = true;
                    } catch (e) { /* not json */ }

                    if (isPotentiallyJson && !messageString.includes('"e":"aggTrade"')) {
                         console.warn(`[Listener] PID: ${process.pid} --- Received non-aggTrade JSON or unexpected message from Binance. Snippet:`, messageString.substring(0, 150));
                    } else if (!isPotentiallyJson) {
                         console.warn(`[Listener] PID: ${process.pid} --- Received non-JSON message from Binance (or parsing failed in check). Snippet:`, messageString.substring(0, 150));
                    }
                    // The manualExtractMinimalData returning null for an aggTrade message means it couldn't parse it.
                }
            }
        } catch (e) { // Catch errors from the outer message handler logic
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance message handler (outer catch):`, e.message, e.stack);
            // Depending on the error, you might want to consider reconnecting to Binance
            // For example, if (binanceWsClient) binanceWsClient.terminate(); to trigger a full reconnect cycle.
        }
    });

    binanceWsClient.on('pong', () => {
        // console.log('[Listener] Pong received from Binance.'); // Can be noisy
    });

    binanceWsClient.on('error', function error(err) {
        console.error(`[Listener] PID: ${process.pid} --- Binance WebSocket error:`, err.message);
        // The 'close' event will also be triggered, which handles reconnection.
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null; // Ensure old client is cleared
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener starting (subscribing to btcusdt@aggTrade, transforming to 'trade' event output, Price Threshold: ${PRICE_CHANGE_THRESHOLD})`);
connectToBinance();
connectToInternalReceiver();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
