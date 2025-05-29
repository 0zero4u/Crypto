// binance_listener.js

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`[Listener] Exception origin: ${origin}`);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to uncaught exception...`);
    setTimeout(() => {
        if (pushIntervalId) { try { clearInterval(pushIntervalId); } catch(e) { /* ignore */ } }
        if (internalWsClient && typeof internalWsClient.terminate === 'function') {
            try { internalWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        if (binanceWsClient && typeof binanceWsClient.terminate === 'function') {
            try { binanceWsClient.terminate(); } catch (e) { /* ignore */ }
        }
        process.exit(1);
    }, 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error(`[Listener] PID: ${process.pid} --- FATAL: UNHANDLED PROMISE REJECTION`);
    console.error('[Listener] Unhandled Rejection at:', promise);
    console.error('[Listener] Reason:', reason instanceof Error ? reason.stack : reason);
    console.error(`[Listener] PID: ${process.pid} --- Exiting due to unhandled promise rejection...`);
    setTimeout(() => {
        if (pushIntervalId) { try { clearInterval(pushIntervalId); } catch(e) { /* ignore */ } }
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
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000;
const PRICE_CHANGE_THRESHOLD = 1.2;
const PUSH_INTERVAL_MS = 20; // Fixed push interval

// --- State Variables ---
let binanceWsClient = null;
let internalWsClient = null;
let binancePingIntervalId = null;
let pushIntervalId = null; // For the 20ms push interval

// Stores the numeric value of the price from the last message that passed the threshold
let lastPriceMeetingThreshold = null;
// Stores the most recent minimalData object that passed the threshold and is ready for interval push
let latestSignificantDataToPush = null;
// Stores the actual data object that was last successfully sent by the 20ms interval
let lastPushedDataByInterval = null;


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
        // Reset state related to pushing data to this internal receiver instance
        lastPushedDataByInterval = null;
        // Clear any pending data from a previous connection state or old Binance data.
        // A new significant trade must come from Binance to populate this.
        latestSignificantDataToPush = null;
        startPushInterval();
    });

    internalWsClient.on('error', (err) => {
        console.error(`[Listener] PID: ${process.pid} --- Internal receiver WebSocket error:`, err.message);
        // The 'close' event will also be triggered, which handles reconnection and interval clearing.
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] PID: ${process.pid} --- Internal receiver closed. Code: ${code}, Reason: ${reasonStr}.`);
        if (pushIntervalId) {
            clearInterval(pushIntervalId);
            pushIntervalId = null;
            console.log('[Listener] Push interval stopped.');
        }
        internalWsClient = null; // Ensure old client is cleared
        console.log(`[Listener] Reconnecting to internal receiver in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL_MS);
    });
}

/**
 * Manually extracts minimal data from an aggTrade message string and transforms it
 * into the format: { e: "trade", E: number, p: string } or null.
 * This function creates a new object on each successful parse.
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

// --- Push Interval Logic ---
function startPushInterval() {
    // Clear existing interval if any (e.g., if called after a quick reconnect where 'close' might not have fired yet for previous instance)
    if (pushIntervalId) {
        clearInterval(pushIntervalId);
        pushIntervalId = null;
    }

    if (internalWsClient && internalWsClient.readyState === WebSocket.OPEN) {
        console.log(`[Listener] PID: ${process.pid} --- Starting aggTrade push interval (${PUSH_INTERVAL_MS}ms).`);
        pushIntervalId = setInterval(() => {
            if (!(internalWsClient && internalWsClient.readyState === WebSocket.OPEN)) {
                // This state means the interval is running but the client closed/errored.
                // The 'close' handler for internalWsClient should have cleared this interval.
                // This is a defensive clear.
                if (pushIntervalId) {
                    clearInterval(pushIntervalId);
                    pushIntervalId = null;
                    console.warn('[Listener] Push interval: Stale interval cleared by tick check as internal client is not open.');
                }
                return;
            }

            if (!latestSignificantDataToPush) {
                // No new significant data has been received from Binance to push.
                return;
            }

            // Only push if the latest significant data is different from what was last pushed.
            // This prevents re-sending the same data if no new significant trades have occurred.
            // `latestSignificantDataToPush` gets a new object reference when updated by `manualExtractMinimalData`.
            if (latestSignificantDataToPush === lastPushedDataByInterval) {
                return;
            }

            const dataToPush = latestSignificantDataToPush; // Get a reference to the current significant data
            let minimalJsonString;
            try {
                minimalJsonString = JSON.stringify(dataToPush);
            } catch (stringifyError) {
                console.error(`[Listener] PID: ${process.pid} --- CRITICAL: Error stringifying data for interval push:`, stringifyError.message, stringifyError.stack);
                return; // Skip this push if stringification fails
            }

            try {
                internalWsClient.send(minimalJsonString);
                lastPushedDataByInterval = dataToPush; // Mark this specific data object as successfully pushed
                // console.log(`[Listener] Interval PUSHED: ${minimalJsonString}`); // Verbose: for high-frequency debugging
            } catch (sendError) {
                console.error(`[Listener] PID: ${process.pid} --- Error sending data to internal receiver via interval:`, sendError.message, sendError.stack);
                // The 'error' or 'close' event on internalWsClient should handle this by attempting reconnection
                // and clearing the interval. If sendError is persistent, it might indicate a problem with internalWsClient.
            }
        }, PUSH_INTERVAL_MS);
    } else {
        console.warn(`[Listener] PID: ${process.pid} --- Internal client not open/ready when attempting to start push interval. Interval NOT started.`);
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
        // Reset state related to this new Binance stream connection
        lastPriceMeetingThreshold = null;
        // Clear any potentially stale "latest significant data" from a previous Binance stream session.
        // This ensures the interval only picks up data validated against the *current* stream.
        latestSignificantDataToPush = null;
        // Note: lastPushedDataByInterval is related to the internal receiver's state and is reset when internal receiver connects.

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                try {
                    binanceWsClient.ping(() => {}); // Empty callback
                } catch (pingError) {
                    console.error(`[Listener] PID: ${process.pid} --- Error sending ping to Binance:`, pingError.message);
                }
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        try {
            const messageString = data.toString();
            const minimalData = manualExtractMinimalData(messageString);

            if (minimalData) {
                const currentPrice = parseFloat(minimalData.p);
                if (isNaN(currentPrice)) {
                    console.warn(`[Listener] PID: ${process.pid} --- Invalid price in transformed aggTrade data:`, minimalData.p);
                    return;
                }

                let isSignificantChange = false;
                if (lastPriceMeetingThreshold === null) { // First message after (re)connect or first ever
                    isSignificantChange = true;
                } else {
                    const priceDifference = Math.abs(currentPrice - lastPriceMeetingThreshold);
                    if (priceDifference >= PRICE_CHANGE_THRESHOLD) {
                        isSignificantChange = true;
                    }
                }

                if (isSignificantChange) {
                    // This new data is significant. Store it for the interval to pick up.
                    // manualExtractMinimalData creates a new object, so this is a new reference.
                    latestSignificantDataToPush = minimalData;
                    lastPriceMeetingThreshold = currentPrice;
                    // The interval push logic will handle sending it.
                }
            } else {
                 // Only log if it's not a known non-data message from Binance
                if (messageString && !messageString.includes('"e":"pong"')) {
                    let isPotentiallyJson = false;
                    try { JSON.parse(messageString); isPotentiallyJson = true; } catch (e) { /* not json */ }

                    if (isPotentiallyJson && !messageString.includes('"e":"aggTrade"')) {
                         console.warn(`[Listener] PID: ${process.pid} --- Received non-aggTrade JSON or unexpected message from Binance. Snippet:`, messageString.substring(0, 150));
                    } else if (!isPotentiallyJson) {
                         console.warn(`[Listener] PID: ${process.pid} --- Received non-JSON message from Binance (or parsing failed in check). Snippet:`, messageString.substring(0, 150));
                    }
                }
            }
        } catch (e) {
            console.error(`[Listener] PID: ${process.pid} --- CRITICAL ERROR in Binance message handler (outer catch):`, e.message, e.stack);
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
        console.log(`[Listener] PID: ${process.pid} --- Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}.`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null; // Ensure old client is cleared
        console.log(`[Listener] Reconnecting to Binance in ${RECONNECT_INTERVAL_MS / 1000}s...`);
        setTimeout(connectToBinance, RECONNECT_INTERVAL_MS);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener starting (subscribing to btcusdt@aggTrade, transforming to 'trade' event output, Price Threshold: ${PRICE_CHANGE_THRESHOLD}, Push Interval: ${PUSH_INTERVAL_MS}ms)`);
connectToBinance();
connectToInternalReceiver();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);