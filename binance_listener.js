// binance_listener.js (Modified for @aggTrade but sending @trade format)

const WebSocket = require('ws');

// --- Global Error Handlers ---
process.on('uncaughtException', (err, origin) => {
    console.error(`[Listener] FATAL: UNCAUGHT EXCEPTION`);
    console.error(err.stack || err);
    console.error(`Exception origin: ${origin}`);
    console.error(`[Listener] Exiting due to uncaught exception...`);
    setTimeout(() => process.exit(1), 1000).unref();
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('[Listener] FATAL: UNHANDLED PROMISE REJECTION');
    console.error('Unhandled Rejection at:', promise);
    console.error('Reason:', reason.stack || reason);
});

// --- Configuration ---
const binanceStreamUrl = 'wss://stream.binance.com:9443/ws/btcusdt@aggTrade'; // CHANGED
const internalReceiverUrl = 'ws://localhost:8082';
const RECONNECT_INTERVAL = 5000; // ms
const BINANCE_PING_INTERVAL_MS = 3 * 60 * 1000; // 3 minutes
const PRICE_CHANGE_THRESHOLD = 1.2; // Send data if price changes by this much or more

// --- State Variables ---
let binanceWsClient;
let internalWsClient;
let binancePingIntervalId;
let lastSentPrice = null; // Stores the numeric value of the last price sent

// --- Internal Receiver Connection ---
function connectToInternalReceiver() {
    if (internalWsClient && (internalWsClient.readyState === WebSocket.OPEN || internalWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to internal receiver: ${internalReceiverUrl}`);
    internalWsClient = new WebSocket(internalReceiverUrl);

    internalWsClient.on('open', () => {
        console.log('[Listener] Connected to internal receiver.');
        lastSentPrice = null; // Reset on new connection to ensure receiver gets fresh data
    });

    internalWsClient.on('error', (err) => {
        console.error('[Listener] Internal receiver WebSocket error:', err.message);
    });

    internalWsClient.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Internal receiver closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        internalWsClient = null;
        setTimeout(connectToInternalReceiver, RECONNECT_INTERVAL);
    });
}

/**
 * Manually extracts minimal data from an aggTrade message string and transforms it
 * into the format: { e: "trade", E: number, p: string } or null.
 */
function manualExtractMinimalData(messageString) {
    try {
        const outputEventTypeStr = "trade"; // "Twist": always report as "trade" type
        let eventTimeNum = null;
        let priceStr = null;

        // 1. Verify it's an aggTrade message. We only process these.
        //    Example aggTrade: {"e":"aggTrade","E":1690877712563,"s":"BTCUSDT","a":2638638505,"p":"29183.18000000","q":"0.00035000","f":3101511910,"l":3101511910,"T":1690877712563,"m":false,"M":true}
        const aggTradeEventTypeKey = '"e":"aggTrade"';
        let currentIndex = messageString.indexOf(aggTradeEventTypeKey);
        if (currentIndex === -1) {
            // Not an aggTrade message (could be ping/pong or other stream messages if Binance adds them)
            return null;
        }
        // We've confirmed it's an aggTrade, proceed to extract E and p.
        // The search for E and p should ideally start *after* the "e" field,
        // but for simplicity and common JSON structures, searching from beginning is often fine
        // if keys are distinct. Let's refine to search after the type key.
        let searchStartIndex = currentIndex + aggTradeEventTypeKey.length;


        // 2. Extract Event Time ("E":) - from aggTrade
        const eventTimeKey = '"E":';
        currentIndex = messageString.indexOf(eventTimeKey, searchStartIndex);
        if (currentIndex === -1) return null;
        currentIndex += eventTimeKey.length;
        let valueEndIndex = currentIndex;
        while (valueEndIndex < messageString.length && messageString[valueEndIndex] >= '0' && messageString[valueEndIndex] <= '9') {
            valueEndIndex++;
        }
        if (currentIndex === valueEndIndex) return null; // No digits found
        eventTimeNum = parseInt(messageString.substring(currentIndex, valueEndIndex), 10);
        if (isNaN(eventTimeNum)) return null;

        // 3. Extract Price ("p":") - from aggTrade
        // Ensure we start searching *after* the event time we just parsed
        const priceKey = '"p":"';
        let priceStartIndex = messageString.indexOf(priceKey, valueEndIndex); // Start search from after eventTime
        if (priceStartIndex === -1) return null;
        priceStartIndex += priceKey.length;
        let priceEndIndex = messageString.indexOf('"', priceStartIndex);
        if (priceEndIndex === -1) return null;
        priceStr = messageString.substring(priceStartIndex, priceEndIndex);
        // Validate price string (non-empty and can be parsed to a number)
        if (priceStr.length === 0 || isNaN(parseFloat(priceStr))) return null;

        return {
            e: outputEventTypeStr, // Hardcoded to "trade"
            E: eventTimeNum,
            p: priceStr
        };
    } catch (error) {
        // This catch is for unexpected errors during string manipulation itself.
        // console.error('[Listener] Error in manualExtractMinimalData:', error); // Optional: for debugging parser issues
        return null; // Keep this silent, main loop handles warnings for unparseable data
    }
}

// --- Binance Stream Connection ---
function connectToBinance() {
    if (binanceWsClient && (binanceWsClient.readyState === WebSocket.OPEN || binanceWsClient.readyState === WebSocket.CONNECTING)) {
        return;
    }
    console.log(`[Listener] Connecting to Binance: ${binanceStreamUrl}`);
    binanceWsClient = new WebSocket(binanceStreamUrl);

    binanceWsClient.on('open', function open() {
        console.log('[Listener] Connected to Binance stream (btcusdt@aggTrade).'); // UPDATED
        lastSentPrice = null; // Reset on new connection for fresh baseline

        if (binancePingIntervalId) clearInterval(binancePingIntervalId);
        binancePingIntervalId = setInterval(() => {
            if (binanceWsClient && binanceWsClient.readyState === WebSocket.OPEN) {
                binanceWsClient.ping(() => {}); // Send ping, no log needed on success
            }
        }, BINANCE_PING_INTERVAL_MS);
    });

    binanceWsClient.on('message', function incoming(data) {
        const messageString = data.toString();
        const minimalData = manualExtractMinimalData(messageString); // Now parses aggTrade, outputs "trade" format

        if (minimalData) {
            // minimalData is now { e: "trade", E: ..., p: ... }
            const currentPrice = parseFloat(minimalData.p);
            if (isNaN(currentPrice)) {
                console.warn('[Listener] Invalid price in transformed aggTrade data:', minimalData.p);
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
                    try {
                        const minimalJsonString = JSON.stringify(minimalData);
                        internalWsClient.send(minimalJsonString);
                        lastSentPrice = currentPrice;
                    } catch (stringifyError) {
                         console.error('[Listener] CRITICAL: Error stringifying minimal data:', stringifyError.message, stringifyError.stack);
                    }
                } else {
                    console.warn('[Listener] Internal receiver not open. Qualified data NOT sent.');
                }
            }
        } else {
            if (messageString && !messageString.includes('"ping"') && !messageString.includes('"pong"') && !messageString.includes('"e":"aggTrade"')) {
                 // Log if it's not ping/pong AND not a successfully parsed aggTrade (which would have returned non-null)
                 // This helps catch if aggTrade format changes or other unexpected messages appear.
                 // If it was an aggTrade message but parsing failed inside manualExtractMinimalData, it returns null, and this log will trigger.
                 console.warn('[Listener] Failed to extract data from aggTrade or unexpected message format. Snippet:', messageString.substring(0, 150));
            } else if (messageString && !messageString.includes('"ping"') && !messageString.includes('"pong"') && !messageString.includes(binanceStreamUrl.split('/').pop())) {
                // A more generic warning if it's not ping/pong and not an expected stream message
                // This might be too noisy if the stream sends other valid, non-data messages.
                // The previous warning is likely more targeted.
            }
        }
    });

    binanceWsClient.on('pong', () => {
        // Pong received, no log needed by default, confirms connection is alive
    });

    binanceWsClient.on('error', function error(err) {
        console.error('[Listener] Binance WebSocket error:', err.message);
    });

    binanceWsClient.on('close', function close(code, reason) {
        const reasonStr = reason ? reason.toString() : 'N/A';
        console.log(`[Listener] Binance WebSocket closed. Code: ${code}, Reason: ${reasonStr}. Reconnecting in ${RECONNECT_INTERVAL / 1000}s...`);
        if (binancePingIntervalId) { clearInterval(binancePingIntervalId); binancePingIntervalId = null; }
        binanceWsClient = null;
        setTimeout(connectToBinance, RECONNECT_INTERVAL);
    });
}

// --- Start the connections ---
console.log(`[Listener] PID: ${process.pid} --- Binance listener starting (btcusdt@aggTrade, transforming to 'trade' event, Price Threshold: ${PRICE_CHANGE_THRESHOLD})`); // UPDATED
connectToBinance();
connectToInternalReceiver();
console.log(`[Listener] PID: ${process.pid} --- Initial connection attempts initiated.`);
